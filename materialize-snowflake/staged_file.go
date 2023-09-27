package main

import (
	"bufio"
	"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// fileBuffer provides Close() for a *bufio.Writer writing to an *os.File. Close() will flush the
// buffer and close the underlying file.
type fileBuffer struct {
	buf  *bufio.Writer
	file *os.File
}

func (f *fileBuffer) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

func (f *fileBuffer) Close() error {
	if err := f.buf.Flush(); err != nil {
		return err
	} else if err := f.file.Close(); err != nil {
		return err
	}
	return nil
}

// stagedFile manages uploading a sequence of local files produced by reading from Load/Store
// iterators to an internal Snowflake stage. The same stagedFile should not be used concurrently
// across multiple goroutines, but multiple concurrent processes can create their own stagedFile and
// use them.
//
// The data to be staged is split into multiple files for a few reasons:
//   - To allow for parallel processing within Snowflake
//   - To enable some amount of concurrency between the network operations for PUTs and the encoding
//     & writing of JSON data locally
//   - To prevent any individual file from becoming excessively large, which seems to bog down the
//     encryption process used within the go-snowflake driver
//
// Ideally we would stream directly to the Snowflake internal stage rather than reading from a local
// disk file, but streaming PUTs do not currently work well and buffer excessive amounts of data
// in-memory; see https://github.com/estuary/connectors/issues/50.
//
// The lifecycle of a staged file for a transaction is as follows:
//
// - start: Initializes the local directory for local disk files and clears the Snowflake stage of
// any staged files from a previous transaction. Starts a worker that will concurrently send files
// to Snowflake via PUT commands as local files are finished.
//
// - encodeRow: Encodes a slice of values as JSON and writes to the current local file. If the local
// file has reached a size threshold a new file will be started. Finished files are sent to the
// worker for staging in Snowflake.
//
// - flush: Sends the current & final local file to the worker for staging and waits for the worker
// to complete before returning.
type stagedFile struct {
	// Random string that will serve as the directory for local files of this binding.
	uuid string

	// The full directory path of local files for this binding formed by joining tempdir and uuid.
	dir string

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush().
	started bool

	// Index of the current file for this transaction. Starts at 0 and is incremented by 1 for each
	// new file that is created.
	fileIdx int

	// References to the current file being written.
	buf     *fileBuffer
	encoder *sql.CountingEncoder

	// Per-transaction coordination.
	putFiles chan string
	group    *errgroup.Group
	groupCtx context.Context // Used to check for group cancellation upon the worker returning an error.
}

func newStagedFile(tempdir string) *stagedFile {
	uuid := uuid.NewString()

	return &stagedFile{
		uuid: uuid,
		dir:  filepath.Join(tempdir, uuid),
	}
}

func (f *stagedFile) start(ctx context.Context, conn *stdsql.Conn) error {
	if f.started {
		return nil
	}
	f.started = true

	// Create the local working directory for this binding. As a simplification we will always
	// remove and re-create the directory since it will already exist for transactions beyond the
	// first one.
	if err := os.RemoveAll(f.dir); err != nil {
		return fmt.Errorf("clearing temp dir: %w", err)
	} else if err := os.Mkdir(f.dir, 0700); err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}

	// Sanity check: Make sure the Snowflake stage directory is really empty. As far as I can tell
	// if the REMOVE command is successful it means that all files really were removed so
	// theoretically this is a superfluous check, but the consequences of any lingering files in the
	// directory are incorrect loaded data and this is a cheap check to be extra sure.
	if rows, err := conn.QueryContext(ctx, fmt.Sprintf(`LIST @flow_v1/%s;`, f.uuid)); err != nil {
		return fmt.Errorf("verifying stage empty: %w", err)
	} else if rows.Next() {
		return fmt.Errorf("unexpected existing file from LIST @flow_v1/%s", f.uuid)
	} else {
		rows.Close()
	}

	// Reset values used per-transaction.
	f.fileIdx = 0
	f.group, f.groupCtx = errgroup.WithContext(ctx)
	f.putFiles = make(chan string)

	// Start the putWorker for this transaction.
	f.group.Go(func() error {
		return f.putWorker(f.groupCtx, conn, f.putFiles)
	})

	return nil
}

func (f *stagedFile) encodeRow(row []interface{}) error {
	// May not have an encoder set yet if the previous encodeRow() resulted in flushing the current
	// file, or for the very first call to encodeRow().
	if f.encoder == nil {
		if err := f.newFile(); err != nil {
			return err
		}
	}

	if err := f.encoder.Encode(row); err != nil {
		return fmt.Errorf("encoding row: %w", err)
	}

	// Concurrently start the PUT process for this file if the current file has reached
	// fileSizeLimit.
	if f.encoder.Written() >= sql.DefaultFileSizeLimit {
		if err := f.putFile(); err != nil {
			return fmt.Errorf("encodeRow putFile: %w", err)
		}
	}

	return nil
}

func (f *stagedFile) flush(ctx context.Context, conn *stdsql.Conn) (func(context.Context), error) {
	if err := f.putFile(); err != nil {
		return nil, fmt.Errorf("flush putFile: %w", err)
	}

	close(f.putFiles)
	f.started = false

	// Wait for all outstanding PUT requests to complete.
	if err := f.group.Wait(); err != nil {
		return nil, err
	}

	return func(ctx context.Context) {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`REMOVE @flow_v1/%s;`, f.uuid)); err != nil {
			log.WithFields(log.Fields{
				"stage": fmt.Sprintf("@flow_v1/%s;", f.uuid),
				"err":   err,
			}).Warn("failed to clear stage")
		}
	}, nil
}

func (f *stagedFile) putWorker(ctx context.Context, conn *stdsql.Conn, filePaths <-chan string) error {
	for {
		var file string

		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-filePaths:
			if !ok {
				return nil
			}
			file = f
		}

		query := fmt.Sprintf(
			// OVERWRITE=TRUE is set here not because we intend to overwrite anything, but rather to
			// avoid an extra LIST query that setting OVERWRITE=FALSE incurs.
			`PUT file://%s @flow_v1/%s AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE;`,
			file, f.uuid,
		)
		var source, target, sourceSize, targetSize, sourceCompression, targetCompression, status, message string
		if err := conn.QueryRowContext(ctx, query).Scan(&source, &target, &sourceSize, &targetSize, &sourceCompression, &targetCompression, &status, &message); err != nil {
			return fmt.Errorf("putWorker PUT to stage: %w", err)
		} else if !strings.EqualFold("uploaded", status) {
			return fmt.Errorf("putWorker PUT to stage unexpected upload status: %s", status)
		}

		// Once the file has been staged to Snowflake we don't need it locally anymore and can
		// remove the local copy to manage disk usage.
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("putWorker removing local file: %w", err)
		}
	}
}

func (f *stagedFile) newFile() error {
	filePath := filepath.Join(f.dir, fmt.Sprintf("%d", f.fileIdx))

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	f.buf = &fileBuffer{
		buf:  bufio.NewWriter(file),
		file: file,
	}
	f.encoder = sql.NewCountingEncoder(f.buf)
	f.fileIdx += 1

	return nil
}

func (f *stagedFile) putFile() error {
	if f.encoder == nil {
		return nil
	}

	if err := f.encoder.Close(); err != nil {
		return fmt.Errorf("closing encoder: %w", err)
	}
	f.encoder = nil

	select {
	case <-f.groupCtx.Done():
		// If the group worker has returned an error and cancelled the group context, return that
		// rather than the general "context cancelled" error.
		return f.group.Wait()
	case f.putFiles <- f.buf.file.Name():
		return nil
	}
}
