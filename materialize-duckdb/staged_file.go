package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// stagedFile is a wrapper around an s3 upload manager & client for streaming file uploads to s3.
// The same stagedFile should not be used concurrently across multiple goroutines, but multiple
// concurrent processes can create their own stagedFile and use them.
//
// The lifecycle of uploading a file is as follows:
//
// - start: Sets up the data pipe and uploader. The uploader reads data from the pipe until EOF and
// continuously buffers/chunks/streams the data to S3. Can be called repeatedly until flush.
//
// - encodeRow: Encodes a slice of values as JSON and outputs a JSON map with keys corresponding to
// the columns the stagedFile was initialized with.
//
// - flush: Closes the pipe and waits for the uploader to finish. When the uploader has finished
// uploading the file, a callback is returned that can be used to delete the created file.
type stagedFile struct {
	cols       []*sql.Column
	client     *s3.Client
	uploader   *manager.Uploader
	bucket     string
	key        string
	group      *errgroup.Group
	pipeWriter *io.PipeWriter
	encoder    *json.Encoder
	started    bool
}

func newStagedFile(client *s3.Client, bucket string, bucketPath string, cols []*sql.Column) *stagedFile {
	return &stagedFile{
		cols:   cols,
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.Concurrency = 1
		}),
		bucket: bucket,
		key:    path.Join(bucketPath, uuid.NewString()) + ".jsonl",
	}
}

func (f *stagedFile) start(ctx context.Context) {
	if f.started {
		return
	}

	r, w := io.Pipe()
	f.pipeWriter = w

	f.encoder = json.NewEncoder(f.pipeWriter)
	f.encoder.SetEscapeHTML(false)
	f.encoder.SetIndent("", "")

	f.group, _ = errgroup.WithContext(ctx)

	f.group.Go(func() error {
		_, err := f.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
			Body:   r,
		})
		if err != nil {
			// Closing the read half of the pipe will cause subsequent writes to fail, with the
			// error received here propagated.
			r.CloseWithError(err)
			return err
		}

		return nil
	})

	f.started = true
}

func (f *stagedFile) encodeRow(row []interface{}) error {
	if len(row) != len(f.cols) {
		return fmt.Errorf("number of headers in row to encode (%d) differs from number of configured headers (%d)", len(row), len(f.cols))
	}

	d := make(map[string]interface{})
	for idx := range row {
		d[f.cols[idx].Field] = row[idx]
	}

	return f.encoder.Encode(d)
}

func (f *stagedFile) flush() (func(context.Context) error, error) {
	f.pipeWriter.Close()
	if err := f.group.Wait(); err != nil {
		return nil, err
	}

	f.started = false

	return func(ctx context.Context) error {
		_, err := f.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		})
		return err
	}, nil
}
