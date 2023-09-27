package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

type binding struct {
	target                  sql.Table
	varcharColumnMetas      []varcharColumnMeta
	loadFile                *stagedFile
	storeFile               *stagedFile
	createLoadTableTemplate *template.Template
	createStoreTableSQL     string
	mergeIntoSQL            string
	loadQuerySQL            string
	copyIntoLoadTableSQL    string
	copyIntoMergeTableSQL   string
	copyIntoTargetTableSQL  string
}

type transactor struct {
	fence    sql.Fence
	bindings []*binding
	cfg      *config

	round       int
	updateDelay time.Duration
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	var cfg = ep.Config.(*config)

	var d = &transactor{
		fence: fence,
		cfg:   cfg,
	}

	if d.updateDelay, err = sql.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
		return nil, err
	}

	s3client, err := d.cfg.toS3Client(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.Connect(ctx, d.cfg.toURI())
	if err != nil {
		return nil, fmt.Errorf("newTransactor pgx.Connect: %w", err)
	}
	defer conn.Close(ctx)

	tableVarchars, err := getVarcharLengths(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("getting existing varchar column lengths: %w", err)
	}

	for idx, target := range bindings {
		p := target.Path
		if len(p) == 1 {
			// No explicit schema set. Assume the default for identifying the table with respect to
			// the VARCHAR lengths introspection query, which always include a schema.
			p = []string{defaultSchema, p[0]}
		}

		// Redshift lowercases all table identifiers.
		identifier := strings.ToLower(rsDialect.Identifier(p...))
		if err = d.addBinding(
			ctx,
			idx,
			target,
			s3client,
			tableVarchars[identifier],
		); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", target.Path, err)
		}
	}

	return d, nil
}

// varcharColumnMeta contains metadata about Redshift varchar columns. Currently this is just the
// maximum length of the field as reported from the database, populated upon connector startup.
type varcharColumnMeta struct {
	identifier string
	maxLength  int
	isVarchar  bool
}

func (t *transactor) addBinding(
	ctx context.Context,
	bindingIdx int,
	target sql.Table,
	client *s3.Client,
	varchars map[string]int,
) error {
	var b = &binding{
		target:    target,
		loadFile:  newStagedFile(client, t.cfg.Bucket, t.cfg.BucketPath, target.KeyPtrs()),
		storeFile: newStagedFile(client, t.cfg.Bucket, t.cfg.BucketPath, target.Columns()),
	}

	// Render templates that require specific S3 "COPY INTO" parameters.
	for _, m := range []struct {
		sql             *string
		target          string
		columns         []*sql.Column
		truncateColumns bool
		stagedFile      *stagedFile
	}{
		{&b.copyIntoLoadTableSQL, fmt.Sprintf("flow_temp_table_%d", bindingIdx), target.KeyPtrs(), false, b.loadFile},
		{&b.copyIntoMergeTableSQL, fmt.Sprintf("flow_temp_table_%d", bindingIdx), target.Columns(), true, b.storeFile},
		{&b.copyIntoTargetTableSQL, target.Identifier, target.Columns(), true, b.storeFile},
	} {
		var sql strings.Builder
		if err := tplCopyFromS3.Execute(&sql, copyFromS3Params{
			Target:          m.target,
			Columns:         m.columns,
			ManifestURL:     m.stagedFile.fileURI(manifestFile),
			Config:          t.cfg,
			TruncateColumns: m.truncateColumns,
		}); err != nil {
			return err
		}
		*m.sql = sql.String()
	}

	// Render templates that rely only on the target table.
	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createStoreTableSQL, tplCreateStoreTable},
		{&b.mergeIntoSQL, tplMergeInto},
		{&b.loadQuerySQL, tplLoadQuery},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	// The load table template is re-evaluated every transaction to account for the specific string
	// lengths observed for string keys in the load key set.
	b.createLoadTableTemplate = tplCreateLoadTable

	// Retain column metadata information for this binding as a snapshot of the target table
	// configuration when the connector started, indexed in the same order as values will be
	// received from the runtime for Store requests. Only VARCHAR columns will have non-zero-valued
	// varcharColumnMeta.
	allColumns := target.Columns()
	columnMetas := make([]varcharColumnMeta, len(allColumns))
	if varchars != nil { // There may not be any varchar columns for this binding
		for idx, col := range allColumns {
			// If this column is not found in varchars, it must not have been a VARCHAR column.
			if existingMaxLength, ok := varchars[strings.ToLower(col.Identifier)]; ok { // Redshift lowercases all column identifiers
				columnMetas[idx] = varcharColumnMeta{
					identifier: col.Identifier,
					maxLength:  existingMaxLength,
					isVarchar:  true,
				}

				log.WithFields(log.Fields{
					"table":            b.target.Identifier,
					"column":           col.Identifier,
					"varcharMaxLength": existingMaxLength,
					"collection":       b.target.Source.String(),
					"field":            col.Field,
				}).Debug("matched string collection field to table VARCHAR column")
			}
		}
	}
	b.varcharColumnMetas = columnMetas

	t.bindings = append(t.bindings, b)
	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()
	gotLoads := false

	// Keep track of the maximum length of any string values encountered so the temporary load table
	// can be created with long enough string columns.
	maxStringLengths := make([]int, len(d.bindings))

	for it.Next() {
		gotLoads = true

		var b = d.bindings[it.Binding]
		b.loadFile.start()

		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

		for _, c := range converted {
			if s, ok := c.(string); ok {
				l := len(s)

				if l > redshiftVarcharMaxLength {
					return fmt.Errorf(
						"cannot load string keys with byte lengths longer than %d: collection for table %s had a string key with length %d",
						redshiftVarcharMaxLength,
						b.target.Identifier,
						l,
					)
				}

				if l > redshiftTextColumnLength && l > maxStringLengths[it.Binding] {
					maxStringLengths[it.Binding] = l
				}
			}
		}

		if err := b.loadFile.encodeRow(ctx, converted); err != nil {
			return fmt.Errorf("encoding row for load: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	if !gotLoads {
		// Early return as an optimization to avoid the remaining load queries if no loads were
		// received, as would be the case if all bindings of the materialization were set to use
		// delta updates.
		return nil
	}

	conn, err := pgx.Connect(ctx, d.cfg.toURI())
	if err != nil {
		return fmt.Errorf("load pgx.Connect: %w", err)
	}
	defer conn.Close(ctx)

	// Transaction for processing the loads.
	txn, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("load BeginTx: %w", err)
	}
	defer txn.Rollback(ctx)

	var subqueries []string
	for idx, b := range d.bindings {
		if !b.loadFile.started {
			// No loads for this binding.
			continue
		}

		subqueries = append(subqueries, b.loadQuerySQL)

		var createLoadTableSQL strings.Builder
		if err := b.createLoadTableTemplate.Execute(&createLoadTableSQL, loadTableParams{
			Target:        b.target,
			VarCharLength: maxStringLengths[idx],
		}); err != nil {
			return fmt.Errorf("evaluating create load table template: %w", err)
		} else if _, err := txn.Exec(ctx, createLoadTableSQL.String()); err != nil {
			return fmt.Errorf("creating load table for target table '%s': %w", b.target.Identifier, err)
		}

		delete, err := b.loadFile.flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing load file for binding[%d]: %w", idx, err)
		}
		defer delete(ctx)

		if _, err := txn.Exec(ctx, b.copyIntoLoadTableSQL); err != nil {
			return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, b.loadFile.prefix, b.target.Identifier, err)
		}
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	loadAllSQL := strings.Join(subqueries, "\nUNION ALL\n") + ";"
	rows, err := txn.Query(ctx, loadAllSQL)
	if err != nil {
		return fmt.Errorf("querying load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document json.RawMessage

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("commiting load transaction: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	ctx := it.Context()
	d.round++

	// hasUpdates is used to track if a given binding includes only insertions for this store round
	// or if it includes any updates. If it is only insertions a more efficient direct copy from S3
	// can be performed into the target table rather than copying into a staging table and merging
	// into the target table.
	hasUpdates := make([]bool, len(d.bindings))

	// varcharColumnUpdates records any VARCHAR columns that need their lengths increased. The keys
	// are table identifiers, and the values are a list of column identifiers that need altered.
	// Columns will only ever be to altered it to VARCHAR(MAX).
	varcharColumnUpdates := make(map[string][]string)

	for it.Next() {
		if it.Exists {
			hasUpdates[it.Binding] = true
		}

		var b = d.bindings[it.Binding]
		b.storeFile.start()

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		// See if we need to increase any VARCHAR column lengths.
		for idx, c := range converted {
			varcharMeta := b.varcharColumnMetas[idx]
			if varcharMeta.isVarchar {
				switch v := c.(type) {
				case string:
					// If the column is already at its maximum length, it can't be enlarged any
					// more. The string values will be truncated by Redshift when loading them into
					// the table.
					if len(v) > varcharMeta.maxLength && varcharMeta.maxLength != redshiftVarcharMaxLength {
						log.WithFields(log.Fields{
							"table":               b.target.Identifier,
							"column":              varcharMeta.identifier,
							"currentColumnLength": varcharMeta.maxLength,
							"stringValueLength":   len(v),
						}).Info("column will be altered to VARCHAR(MAX) to accommodate large string value")
						varcharColumnUpdates[b.target.Identifier] = append(varcharColumnUpdates[b.target.Identifier], varcharMeta.identifier)
						b.varcharColumnMetas[idx].maxLength = redshiftVarcharMaxLength // Do not need to alter this column again.
					}
				case nil:
					// Values for string fields may be null, in which case there is nothing to do.
					continue
				default:
					// Invariant: This value must either be a string or nil, since the column it is
					// going into is a VARCHAR.
					return nil, fmt.Errorf("expected type string or nil for column %s of table %s, got %T", varcharMeta.identifier, b.target.Identifier, c)
				}
			}
		}

		if err := b.storeFile.encodeRow(ctx, converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var err error
		if d.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, d.fence); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("evaluating fence update template: %w", err))
		}

		return nil, sql.CommitWithDelay(
			ctx,
			d.round,
			d.updateDelay,
			it.Total,
			func(ctx context.Context) error {
				return d.commit(ctx, fenceUpdate.String(), hasUpdates, varcharColumnUpdates)
			},
		)
	}, nil
}

func (d *transactor) commit(ctx context.Context, fenceUpdate string, hasUpdates []bool, varcharColumnUpdates map[string][]string) error {
	conn, err := pgx.Connect(ctx, d.cfg.toURI())
	if err != nil {
		return fmt.Errorf("store pgx.Connect: %w", err)
	}
	defer conn.Close(ctx)

	// Truncate strings within SUPER types by setting this option, since these have the same limits
	// on maximum VARCHAR lengths as table columns do. Notably, this will truncate any strings in
	// `flow_document` (stored as a SUPER column) that otherwise would prevent the row from being
	// added to the table.
	if _, err := conn.Exec(ctx, "SET json_parse_truncate_strings=ON;"); err != nil {
		return fmt.Errorf("configuring json_parse_truncate_strings=ON: %w", err)
	}

	// Update any columns that require setting to VARCHAR(MAX) for storing large strings. ALTER
	// TABLE ALTER COLUMN statements cannot be run inside transaction blocks.
	for table, updates := range varcharColumnUpdates {
		for _, column := range updates {
			if _, err := conn.Exec(ctx, fmt.Sprintf(varcharTableAlter, table, column)); err != nil {
				// It is possible that another shard of this materialization will have already
				// updated this column. Practically this means we will try to set a column that is
				// already VARCHAR(MAX) to VARCHAR(MAX), and Redshift returns a specific error in
				// this case that can be safely discarded.
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					if pgErr.Code == "0A000" && strings.Contains(pgErr.Message, "target column size should be different") {
						log.WithFields(log.Fields{
							"table":  table,
							"column": column,
						}).Info("attempted to alter column VARCHAR(MAX) but it was already VARCHAR(MAX)")
						continue
					}
				}

				return fmt.Errorf("altering size for column %s of table %s: %w", column, table, err)
			}

			log.WithFields(log.Fields{
				"table":  table,
				"column": column,
			}).Info("column altered to VARCHAR(MAX) to accommodate large string value")
		}
	}

	txn, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("store BeginTx: %w", err)
	}
	defer txn.Rollback(ctx)

	// If there are multiple materializations operating on different tables within the same database
	// using the same metadata table path, they will need to concurrently update the checkpoints
	// table. Acquiring a table-level lock prevents "serializable isolation violation" errors in
	// this case (they still happen even though the queries update different _rows_ in the same
	// table), although it means each materialization will have to take turns updating the
	// checkpoints table. For best performance, a single materialization per database should be
	// used, or separate materializations within the same database should use a different schema for
	// their metadata.
	if _, err := txn.Exec(ctx, fmt.Sprintf("lock %s;", rsDialect.Identifier(d.fence.TablePath...))); err != nil {
		return fmt.Errorf("obtaining checkpoints table lock: %w", err)
	}

	for idx, b := range d.bindings {
		if !b.storeFile.started {
			// No stores for this binding.
			continue
		}

		delete, err := b.storeFile.flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}
		defer delete(ctx)

		if hasUpdates[idx] {
			// Create the temporary table for staging values to merge into the target table.
			// Redshift actually supports transactional DDL for creating tables, so this can be
			// executed within the transaction.
			if _, err := txn.Exec(ctx, b.createStoreTableSQL); err != nil {
				return fmt.Errorf("creating store table: %w", err)
			}

			if _, err := txn.Exec(ctx, b.copyIntoMergeTableSQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, b.storeFile.prefix, b.target.Identifier, err)
			} else if _, err := txn.Exec(ctx, b.mergeIntoSQL); err != nil {
				return fmt.Errorf("merging to table '%s': %w", b.target.Identifier, err)
			}
		} else {
			// Can copy directly into the target table since all values are new.
			if _, err := txn.Exec(ctx, b.copyIntoTargetTableSQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, b.storeFile.prefix, b.target.Identifier, err)
			}
		}
	}

	if fenceRes, err := txn.Exec(ctx, fenceUpdate); err != nil {
		return fmt.Errorf("fetching fence update rows: %w", err)
	} else if fenceRes.RowsAffected() != 1 {
		return errors.New("this instance was fenced off by another")
	} else if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("committing store transaction: %w", err)
	}

	return nil
}

// handleCopyIntoErr queries the `sys_load_error_detail` table for relevant COPY INTO error details
// and returns a more useful error than the opaque error returned by Redshift. This function will
// always return an error. `sys_load_error_detail` is queried instead of `stl_load_errors` since it
// is available to both serverless and provisioned versions of Redshift, whereas `stl_load_errors`
// is only available on provisioned Redshift.
func handleCopyIntoErr(ctx context.Context, txn pgx.Tx, bucket, prefix, table string, copyIntoErr error) error {
	if strings.Contains(copyIntoErr.Error(), "Cannot COPY into nonexistent table") {
		// If the target table does not exist, there will be no information in
		// `sys_load_error_detail`, and the error message from Redshift is good enough as-is to
		// spell out the reason and table involved in the error.
		return copyIntoErr
	}

	// The transaction has failed. It must be finish being rolled back before using its underlying
	// connection again.
	txn.Rollback(ctx)
	conn := txn.Conn()

	log.WithFields(log.Fields{
		"table": table,
		"err":   copyIntoErr.Error(),
	}).Warn("COPY INTO error")

	loadErrInfo, err := getLoadErrorInfo(ctx, conn, bucket, prefix)
	if err != nil {
		return fmt.Errorf("COPY INTO error for table '%s' but could not query sys_load_error_detail: %w", table, err)
	}

	log.WithFields(log.Fields{
		"errMsg":    loadErrInfo.errMsg,
		"errCode":   loadErrInfo.errCode,
		"colName":   loadErrInfo.colName,
		"colType":   loadErrInfo.colType,
		"colLength": loadErrInfo.colLength,
	}).Warn("loadErrInfo")

	// See https://docs.aws.amazon.com/redshift/latest/dg/r_Load_Error_Reference.html for load error
	// codes.
	switch code := loadErrInfo.errCode; code {
	case 1204:
		// Input data exceeded the acceptable range for the data type. This is a case where the
		// column has some kind of length limit (like a VARCHAR(X)), but the input to the column is too
		// long.
		return fmt.Errorf(
			"cannot COPY INTO table '%s' column '%s' having type '%s' and allowable length '%s': %s (code %d)",
			table,
			loadErrInfo.colName,
			loadErrInfo.colType,
			loadErrInfo.colLength,
			loadErrInfo.errMsg,
			loadErrInfo.errCode,
		)
	case 1216:
		// General "Input line is not valid" error. This is almost always going to be because a very
		// large document is in the collection being materialized, and Redshift cannot parse single
		// JSON lines larger than 4MB.
		return fmt.Errorf(
			"cannot COPY INTO table '%s': %s (code %d)",
			table,
			loadErrInfo.errMsg,
			loadErrInfo.errCode,
		)
	case 1224:
		// A problem copying into a SUPER column. The most common cause is field of the root
		// document being excessively large (ex: an object field larger than 1MB, or having too many
		// attributes). May also happen for an individual selected field that is mapped as a SUPER
		// column type.
		return fmt.Errorf(
			"cannot COPY INTO table '%s' SUPER column '%s': %s (code %d)",
			table,
			loadErrInfo.colName,
			loadErrInfo.errMsg,
			loadErrInfo.errCode,
		)
	default:
		// Catch-all: Return general information from sys_load_error_detail. This will be pretty
		// helpful on its own. We'll want to specifically handle any other cases as we see them come
		// up frequently.
		return fmt.Errorf(
			"COPY INTO table `%s` failed with details from sys_load_error_detail: column_name: '%s', column_type: '%s', column_length: '%s', error_code: %d: error_message: %s",
			table,
			loadErrInfo.colName,
			loadErrInfo.colType,
			loadErrInfo.colLength,
			loadErrInfo.errCode,
			loadErrInfo.errMsg,
		)
	}
}

func (d *transactor) Destroy() {}
