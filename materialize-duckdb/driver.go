package main

import (
	"bufio"
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsHttp "github.com/aws/smithy-go/transport/http"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	_ "github.com/marcboeker/go-duckdb"
)

const (
	loadFile = "loaded.json"
)

type config struct {
	Token              string `json:"token" jsonschema:"title=Motherduck Service Token,description=Service token for authenticating with MotherDuck." jsonschema_extras:"secret=true,order=0"`
	Database           string `json:"database" jsonschema:"title=Database,description=The database to materialize to." jsonschema_extras:"order=1"`
	Schema             string `json:"schema" jsonschema:"title=Database Schema,default=main,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables" jsonschema_extras:"order=2"`
	Bucket             string `json:"bucket" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads." jsonschema_extras:"order=3"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for reading and writing data to the S3 staging bucket." jsonschema_extras:"order=4"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for reading and writing data to the S3 staging bucket." jsonschema_extras:"secret=true,order=5"`
	Region             string `json:"region" jsonschema:"title=S3 Bucket Region,description=Region of the S3 staging bucket." jsonschema_extras:"order=6"`
	BucketPath         string `json:"bucketPath,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in S3." jsonschema_extras:"order=7"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"token", c.Token},
		{"database", c.Database},
		{"schema", c.Schema},
		{"bucket", c.Bucket},
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Sanity check that the provided authentication token is a well-formed JWT. It it's not, the
	// sql.Open function used elsewhere will return an error string that is difficult to comprehend.
	// This check isn't perfect but it should catch most of the blatantly obvious error cases.
	for _, part := range strings.Split(c.Token, ".") {
		if _, err := base64.RawURLEncoding.DecodeString(part); err != nil {
			return fmt.Errorf("invalid token: must be a base64 encoded JWT")
		}
	}

	if c.BucketPath != "" {
		// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
		// chars in the URI and so that the object key does not start with a /.
		c.BucketPath = strings.TrimPrefix(c.BucketPath, "/")
	}

	return nil
}

// setupCmds are the common commands needed for the connector to work with MotherDuck.
func (c *config) setupCmds() []string {
	return []string{
		"INSTALL httpfs;",
		"LOAD httpfs;",
		"INSTALL 'json'",
		"LOAD 'json'",
		fmt.Sprintf("SET s3_access_key_id='%s'", c.AWSAccessKeyID),
		fmt.Sprintf("SET s3_secret_access_key='%s';", c.AWSSecretAccessKey),
		fmt.Sprintf("SET s3_region='%s';", c.Region),
	}
}

func (c *config) db() (*stdsql.DB, error) {
	db, err := stdsql.Open("duckdb", fmt.Sprintf("md:%s?motherduck_token=%s", c.Database, c.Token))
	if err != nil {
		if strings.Contains(err.Error(), "Jwt header is an invalid JSON (UNAUTHENTICATED)") {
			return nil, fmt.Errorf("invalid token: unauthenticated")
		}
		return nil, err
	}

	return db, err
}

func (c *config) configuredDb(ctx context.Context) (*stdsql.DB, error) {
	db, err := c.db()
	if err != nil {
		return nil, err
	}

	for idx, c := range c.setupCmds() {
		if _, err := db.ExecContext(ctx, c); err != nil {
			return nil, fmt.Errorf("executing setup command %d: %w", idx, err)
		}
	}

	return db, nil
}

func (c *config) configuredConn(ctx context.Context, db *stdsql.DB) (*stdsql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening database connection: %w", err)
	}

	for idx, c := range c.setupCmds() {
		if _, err := conn.ExecContext(ctx, c); err != nil {
			return nil, fmt.Errorf("executing setup command %d: %w", idx, err)
		}
	}

	return conn, nil
}

func (c *config) toS3Client(ctx context.Context) (*s3.Client, error) {
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Region),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg), nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`

	database string
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to the endpoint schema. This will be over-written by a present `schema` property
		// within `raw`.
		Schema:   ep.Config.(*config).Schema,
		database: ep.Config.(*config).Database,
	}
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.database, c.Schema, c.Table}
}

func (c tableConfig) GetAdditionalSql() string {
	return ""
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newDuckDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-duckdb",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("could not parse endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"database": cfg.Database,
			}).Info("opening database")

			metaSpecs, metaCheckpoints := sql.MetaTables([]string{cfg.Database, cfg.Schema})

			db, err := cfg.configuredDb(ctx)
			if err != nil {
				return nil, fmt.Errorf("opening database: %w", err)
			}

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             duckDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client{db: db},
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				CheckPrerequisites:  prereqs,
				Tenant:              tenant,
			}, nil
		},
	}
}

func prereqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if db, err := cfg.configuredDb(ctx); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(pingCtx); err != nil {
		errs.Err(err)
	} else {
		defer db.Close()
		var c int
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM information_schema.schemata WHERE schema_name = '%s'", cfg.Schema)).Scan(&c); err != nil {
			if errors.Is(err, stdsql.ErrNoRows) {
				errs.Err(fmt.Errorf("schema %q does not exist", cfg.Schema))
			} else {
				errs.Err(err)
			}
		}
	}

	s3client, err := cfg.toS3Client(ctx)
	if err != nil {
		// This is not caused by invalid S3 credentials, and would most likely be a logic error in
		// the connector code.
		errs.Err(err)
		return errs
	}

	testCol := &sql.Column{}
	testCol.Field = "test"

	s3file := newStagedFile(s3client, cfg.Bucket, cfg.BucketPath, []*sql.Column{testCol})
	s3file.start(ctx)

	var awsErr *awsHttp.ResponseError

	objectDir := path.Join(cfg.Bucket, cfg.BucketPath)

	if err := s3file.encodeRow([]interface{}{[]byte("test")}); err != nil {
		// This won't err immediately until the file is flushed in the case of having the wrong
		// bucket or authorization configured, and would most likely be caused by a logic error in
		// the connector code.
		errs.Err(err)
	} else if delete, err := s3file.flush(); err != nil {
		if errors.As(err, &awsErr) {
			// Handling for the two most common cases: The bucket doesn't exist, or the bucket does
			// exist but the configured credentials aren't authorized to write to it.
			if awsErr.Response.Response.StatusCode == http.StatusNotFound {
				err = fmt.Errorf("bucket %q does not exist", cfg.Bucket)
			} else if awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to write to bucket %q", objectDir)
			}
		}

		errs.Err(err)
	} else {
		// Verify that the created object can be read and deleted. The unauthorized case is handled
		// & formatted specifically in these checks since the existence of the bucket has already
		// been verified by creating the temporary test object.
		if _, err := s3client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(s3file.key),
		}); err != nil {
			if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to read from bucket %q", objectDir)
			}
			errs.Err(err)
		}

		if err := delete(ctx); err != nil {
			if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to delete from bucket %q", objectDir)
			}
			errs.Err(err)
		}
	}

	return errs
}

type client struct {
	db *stdsql.DB
}

func (c client) AddColumnToTable(ctx context.Context, dryRun bool, tableIdentifier string, columnIdentifier string, columnDDL string) (string, error) {
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;",
		tableIdentifier,
		columnIdentifier,
		columnDDL,
	)

	if !dryRun {
		if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, []string{query}) }); err != nil {
			return "", err
		}
	}

	return query, nil
}

func (c client) DropNotNullForColumn(ctx context.Context, dryRun bool, table sql.Table, column sql.Column) (string, error) {
	query := fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
		table.Identifier,
		column.Identifier,
	)

	if !dryRun {
		if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, []string{query}) }); err != nil {
			return "", err
		}
	}

	return query, nil
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		specB64, version, err = sql.StdFetchSpecAndVersion(ctx, db, specs, materialization)
		return err
	})
	return
}

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	// Evidently you can't query a table immediately from within the same connection that created
	// table, so all of the table creation statements are run first, followed by the spec insert
	// query from a separate connection. This allows for the metadata table to be created and the
	// spec inserted in a single apply invocation, otherwise it would fail with a "table does not
	// exist" error.
	if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements[:len(statements)-1]) }); err != nil {
		return err
	}

	return c.withDB(func(db *stdsql.DB) error {
		if _, err := db.ExecContext(ctx, statements[len(statements)-1]); err != nil {
			return fmt.Errorf("updating spec: %w", err)
		}
		return nil
	})
}

func (c client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	var err = c.withDB(func(db *stdsql.DB) error {
		var err error
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence, base64.StdEncoding.DecodeString)
		return err
	})
	return fence, err
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	return fn(c.db)
}

type transactor struct {
	cfg *config

	fence     sql.Fence
	loadConn  *stdsql.Conn
	storeConn *stdsql.Conn

	bindings []*binding
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	cfg := ep.Config.(*config)

	s3client, err := cfg.toS3Client(ctx)
	if err != nil {
		return nil, err
	}

	db, err := cfg.db()
	if err != nil {
		return nil, err
	}

	loadConn, err := cfg.configuredConn(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("creating load connection: %w", err)
	}

	storeConn, err := cfg.configuredConn(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("creating store connection: %w", err)
	}

	d := &transactor{
		cfg:       cfg,
		loadConn:  loadConn,
		storeConn: storeConn,
		fence:     fence,
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding, cfg.Bucket, cfg.BucketPath, s3client); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	return d, nil
}

type binding struct {
	target        sql.Table
	loadFile      *stagedFile
	storeFile     *stagedFile
	loadQuerySql  string
	storeQuerySql string
}

func (t *transactor) addBinding(
	ctx context.Context,
	target sql.Table,
	bucket string,
	bucketPath string,
	client *s3.Client,
) error {
	b := &binding{
		target:    target,
		loadFile:  newStagedFile(client, bucket, bucketPath, target.KeyPtrs()),
		storeFile: newStagedFile(client, bucket, bucketPath, target.Columns()),
	}

	var loadQuery strings.Builder
	if err := tplLoadQuery.Execute(&loadQuery, &s3Params{
		Table:  target,
		Bucket: bucket,
		Key:    b.loadFile.key,
	}); err != nil {
		return err
	}

	var storeQuery strings.Builder
	if err := tplStoreQuery.Execute(&storeQuery, &s3Params{
		Table:  target,
		Bucket: bucket,
		Key:    b.storeFile.key,
	}); err != nil {
		return err
	}

	b.loadQuerySql = loadQuery.String()
	b.storeQuerySql = storeQuery.String()

	t.bindings = append(t.bindings, b)
	return nil
}

type bindingDoc struct {
	Binding int             `json:"binding"`
	Doc     json.RawMessage `json:"doc"`
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		b := d.bindings[it.Binding]
		b.loadFile.start(ctx)

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = b.loadFile.encodeRow(converted); err != nil {
			return fmt.Errorf("encoding Load key to scratch file: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	var subqueries []string
	for _, b := range d.bindings {
		if !b.loadFile.started {
			// Pass.
		} else if delete, err := b.loadFile.flush(); err != nil {
			return fmt.Errorf("load.stage(): %w", err)
		} else {
			defer delete(ctx)
			subqueries = append(subqueries, b.loadQuerySql)
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}
	loadAllSql := strings.Join(subqueries, "\nUNION ALL\n")

	rows, err := d.loadConn.QueryContext(ctx, fmt.Sprintf("COPY (%s) to 'loaded.json' (FORMAT JSON);", loadAllSql))
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	var loadQueryCount int
	for rows.Next() {
		if err := rows.Scan(&loadQueryCount); err != nil {
			return fmt.Errorf("scanning loadQueryCount: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	loadedFile, err := os.Open(loadFile)
	if err != nil {
		return fmt.Errorf("opening loaded file: %w", err)
	}

	dec := json.NewDecoder(bufio.NewReader(loadedFile))
	loadedCount := 0
	for {
		var doc bindingDoc
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("scanning loaded document from file: %w", err)
		}
		if err = loaded(doc.Binding, doc.Doc); err != nil {
			return fmt.Errorf("sending loaded document: %w", err)
		}
		loadedCount += 1
	}

	// Note: Not deferred immediately after os.Open to make it easier to actually handle the
	// returned errors. If there's an error between then and here the connector will exit.
	if err := loadedFile.Close(); err != nil {
		return err
	} else if err := os.Remove(loadFile); err != nil {
		return fmt.Errorf("removing load scratch file: %w", err)
	}

	// Sanity check; these counts should always be equal.
	if loadedCount != loadQueryCount {
		return fmt.Errorf("mismatched loadedCount vs loadQueryCount: %d vs %d", loadedCount, loadQueryCount)
	}

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	ctx := it.Context()

	cleanups := make([]func(context.Context) error, len(d.bindings))
	lastBinding := -1
	flushBinding := func() error {
		cleanup, err := d.bindings[lastBinding].storeFile.flush()
		if err != nil {
			return fmt.Errorf("flushing binding %d: %w", lastBinding, err)
		}
		cleanups[lastBinding] = cleanup

		return nil
	}

	for it.Next() {
		if lastBinding != -1 && lastBinding != it.Binding {
			if err := flushBinding(); err != nil {
				return nil, err
			}
		}

		b := d.bindings[it.Binding]
		b.storeFile.start(ctx)

		if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err := b.storeFile.encodeRow(converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}

		lastBinding = it.Binding
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	// Flush the final binding.
	if err := flushBinding(); err != nil {
		return nil, err
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, _ <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var err error
		if d.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, d.fence); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("evaluating fence template: %w", err))
		}

		return nil, pf.RunAsyncOperation(func() error {
			txn, err := d.storeConn.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("store BeginTx: %w", err)
			}
			defer txn.Rollback()

			for idx, b := range d.bindings {
				if cleanups[idx] == nil {
					// No stores for this binding.
					continue
				}
				defer cleanups[idx](ctx)
				if _, err := txn.ExecContext(ctx, b.storeQuerySql); err != nil {
					return fmt.Errorf("executing store query for binding[%d]: %w", idx, err)
				}
			}
			if res, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("updating checkpoints: %w", err)
			} else if rows, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("getting fence update rows affected: %w", err)
			} else if rows != 1 {
				return fmt.Errorf("this instance was fenced off by another")
			} else if err := txn.Commit(); err != nil {
				return fmt.Errorf("committing store transaction: %w", err)
			}

			return nil
		})
	}, nil
}

func (d *transactor) Destroy() {}
