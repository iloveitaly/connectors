//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"

	_ "github.com/microsoft/go-mssqldb"
)

//go:generate ../materialize-boilerplate/testdata/generate-spec-proto.sh testdata/apply-changes.flow.yaml

func testConfig() *config {
	return &config{
		Address:  "localhost:1433",
		User:     "sa",
		Password: "!Flow1234",
		Database: "master",
	}
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db, err := stdsql.Open("sqlserver", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newSqlServerDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, "dbo", resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Table)))

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = 'test/sqlite'",
				testDialect.Identifier("flow_materializations_v2"),
			))
		},
	)
}

func TestApplyChanges(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()
	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	resourceConfig := tableConfig{
		Table: "data_types",
	}
	resourceConfigJson, err := json.Marshal(resourceConfig)
	require.NoError(t, err)

	db, err := stdsql.Open("sqlserver", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	cleanup := func() {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s;", testDialect.Identifier(resourceConfig.Table)))
	}
	cleanup()
	t.Cleanup(cleanup)

	// These data types are somewhat interesting as test-cases to make sure we don't lose anything
	// about them when dropping nullability constraints, since we must re-state the column
	// definition when doing that.
	q := fmt.Sprintf(`
	CREATE TABLE %s(
		vc123 VARCHAR(123) COLLATE Latin1_General_100_BIN2 NOT NULL,
		vcMax VARCHAR(MAX) NOT NULL,
		t TEXT NOT NULL,
		nvc123 NVARCHAR(123) NOT NULL,
		nvcMax NVARCHAR(MAX) NOT NULL,
		c10 CHAR(10) NOT NULL,
		n38 NUMERIC(38,0) NOT NULL,
		d DECIMAL NOT NULL,
		n NUMERIC NOT NULL,
		mn NUMERIC(30,5) NOT NULL,
		money MONEY NOT NULL,
		datetime DATETIME2(2) NOT NULL,
		time TIME(3) NOT NULL,
		dtoffset DATETIMEOFFSET(5) NOT NULL,
		dbl DOUBLE PRECISION NOT NULL
	);
	`,
		testDialect.Identifier(resourceConfig.Table),
	)

	_, err = db.ExecContext(ctx, q)
	require.NoError(t, err)

	// Initial snapshot of the table, as created.
	original, err := snapshotTable(t, ctx, db, cfg.Database, "dbo", resourceConfig.Table)
	require.NoError(t, err)

	// Apply the spec, which will drop the nullability constraints because none of the fields are in
	// the materialized collection.
	specBytes, err := os.ReadFile("testdata/generated_specs/apply-changes.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	spec.ConfigJson = configJson
	spec.Bindings[0].ResourceConfigJson = resourceConfigJson
	spec.Bindings[0].ResourcePath = resourceConfig.Path()

	_, err = newSqlServerDriver().Apply(ctx, &pm.Request_Apply{
		Materialization: &spec,
		Version:         "",
	})
	require.NoError(t, err)

	// Snapshot of the table after our apply.
	new, err := snapshotTable(t, ctx, db, cfg.Database, "dbo", resourceConfig.Table)
	require.NoError(t, err)

	var snap strings.Builder
	snap.WriteString("Pre-Apply Table Schema:\n")
	snap.WriteString(original)
	snap.WriteString("\nPost-Apply Table Schema:\n")
	snap.WriteString(new)
	cupaloy.SnapshotT(t, snap.String())
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()
	var dialect = testDialect
	var templates = renderTemplates(dialect)

	c, err := newClient(ctx, &sql.Endpoint{Config: testConfig(), Dialect: dialect})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{"temp_test_fencing_checkpoints"},
		dialect,
		templates.createTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := templates.updateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return c.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
		func(table sql.Table) (out string, err error) {
			out, err = sql.StdDumpTable(ctx, c.(*client).db, table)
			// SQLServer quotes "checkpoint" because it is a reserved word. The
			// snapshots for the test expect a checkpoint without quotes, so this is
			// a hack to allow this test to proceed
			return strings.Replace(out, "\"checkpoint\"", "checkpoint", 1), err
		},
	)
}

func TestPrereqs(t *testing.T) {
	cfg := testConfig()

	tests := []struct {
		name string
		cfg  func(config) *config
		want []string
	}{
		{
			name: "valid",
			cfg:  func(cfg config) *config { return &cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) *config {
				cfg.User = "wrong" + cfg.User
				return &cfg
			},
			want: []string{"Login failed for user 'wrongsa'"},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) *config {
				cfg.Password = "wrong" + cfg.Password
				return &cfg
			},
			want: []string{"Login failed for user 'sa'"},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) *config {
				cfg.Database = "wrong" + cfg.Database
				return &cfg
			},
			want: []string{"Cannot open database \"wrongflow\" that was requested by the login."},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) *config {
				cfg.Address = "wrong." + cfg.Address
				return &cfg
			},
			want: []string{fmt.Sprintf("host at address %q cannot be found", "wrong."+cfg.Address)},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg(*cfg)

			c, err := newClient(ctx, &sql.Endpoint{Config: cfg})
			require.NoError(t, err)
			defer c.Close()

			var actual = c.PreReqs(ctx).Unwrap()

			require.Equal(t, len(tt.want), len(actual))
			for i := 0; i < len(tt.want); i++ {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}

func snapshotTable(t *testing.T, ctx context.Context, db *stdsql.DB, catalog string, schema string, name string) (string, error) {
	t.Helper()

	cols, err := tableDetails(ctx, db, testDialect, catalog, schema, name)
	require.NoError(t, err)

	colSlice := make([]foundColumn, 0, len(cols))
	for _, c := range cols {
		colSlice = append(colSlice, c)
	}

	slices.SortFunc(colSlice, func(a, b foundColumn) int {
		return strings.Compare(a.Name, b.Name)
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, c := range colSlice {
		if err := enc.Encode(c); err != nil {
			return "", err
		}
	}

	return out.String(), nil
}
