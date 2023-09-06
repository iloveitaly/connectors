package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var spec *pf.MaterializationSpec
	var specJson, err = os.ReadFile("testdata/spec.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(specJson, &spec))

	var shape1 = sqlDriver.BuildTableShape(spec, 0, tableConfig{
		Table:    "table",
		Schema:   "schema",
		Delta:    false,
		database: "db",
	})
	var shape2 = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Table:    "table",
		Schema:   "schema",
		Delta:    true,
		database: "db",
	})
	shape2.Document = nil

	table1, err := sqlDriver.ResolveTable(shape1, duckDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, duckDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		tplCreateTargetTable,
		tplLoadQuery,
		tplStoreQuery,
	} {
		for _, tbl := range []sqlDriver.Table{table1, table2} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &s3Params{
				Table:  tbl,
				Bucket: "a-bucket",
				Key:    "key.jsonl",
			}))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Schema: "",
		Table:  "target_table_no_values_materialized",
		Delta:  false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, duckDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized storeUpdate" + " ---")
	require.NoError(t, tplStoreQuery.Execute(&snap, &s3Params{
		Table:  tableNoValues,
		Bucket: "a-bucket",
		Key:    "key.jsonl",
	}))
	snap.WriteString("--- End " + "target_table_no_values_materialized storeUpdate" + " ---\n\n")

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}

	snap.WriteString("--- Begin Fence Update ---")
	require.NoError(t, tplUpdateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---")

	cupaloy.SnapshotT(t, snap.String())
}
