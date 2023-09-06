package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	stdsql "database/sql"

	_ "github.com/marcboeker/go-duckdb"
)

var deleteTable = flag.Bool("delete", false, "delete the table instead of dumping its contents")
var deleteSpecs = flag.Bool("delete-specs", false, "stored materialize checkpoint and specs")

func main() {
	flag.Parse()
	tables := flag.Args()
	if len(tables) != 1 {
		log.Fatal("must provide table name as an argument")
	}

	token, ok := os.LookupEnv("DUCKDB_TOKEN")
	if !ok {
		log.Fatal("missing DUCKDB_TOKEN environment variable")
	}

	database, ok := os.LookupEnv("DUCKDB_DATABASE")
	if !ok {
		log.Fatal("missing DUCKDB_DATABASE environment variable")
	}

	schema, ok := os.LookupEnv("DUCKDB_SCHEMA")
	if !ok {
		log.Fatal("missing DUCKDB_SCHEMA environment variable")
	}

	db, err := stdsql.Open("duckdb", fmt.Sprintf("md:%s?motherduck_token=%s", database, token))
	if err != nil {
		log.Fatal(fmt.Errorf("connecting to db: %w", err))
	}
	defer db.Close()

	tableIdentifier := fmt.Sprintf("%q.%q.%q", database, schema, tables[0])

	// Handle cleanup cases of for dropping a table and deleting the stored materialization spec &
	// checkpoint if flags were provided.
	if *deleteTable {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableIdentifier)); err != nil {
			fmt.Println(fmt.Errorf("could not drop table %s: %w", tables[0], err))
		}
		os.Exit(0)
	} else if *deleteSpecs {
		if _, err := db.Exec(
			fmt.Sprintf(
				"delete from %[1]q.%[2]q.flow_checkpoints_v1 where materialization='tests/materialize-duckdb/materialize';"+
					"delete from %[1]q.%[2]q.flow_materializations_v2 where materialization='tests/materialize-duckdb/materialize';",
				database,
				schema,
			),
		); err != nil {
			fmt.Println(fmt.Errorf("could not delete stored materialization spec/checkpoint: %w", err))
		}

		os.Exit(0)
	}

	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s ORDER BY ID`, tableIdentifier))
	if err != nil {
		log.Fatal(fmt.Errorf("running query: %w", err))
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(fmt.Errorf("reading columns: %w", err))
	}

	data := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range data {
		ptrs[i] = &data[i]
	}

	queriedRows := []map[string]any{}

	for rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			log.Fatal("scanning row: %w", err)
		}
		row := make(map[string]any)
		for idx := range data {
			d := data[idx]
			if t, ok := d.(time.Time); ok {
				// Go JSON encoding apparently doesn't like timestamps with years 9999.
				d = t.UTC().String()
			}
			row[cols[idx]] = d
		}

		queriedRows = append(queriedRows, row)
	}
	rows.Close()

	if err := json.NewEncoder(os.Stdout).Encode(queriedRows); err != nil {
		log.Fatal(fmt.Errorf("writing output: %w", err))
	}
}
