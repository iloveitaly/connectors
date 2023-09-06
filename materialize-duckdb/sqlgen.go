package main

import (
	"regexp"
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

// Identifiers matching the this pattern do not need to be quoted. See
// https://docs.aws.amazon.com/redshift/latest/dg/r_names.html. Identifiers (table names and column
// names) are case-insensitive, even if they are quoted. They are always converted to lowercase by
// default.
var simpleIdentifierRegexp = regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`)

var duckDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER:  sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
		sql.NUMBER:   sql.NewStaticMapper("DOUBLE", sql.WithElementConverter(sql.StdStrToFloat())),
		sql.BOOLEAN:  sql.NewStaticMapper("BOOLEAN"),
		sql.OBJECT:   sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.ARRAY:    sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.BINARY:   sql.NewStaticMapper("BLOB"),
		sql.MULTIPLE: sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("VARCHAR"),
			WithFormat: map[string]sql.TypeMapper{
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMP WITH TIME ZONE"),
				"duration":  sql.NewStaticMapper("INTERVAL"),
				"time":      sql.NewStaticMapper("TIME"),
				"uuid":      sql.NewStaticMapper("UUID"),
			},
		},
	}

	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(DUCKDB_RESERVED_WORDS, strings.ToUpper(s))
				},
				sql.QuoteTransform("\"", "\"\""),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper: mapper,
	}
}()

type s3Params struct {
	sql.Table
	Bucket string
	Key    string
}

var (
	tplAll = sql.MustParseTemplate(duckDialect, "root", `
-- Templated creation of a materialized table definition. DuckDB does not currently support comments.

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.DDL}}
{{- end }}
{{- if not $.DeltaUpdates }},

	PRIMARY KEY (
{{- range $ind, $key := $.Keys }}
	{{- if $ind }}, {{end -}}
	{{$key.Identifier}}
{{- end -}}
)
{{- end }}
);
{{ end }}

-- Templated query for merging documents from S3 into the target table.

{{ define "storeQuery" }}
INSERT INTO {{$.Identifier}} BY NAME
SELECT * FROM read_json(
	's3://{{$.Bucket}}/{{$.Key}}',
	format='newline_delimited',
	columns={
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}: '{{$col.DDL}}'
	{{- end }}
	}
)
{{- if not $.DeltaUpdates }}
ON CONFLICT ({{ range $ind, $key := $.Keys }}
	{{- if $ind }}, {{ end -}}
	{{ $key.Identifier }}
{{- end }})
DO UPDATE SET
{{- range $ind, $val := $.Values -}}
{{- if $ind }}, {{ end }}
	{{ $val.Identifier }} = excluded.{{ $val.Identifier }}
{{- end -}}
{{- if $.Document -}}{{ if $.Values }}, {{ end }}
	{{ $.Document.Identifier}} = excluded.{{ $.Document.Identifier }}
{{- end }}
{{- end -}};
{{ end }}

-- Templated query which joins keys from the staged keys file with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }} AS binding, l.{{ $.Document.Identifier }} AS doc
FROM {{ $.Identifier }} AS l
JOIN read_json(
	's3://{{$.Bucket}}/{{$.Key}}',
	format='newline_delimited',
	columns={
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{$key.Identifier}}: '{{$key.DDL}}'
	{{- end }}
	}
) AS r
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end -}}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{- end }}
{{ end }}

-- Templated update of a fence checkpoint.

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND   key_begin = {{ $.KeyBegin }}
	AND   key_end   = {{ $.KeyEnd }}
	AND   fence     = {{ $.Fence }};
{{ end }}
`)
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplStoreQuery        = tplAll.Lookup("storeQuery")
	tplUpdateFence       = tplAll.Lookup("updateFence")
)
