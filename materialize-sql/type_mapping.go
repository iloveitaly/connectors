package sql

import (
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/materialize-boilerplate/validate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// FlatType is a flattened, database-friendly representation of a document location's type.
// It differs from JSON types by:
// * Having a single type, with cases like "JSON string OR integer" delegated to a MULTIPLE case.
// * Hoisting JSON `null` out of the type representation and into a separate orthogonal concern.
type FlatType string

// FlatType constants that are used by ColumnMapper
const (
	ARRAY    FlatType = "array"
	BINARY   FlatType = "binary"
	BOOLEAN  FlatType = "boolean"
	INTEGER  FlatType = "integer"
	MULTIPLE FlatType = "multiple"
	NEVER    FlatType = "never"
	NUMBER   FlatType = "number"
	OBJECT   FlatType = "object"
	STRING   FlatType = "string"
)

// Projection lifts a pf.Projection into a form that's more easily worked with for SQL column mapping.
type Projection struct {
	pf.Projection
	// Comment for this projection.
	Comment string
	// RawFieldConfig is (optional) field configuration supplied within the field selection.
	RawFieldConfig json.RawMessage
}

// BuildProjections returns the Projections extracted from a Binding.
func BuildProjections(spec *pf.MaterializationSpec_Binding) (keys, values []Projection, document *Projection) {
	var do = func(field string) Projection {
		var p = Projection{
			Projection:     *spec.Collection.GetProjection(field),
			RawFieldConfig: spec.FieldSelection.FieldConfigJsonMap[field],
		}

		var source = "auto-generated"
		if p.Explicit {
			source = "user-provided"
		}
		p.Comment = fmt.Sprintf("%s projection of JSON at: %s with inferred types: %s",
			source, p.Ptr, p.Inference.Types)

		if p.Inference.Description != "" {
			p.Comment = p.Inference.Description + "\n" + p.Comment
		}
		if p.Inference.Title != "" {
			p.Comment = p.Inference.Title + "\n" + p.Comment
		}

		return p
	}

	for _, field := range spec.FieldSelection.Keys {
		keys = append(keys, do(field))
	}
	for _, field := range spec.FieldSelection.Values {
		values = append(values, do(field))
	}
	if field := spec.FieldSelection.Document; field != "" {
		document = new(Projection)
		*document = do(field)
	}

	return
}

// AsFlatType returns the Projection's FlatType.
func (p *Projection) AsFlatType() (_ FlatType, mustExist bool) {
	mustExist = p.Inference.Exists == pf.Inference_MUST
	if slices.Contains(p.Inference.Types, "null") {
		mustExist = false
	}

	// Compatible numeric formatted strings can be materialized as either integers or numbers,
	// depending on the format string.
	if _, ok := validate.AsFormattedNumeric(&p.Projection); ok && !p.IsPrimaryKey {
		return STRING, mustExist
	}

	var types []FlatType
	for _, ty := range p.Inference.Types {
		switch ty {
		case "string":
			types = append(types, STRING)
		case "integer":
			types = append(types, INTEGER)
		case "number", "fractional":
			types = append(types, NUMBER)
		case "boolean":
			types = append(types, BOOLEAN)
		case "object":
			types = append(types, OBJECT)
		case "array":
			types = append(types, ARRAY)
		}
	}

	switch len(types) {
	case 0:
		return NEVER, false
	case 1:
		return types[0], mustExist
	default:
		return MULTIPLE, mustExist
	}
}

type MappedType struct {
	// DDL is the "CREATE TABLE" DDL type for this mapping, suited for direct inclusion in raw SQL.
	DDL string
	// Converter of tuple elements for this mapping, into SQL runtime values.
	Converter ElementConverter `json:"-"`
	// ParsedFieldConfig is a Dialect-defined parsed implementation of the (optional)
	// additional field configuration supplied within the field selection.
	ParsedFieldConfig interface{}
}

// ElementConverter maps from a TupleElement into a runtime type instance that's compatible with the SQL driver.
type ElementConverter func(tuple.TupleElement) (interface{}, error)

// TupleConverter maps from a Tuple into a slice of runtime type instances that are compatible with the SQL driver.
type TupleConverter func(tuple.Tuple) ([]interface{}, error)

// NewTupleConverter builds a TupleConverter from an ordered list of ElementConverter.
func NewTupleConverter(e ...ElementConverter) TupleConverter {
	return func(t tuple.Tuple) (out []interface{}, err error) {
		out = make([]interface{}, len(e))
		for i := range e {
			if out[i], err = e[i](t[i]); err != nil {
				return nil, fmt.Errorf("converting tuple index %d: %w", i, err)
			}
		}
		return out, nil
	}
}

// StaticMapper is a TypeMapper which returns a consistent MappedType.
type StaticMapper MappedType

var _ TypeMapper = StaticMapper{}

type StaticMapperOption func(*StaticMapper)

func (sm StaticMapper) MapType(*Projection) (MappedType, error) {
	return (MappedType)(sm), nil
}

func NewStaticMapper(ddl string, opts ...StaticMapperOption) StaticMapper {
	sm := StaticMapper{
		DDL:               ddl,
		Converter:         func(te tuple.TupleElement) (interface{}, error) { return te, nil },
		ParsedFieldConfig: nil,
	}

	for _, o := range opts {
		o(&sm)
	}

	return sm
}

func WithElementConverter(converter ElementConverter) StaticMapperOption {
	return func(sm *StaticMapper) {
		sm.Converter = converter
	}
}

// JsonBytesConverter serializes a value to raw JSON bytes for storage in an endpoint compatible
// with JSON bytes.
func JsonBytesConverter(te tuple.TupleElement) (interface{}, error) {
	switch ii := te.(type) {
	case []byte:
		return json.RawMessage(ii), nil
	case json.RawMessage:
		return ii, nil
	case nil:
		return json.RawMessage(nil), nil
	default:
		bytes, err := json.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("could not serialize %q as json bytes: %w", te, err)
		}

		return json.RawMessage(bytes), nil
	}
}

// StringCastConverter builds an ElementConverter from the string-handling callback. Any non-string
// types are returned without modification. This should be used for converting fields that have a
// string type and one additional type. The callback should convert the string into the desired type
// per the endpoint's requirements.
func StringCastConverter(fn func(string) (interface{}, error)) ElementConverter {
	return func(te tuple.TupleElement) (interface{}, error) {
		switch tt := te.(type) {
		case string:
			return fn(tt)
		default:
			return te, nil
		}
	}
}

func Compose(upper ElementConverter, lower ElementConverter) ElementConverter {
	return func(te tuple.TupleElement) (interface{}, error) {
		var a, err = lower(te)
		if err != nil {
			return nil, err
		}

		return upper(a)
	}
}

// StdByteArrayToStr builds an ElementConverter that converts []byte to string.
func StdByteArrayToStr(te tuple.TupleElement) (interface{}, error) {
	switch tt := te.(type) {
	case []byte:
		if tt == nil {
			return nil, nil
		}
		return string(tt), nil
	case json.RawMessage:
		if tt == nil {
			return nil, nil
		}
		return string(tt), nil
	default:
		return te, nil
	}
}

// StdStrToInt builds an ElementConverter that attempts to convert a string into a big.Int. Strings
// formatted as integers are often larger than the maximum that can be represented by an int64. The
// concrete type of the return value is a big.Int which will be represented correctly as JSON, but
// may not be directly usable by a SQL driver as a parameter.
func StdStrToInt() ElementConverter {
	return StringCastConverter(func(str string) (interface{}, error) {
		// Strings ending in a 0 decimal part like "1.0" or "3.00" are considered valid as integers
		// per JSON specification so we must handle this possibility here. Anything after the
		// decimal is discarded on the assumption that Flow has validated the data and verified that
		// the decimal component is all 0's.
		if idx := strings.Index(str, "."); idx != -1 {
			str = str[:idx]
		}

		var i big.Int
		out, ok := i.SetString(str, 10)
		if !ok {
			return nil, fmt.Errorf("could not convert %q to big.Int", str)
		}
		return out, nil
	})
}

// StdStrToFloat builds an ElementConverter that attempts to convert a string into an float64 value.
// It can be used for endpoints that do not require more digits than an float64 can provide.
func StdStrToFloat() ElementConverter {
	return StringCastConverter(func(str string) (interface{}, error) {
		out, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return nil, fmt.Errorf("could not convert %q to float64: %w", str, err)
		}

		return out, nil
	})
}

const (
	minimumTimestamp = "0001-01-01T00:00:00Z"
	minimumDate      = "0001-01-01"
)

// ClampDatetime provides handling for endpoints that do not accept "0000" as a year by replacing
// these datetimes with minimumTimestamp.
func ClampDatetime() ElementConverter {
	return StringCastConverter(func(str string) (interface{}, error) {
		if parsed, err := time.Parse(time.RFC3339Nano, str); err != nil {
			return nil, err
		} else if parsed.Year() == 0 {
			return minimumTimestamp, nil
		}
		return str, nil
	})
}

// ClampDate is like ClampDatetime but just for dates.
func ClampDate() ElementConverter {
	return StringCastConverter(func(str string) (interface{}, error) {
		if parsed, err := time.Parse(time.DateOnly, str); err != nil {
			return nil, err
		} else if parsed.Year() == 0 {
			return minimumDate, nil
		}
		return str, nil
	})
}

// MaybeNullableMapper wraps a ColumnMapper to add "NULL" and/or "NOT NULL" to the generated SQL type
// depending on the nullability of the column. Most databases will assume that a column may contain
// null as long as it isn't declared with a NOT NULL constraint, but some databases (e.g. ms sql
// server) make that behavior configurable, requiring the DDL to explicitly declare a column with
// NULL if it may contain null values. This wrapper will handle either or both cases.
type MaybeNullableMapper struct {
	NotNullText, NullableText string
	Delegate                  TypeMapper
}

var _ TypeMapper = MaybeNullableMapper{}

func (m MaybeNullableMapper) MapType(p *Projection) (mapped MappedType, err error) {
	if mapped, err = m.Delegate.MapType(p); err != nil {
		return
	} else if _, notNull := p.AsFlatType(); notNull && m.NotNullText != "" {
		mapped.DDL += " " + m.NotNullText
	} else if m.NullableText != "" {
		mapped.DDL += " " + m.NullableText
	}

	return
}

// AlwaysNullableMapper wraps a ColumnMapper to always produce DDL for a nullable column.
type AlwaysNullableMapper struct {
	Delegate TypeMapper
}

var _ AlwaysNullableTypeMapper = AlwaysNullableMapper{}

func (m AlwaysNullableMapper) MapTypeNullable(p *Projection) (mapped MappedType, err error) {
	return m.Delegate.MapType(p)
}

// PrimaryKeyMapper wraps a ColumnMapper to specify the type of the column in
// the case that it is a primary key. This is useful for cases where specific
// databases require certain variations of a type for primary keys. For example,
// MySQL does not accept TEXT as a primary key, but a VARCHAR with specified
// size works.
type PrimaryKeyMapper struct {
	PrimaryKey TypeMapper
	Delegate   TypeMapper
}

var _ TypeMapper = PrimaryKeyMapper{}

func (m PrimaryKeyMapper) MapType(p *Projection) (mapped MappedType, err error) {
	if p.IsPrimaryKey {
		return m.PrimaryKey.MapType(p)
	} else {
		return m.Delegate.MapType(p)
	}
}

// StringTypeMapper is a special TypeMapper for string type columns, which can take the format
// and/or content type into account when deciding what sql column type to generate.
type StringTypeMapper struct {
	WithFormat      map[string]TypeMapper
	WithContentType map[string]TypeMapper
	Fallback        TypeMapper
}

var _ TypeMapper = StringTypeMapper{}

func (m StringTypeMapper) MapType(p *Projection) (MappedType, error) {
	// TODO(whb): This bit of hackery is to provide backwards compatibility for materializations
	// that use numeric-as-string fields with numeric formats which were created before support for
	// materializing these fields as numeric values was added. It provides an escape hatch via the
	// field config to ignore the string formatting and continue materializing the fields as a
	// regular string, which will allow such materializations to continue working. It is not meant
	// to be user-facing and is configured as-needed by Estuary support staff. We should remove this
	// when a more comprehensive backwards compatibility layer is added to the materialization
	// dialects.
	type fieldConfigOptions struct {
		IgnoreStringFormat bool `json:"ignoreStringFormat"`
	}
	if p.RawFieldConfig != nil {
		var options fieldConfigOptions
		if err := json.Unmarshal(p.RawFieldConfig, &options); err != nil {
			ErrorMapper{}.MapType(p)
		} else if options.IgnoreStringFormat {
			log.WithFields(log.Fields{
				"field":  p.Field,
				"format": p.Projection.Inference.String_.Format,
			}).Info("ignoring string format for field")
			p.Projection.Inference.String_.Format = ""
		}
	}

	if flat, _ := p.AsFlatType(); flat != STRING && m.Fallback == nil {
		return ErrorMapper{}.MapType(p)
	} else if flat != STRING {
		return m.Fallback.MapType(p)
	} else if delegate, ok := m.WithFormat[p.Inference.String_.Format]; ok {
		return delegate.MapType(p)
	} else if delegate, ok := m.WithContentType[p.Inference.String_.ContentType]; ok {
		return delegate.MapType(p)
	} else if m.Fallback == nil {
		return ErrorMapper{}.MapType(p)
	} else {
		return m.Fallback.MapType(p)
	}
}

// ProjectionTypeMapper selects an inner TypeMapper based on a Projection's FlatType.
type ProjectionTypeMapper map[FlatType]TypeMapper

var _ TypeMapper = ProjectionTypeMapper{}

func (m ProjectionTypeMapper) MapType(p *Projection) (MappedType, error) {
	var flat, _ = p.AsFlatType()

	if delegate, ok := m[flat]; ok {
		return delegate.MapType(p)
	} else {
		return ErrorMapper{}.MapType(p)
	}
}

// MaxLengthMapper checks if the projection is a STRING type Projection having a MaxLength.
// If it is, it invokes WithLength with the MaxLength to map the Projection and returns
// the result. Otherwise, it invokes and returns Fallback.
type MaxLengthMapper struct {
	WithLength           TypeMapper
	WithLengthFmtPattern string
	Fallback             TypeMapper
}

var _ TypeMapper = MaxLengthMapper{}

func (m MaxLengthMapper) MapType(p *Projection) (MappedType, error) {
	var flat, _ = p.AsFlatType()

	if flat != STRING || p.Inference.String_.MaxLength == 0 {
		return m.Fallback.MapType(p)
	} else if mapped, err := m.WithLength.MapType(p); err != nil {
		return MappedType{}, err
	} else {
		mapped.DDL = fmt.Sprintf(mapped.DDL, p.Inference.String_.MaxLength)
		return mapped, nil
	}
}

// ErrorMapper returns a mapping error for the Projection.
type ErrorMapper struct{}

var _ TypeMapper = ErrorMapper{}

func (m ErrorMapper) MapType(p *Projection) (MappedType, error) {
	return MappedType{}, fmt.Errorf("unable to map field %s with type %s", p.Field, p.Inference.Types)
}

type constrainter struct {
	dialect Dialect
}

func (constrainter) NewConstraints(p *pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint {
	_, isNumeric := validate.AsFormattedNumeric(p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"

	default:
		// Any other case is one where the field has multiple types, which will be materialized as a
		// JSON (or equivalent) column with the default JSON serialization of the value.
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"
	}

	return &constraint
}

func (c constrainter) Compatible(existing *pf.Projection, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, error) {
	var numericCompatibilities = map[validate.StringWithNumericFormat][]string{
		validate.StringFormatInteger: {pf.JsonTypeInteger, pf.JsonTypeNull},
		validate.StringFormatNumber:  {pf.JsonTypeNumber, pf.JsonTypeNull},
	}

	typesMatch := func(actual, allowed []string) bool {
		for _, t := range actual {
			if !slices.Contains(allowed, t) {
				return false
			}
		}
		return true
	}

	// Special case: Strings formatted as integers or numbers are compatible with the integer or
	// number JSON type, even though a string formatted integer or number may result in a different
	// column type than a pure integer or number. This may result in breakage of the materialization
	// if, for example, a pure integer field is changed to a string formatted as an integer and the
	// collection gets a very large integer (formatted as a string) added to it. This is allowed
	// because it's far more common for a pure integer field to have a string formatted as an
	// integer show up later on in the collection's life and we don't want to always require
	// re-versioning/recreating tables when this happens.
	if format, ok := validate.AsFormattedNumeric(existing); ok {
		if typesMatch(proposed.Inference.Types, numericCompatibilities[format]) {
			return true, nil
		}
	} else if format, ok := validate.AsFormattedNumeric(proposed); ok {
		if typesMatch(existing.Inference.Types, numericCompatibilities[format]) {
			return true, nil
		}
	}

	existingMapped, err := c.dialect.MapTypeNullable(&Projection{
		Projection:     *existing,
		RawFieldConfig: rawFieldConfig,
	})
	if err != nil {
		return false, err
	}

	proposedMapped, err := c.dialect.MapTypeNullable(&Projection{
		Projection:     *proposed,
		RawFieldConfig: rawFieldConfig,
	})
	if err != nil {
		return false, err
	}

	return existingMapped.DDL == proposedMapped.DDL, nil
}

func (constrainter) DescriptionForType(p *pf.Projection) string {
	desc := "[" + strings.Join(p.Inference.Types, ",") + "]"

	if p.Inference.String_ != nil {
		if p.Inference.String_.Format != "" {
			desc += " format: " + p.Inference.String_.Format
		}
		if p.Inference.String_.ContentType != "" {
			desc += " content-type: " + p.Inference.String_.ContentType
		}
		if p.Inference.String_.ContentEncoding != "" {
			desc += " content-encoding: " + p.Inference.String_.ContentEncoding
		}
	}

	return desc
}
