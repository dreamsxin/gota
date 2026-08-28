package dataframe

import (
	"reflect"

	"github.com/dreamsxin/gota/v2/series"
)

// Field describes one column of a Schema.
type Field struct {
	// Name is the column name.
	Name string
	// Type is the physical storage type of the column.
	Type series.Type
	// Nullable reports whether the column may contain missing values.
	// Every v1.x Series supports NaN, so this is always true; it exists so
	// the columnar kernel can introduce non-nullable buffers without
	// another API change.
	Nullable bool
}

// Schema is the ordered column layout of a DataFrame. It is the public
// surface for join and concat compatibility checks and for building
// conforming empty frames; logical types (Decimal, ordered Enum) will extend
// Field when the columnar kernel lands.
type Schema struct {
	fields []Field
}

// Schema returns the column layout of the DataFrame. A DataFrame carrying an
// error has a zero-length schema.
func (df DataFrame) Schema() Schema {
	if df.Err != nil {
		return Schema{}
	}
	fields := make([]Field, df.ncols)
	for i, col := range df.columns {
		fields[i] = Field{Name: col.Name, Type: col.Type(), Nullable: true}
	}
	return Schema{fields: fields}
}

// Len returns the number of fields.
func (s Schema) Len() int { return len(s.fields) }

// Fields returns a copy of the ordered fields.
func (s Schema) Fields() []Field {
	out := make([]Field, len(s.fields))
	copy(out, s.fields)
	return out
}

// Names returns the ordered column names.
func (s Schema) Names() []string {
	out := make([]string, len(s.fields))
	for i, f := range s.fields {
		out[i] = f.Name
	}
	return out
}

// Types returns the ordered physical column types.
func (s Schema) Types() []series.Type {
	out := make([]series.Type, len(s.fields))
	for i, f := range s.fields {
		out[i] = f.Type
	}
	return out
}

// Field returns the field with the given column name.
func (s Schema) Field(name string) (Field, bool) {
	for _, f := range s.fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// Equal reports whether both schemas describe the same columns in the same
// order with the same types and nullability.
func (s Schema) Equal(other Schema) bool {
	return reflect.DeepEqual(s.fields, other.fields)
}

// FromSchema returns a zero-row DataFrame conforming to the schema. It is
// intended for output buffers and streaming accumulators whose column
// layout is known up front.
func FromSchema(s Schema) DataFrame {
	cols := make([]series.Series, len(s.fields))
	for i, f := range s.fields {
		cols[i] = series.New(nil, f.Type, f.Name).Empty()
	}
	return New(cols...)
}
