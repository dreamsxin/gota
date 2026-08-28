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
	// DType is the logical kernel type (RFC §4): a physical DType for
	// plain columns, the Dictionary DType for dictionary-encoded columns.
	// Decimal and ordered Enum will extend this surface later.
	DType series.DType
	// Nullable reports whether the column may contain missing values.
	// Schema() derives it from the actual data: columns without any
	// missing value are non-nullable, letting kernels skip validity work
	// entirely (nil validity bitmaps).
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
		fields[i] = Field{
			Name:     col.Name,
			Type:     col.Type(),
			DType:    col.DType(),
			Nullable: col.HasNaN(),
		}
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

// DTypes returns the ordered logical kernel types.
func (s Schema) DTypes() []series.DType {
	out := make([]series.DType, len(s.fields))
	for i, f := range s.fields {
		out[i] = f.DType
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
// order with the same types and nullability. DType identity compares
// physical layout plus, for Dictionary fields, category content.
func (s Schema) Equal(other Schema) bool {
	if len(s.fields) != len(other.fields) {
		return false
	}
	for i := range s.fields {
		a, b := s.fields[i], other.fields[i]
		if a.Name != b.Name || a.Type != b.Type || a.Nullable != b.Nullable {
			return false
		}
		if !dtypeEqual(a.DType, b.DType) {
			return false
		}
	}
	return true
}

func dtypeEqual(a, b series.DType) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Physical() != b.Physical() {
		return false
	}
	if a.Physical() == series.PhysDictionary {
		ca, _ := series.DictionaryCategories(a)
		cb, _ := series.DictionaryCategories(b)
		return reflect.DeepEqual(ca, cb)
	}
	return true
}

// FromSchema returns a zero-row DataFrame conforming to the schema. It is
// intended for output buffers and streaming accumulators whose column
// layout is known up front. Dictionary fields produce empty dictionary
// columns sharing the schema's categories.
func FromSchema(s Schema) DataFrame {
	cols := make([]series.Series, len(s.fields))
	for i, f := range s.fields {
		if cats, ok := series.DictionaryCategories(f.DType); ok {
			cols[i] = series.EmptyDictionarySeries(cats, f.Name)
			continue
		}
		cols[i] = series.New(nil, f.Type, f.Name).Empty()
	}
	return New(cols...)
}
