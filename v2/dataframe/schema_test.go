package dataframe

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

var schemaDF = New(
	series.New([]string{"a", "b"}, series.String, "key"),
	series.New([]int{1, 2}, series.Int, "num"),
	series.New([]float64{1.5, 2.5}, series.Float, "val"),
)

func TestSchema_Describe(t *testing.T) {
	s := schemaDF.Schema()

	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
	if got, want := s.Names(), []string{"key", "num", "val"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Names = %v, want %v", got, want)
	}
	if got, want := s.Types(), []series.Type{series.String, series.Int, series.Float}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Types = %v, want %v", got, want)
	}
	for _, f := range s.Fields() {
		if f.Nullable {
			t.Fatalf("fields without missing values must be non-nullable: %+v", f)
		}
	}
	// Physical DTypes mirror the column types.
	if got, want := s.DTypes(), []series.DType{series.DTUtf8, series.DTInt64, series.DTFloat64}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DTypes = %v, want %v", got, want)
	}
	// A column with missing values reports nullable.
	withNA := New(series.New([]interface{}{1, nil}, series.Int, "x"))
	if f, ok := withNA.Schema().Field("x"); !ok || !f.Nullable {
		t.Fatalf("column with missing values must be nullable: %+v", f)
	}

	f, ok := s.Field("num")
	if !ok || f.Type != series.Int {
		t.Fatalf("Field(num) = %+v, %v", f, ok)
	}
	if _, ok := s.Field("missing"); ok {
		t.Fatal("Field(missing) should not be found")
	}
}

func TestSchema_Equal(t *testing.T) {
	base := schemaDF.Schema()

	if !base.Equal(schemaDF.Schema()) {
		t.Fatal("identical schemas should be Equal")
	}
	if base.Equal(schemaDF.Select([]string{"num", "key"}).Schema()) {
		t.Fatal("different order should not be Equal")
	}
	if base.Equal(schemaDF.Drop([]string{"val"}).Schema()) {
		t.Fatal("different length should not be Equal")
	}
	renamed := schemaDF.Rename("id", "key").Schema()
	if base.Equal(renamed) {
		t.Fatal("different names should not be Equal")
	}

	// A zero schema (error frame) equals only another zero schema.
	if base.Equal(New().Schema()) {
		t.Fatal("non-zero vs zero schema should not be Equal")
	}
}

func TestSchema_FieldsAreCopies(t *testing.T) {
	s := schemaDF.Schema()
	fields := s.Fields()
	fields[0].Name = "mutated"
	if got := schemaDF.Schema().Names()[0]; got != "key" {
		t.Fatalf("mutating returned Fields changed the schema: %q", got)
	}
}

func TestFromSchema(t *testing.T) {
	s := schemaDF.Schema()

	empty := FromSchema(s)
	if empty.Err != nil {
		t.Fatalf("FromSchema: %v", empty.Err)
	}
	if empty.Nrow() != 0 || empty.Ncol() != 3 {
		nrows, ncols := empty.Dims()
		t.Fatalf("FromSchema dims = (%d, %d), want (0, 3)", nrows, ncols)
	}
	if !empty.Schema().Equal(s) {
		t.Fatalf("FromSchema schema = %v, want %v", empty.Schema().Fields(), s.Fields())
	}

	// Rows can be appended into the conforming frame.
	appended := New(
		series.New([]string{"a", "b"}, series.String, "key"),
		series.New([]int{1, 2}, series.Int, "num"),
		series.New([]float64{1.5, 2.5}, series.Float, "val"),
	)
	if !appended.Schema().Equal(s) {
		t.Fatal("same construction should keep the schema")
	}
}

func TestSchema_ErrorFrame(t *testing.T) {
	errDF := New()
	s := errDF.Schema()
	if s.Len() != 0 {
		t.Fatalf("error frame schema Len = %d, want 0", s.Len())
	}
	if got := FromSchema(s); got.Err == nil {
		t.Fatal("FromSchema with no fields should error")
	}
}
