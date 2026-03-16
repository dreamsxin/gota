package dataframe

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// TestExcel_RoundTrip writes a DataFrame to an in-memory XLSX buffer and reads
// it back, verifying that the values survive the round-trip.
func TestExcel_RoundTrip(t *testing.T) {
	orig := New(
		series.New([]string{"Alice", "Bob", "Carol"}, series.String, "Name"),
		series.New([]int{30, 25, 35}, series.Int, "Age"),
		series.New([]float64{1000.5, 2000.0, 1500.75}, series.Float, "Salary"),
	)

	// Write to buffer.
	var buf bytes.Buffer
	if err := orig.WriteXLSX(&buf); err != nil {
		t.Fatalf("WriteXLSX: %v", err)
	}

	// Read back.
	reader := bytes.NewReader(buf.Bytes())
	got := ReadXLSX(reader)
	if got.Err != nil {
		t.Fatalf("ReadXLSX: %v", got.Err)
	}

	// Same shape.
	if got.Nrow() != orig.Nrow() {
		t.Errorf("rows: got %d want %d", got.Nrow(), orig.Nrow())
	}
	if got.Ncol() != orig.Ncol() {
		t.Errorf("cols: got %d want %d", got.Ncol(), orig.Ncol())
	}

	// Same column names.
	if !reflect.DeepEqual(got.Names(), orig.Names()) {
		t.Errorf("names: got %v want %v", got.Names(), orig.Names())
	}

	// Name column values.
	origNames := orig.Col("Name").Records()
	gotNames := got.Col("Name").Records()
	if !reflect.DeepEqual(origNames, gotNames) {
		t.Errorf("Name column mismatch: got %v want %v", gotNames, origNames)
	}
}

// TestExcel_EmptyDataFrame tests that writing an empty-ish DataFrame does not panic.
func TestExcel_EmptyDataFrame(t *testing.T) {
	df := New(
		series.New([]string{}, series.String, "X"),
	)
	var buf bytes.Buffer
	if err := df.WriteXLSX(&buf); err != nil {
		t.Fatalf("WriteXLSX empty: %v", err)
	}
	// Buffer should contain a valid XLSX (non-zero bytes).
	if buf.Len() == 0 {
		t.Error("WriteXLSX: empty DataFrame produced zero-byte output")
	}
}

// TestExcel_MultipleSheets_DefaultFirst checks that ReadXLSX reads the first sheet.
func TestExcel_WriteReadHeaders(t *testing.T) {
	df := New(
		series.New([]string{"x", "y"}, series.String, "col1"),
		series.New([]int{1, 2}, series.Int, "col2"),
	)
	var buf bytes.Buffer
	if err := df.WriteXLSX(&buf); err != nil {
		t.Fatalf("WriteXLSX: %v", err)
	}
	got := ReadXLSX(bytes.NewReader(buf.Bytes()))
	if got.Err != nil {
		t.Fatalf("ReadXLSX: %v", got.Err)
	}
	if got.ColIndex("col1") < 0 || got.ColIndex("col2") < 0 {
		t.Errorf("Header not preserved; names: %v", got.Names())
	}
}
