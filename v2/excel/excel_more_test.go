package excel_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dreamsxin/gota/v2/dataframe"
	"github.com/dreamsxin/gota/v2/excel"
	"github.com/dreamsxin/gota/v2/series"
)

// Migrated from the former dataframe-package suites (final_test.go,
// coverage_test.go, v14_v15_test.go, errors_test.go) alongside the adapter.

func TestWriteXLSXMultiSheet(t *testing.T) {
	df1 := dataframe.New(series.New([]string{"a", "b"}, series.String, "name"))
	df2 := dataframe.New(series.New([]int{1, 2, 3}, series.Int, "value"))

	var buf bytes.Buffer
	err := excel.WriteXLSXMultiSheet(&buf,
		excel.SheetData{"Names", df1},
		excel.SheetData{"Values", df2},
	)
	if err != nil {
		t.Fatalf("WriteXLSXMultiSheet: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("WriteXLSXMultiSheet: empty output")
	}

	// Read back the first sheet.
	got1 := excel.ReadXLSX(bytes.NewReader(buf.Bytes()), excel.WithSheet("Names"))
	if got1.Err != nil {
		t.Fatalf("ReadXLSX Names: %v", got1.Err)
	}
	if got1.Nrow() != 2 {
		t.Errorf("Names sheet rows: got %d want 2", got1.Nrow())
	}
	if got := got1.Records(); !recordsEqual(got, df1.Records()) {
		t.Errorf("Names sheet records: got %v want %v", got, df1.Records())
	}

	// Read back the second sheet.
	got2 := excel.ReadXLSX(bytes.NewReader(buf.Bytes()), excel.WithSheet("Values"))
	if got2.Err != nil {
		t.Fatalf("ReadXLSX Values: %v", got2.Err)
	}
	if got2.Nrow() != 3 {
		t.Errorf("Values sheet rows: got %d want 3", got2.Nrow())
	}
	if got := got2.Records(); !recordsEqual(got, df2.Records()) {
		t.Errorf("Values sheet records: got %v want %v", got, df2.Records())
	}
}

func TestWriteXLSXMultiSheet_Empty(t *testing.T) {
	var buf bytes.Buffer
	err := excel.WriteXLSXMultiSheet(&buf)
	if err == nil {
		t.Error("WriteXLSXMultiSheet empty: expected error")
	}
}

func TestReadXLSXSheets(t *testing.T) {
	df1 := dataframe.New(series.New([]string{"a", "b"}, series.String, "name"))
	df2 := dataframe.New(series.New([]int{1, 2, 3}, series.Int, "value"))

	var buf bytes.Buffer
	err := excel.WriteXLSXMultiSheet(&buf,
		excel.SheetData{"Names", df1},
		excel.SheetData{"Values", df2},
	)
	if err != nil {
		t.Fatalf("WriteXLSXMultiSheet: %v", err)
	}

	sheets, err := excel.ReadXLSXSheets(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadXLSXSheets: %v", err)
	}
	if len(sheets) != 2 {
		t.Fatalf("ReadXLSXSheets count: got %d want 2", len(sheets))
	}
	if got := sheets["Names"].Records(); !recordsEqual(got, df1.Records()) {
		t.Errorf("Names sheet records: got %v want %v", got, df1.Records())
	}
	if got := sheets["Values"].Records(); !recordsEqual(got, df2.Records()) {
		t.Errorf("Values sheet records: got %v want %v", got, df2.Records())
	}
}

func TestWriteXLSXSheet_ReadBack(t *testing.T) {
	// WriteXLSXSheet writes into an existing workbook; exercise the shared
	// path via WriteXLSXMultiSheet and read sheet B back.
	df1 := dataframe.New(series.New([]string{"a", "b"}, series.String, "col"))
	df2 := dataframe.New(series.New([]int{1, 2, 3}, series.Int, "num"))
	var buf bytes.Buffer
	err := excel.WriteXLSXMultiSheet(&buf, excel.SheetData{"A", df1}, excel.SheetData{"B", df2})
	if err != nil {
		t.Fatal(err)
	}
	got := excel.ReadXLSX(bytes.NewReader(buf.Bytes()), excel.WithSheet("B"))
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Nrow() != 3 {
		t.Errorf("WriteXLSXSheet B rows: got %d want 3", got.Nrow())
	}
}

func TestReadXLSX_WithSheet(t *testing.T) {
	df1 := dataframe.New(series.New([]string{"sheet1"}, series.String, "name"))
	df2 := dataframe.New(
		series.New([]string{"sheet2"}, series.String, "name"),
		series.New([]int{42}, series.Int, "value"),
	)

	var buf bytes.Buffer
	if err := excel.WriteXLSXMultiSheet(&buf, excel.SheetData{"First", df1}, excel.SheetData{"Second", df2}); err != nil {
		t.Fatal(err)
	}
	got := excel.ReadXLSX(bytes.NewReader(buf.Bytes()), excel.WithSheet("Second"))
	if got.Err != nil {
		t.Fatalf("ReadXLSX WithSheet: %v", got.Err)
	}
	if got.Nrow() != 1 {
		t.Errorf("ReadXLSX WithSheet rows: got %d want 1", got.Nrow())
	}
	if got.Ncol() != 2 {
		t.Fatalf("ReadXLSX WithSheet cols: got %d want 2", got.Ncol())
	}
	if got.Col("name").Record(0) != "sheet2" {
		t.Errorf("ReadXLSX WithSheet name: got %s want sheet2", got.Col("name").Record(0))
	}
	if got.Col("value").Record(0) != "42" {
		t.Errorf("ReadXLSX WithSheet value: got %s want 42", got.Col("value").Record(0))
	}
}

func TestReadXLSX_WithSheet_NotFound(t *testing.T) {
	df := dataframe.New(series.New([]string{"a"}, series.String, "x"))
	var buf bytes.Buffer
	if err := excel.WriteXLSX(df, &buf); err != nil {
		t.Fatal(err)
	}
	got := excel.ReadXLSX(bytes.NewReader(buf.Bytes()), excel.WithSheet("NoSuchSheet"))
	if got.Err == nil {
		t.Error("ReadXLSX WithSheet non-existent: expected error")
	}
}

func TestWriteXLSX_UnknownStyleColumn(t *testing.T) {
	df := dataframe.New(series.New([]string{"a"}, series.String, "x"))
	var buf bytes.Buffer
	err := excel.WriteXLSX(df, &buf, excel.WithColumnWidths(map[string]float64{"missing": 18}))
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, dataframe.ErrColumnNotFound) {
		t.Errorf("errors.Is: got %v, want ErrColumnNotFound", err)
	}
}

func TestReadXLSX_LoadOptionsPassthrough(t *testing.T) {
	df := dataframe.New(
		series.New([]string{"k", "a", "b"}, series.String, "name"),
		series.New([]int{9, 1, 2}, series.Int, "value"),
	)
	var buf bytes.Buffer
	if err := excel.WriteXLSX(df, &buf); err != nil {
		t.Fatal(err)
	}
	// Force explicit column types through the passthrough option.
	got := excel.ReadXLSX(bytes.NewReader(buf.Bytes()),
		excel.WithLoadOptions(dataframe.WithTypes(map[string]series.Type{
			"name":  series.String,
			"value": series.Int,
		})),
	)
	if got.Err != nil {
		t.Fatalf("ReadXLSX with load options: %v", got.Err)
	}
	if got.Col("value").Type() != series.Int {
		t.Errorf("value column type: got %v want int", got.Col("value").Type())
	}
}
