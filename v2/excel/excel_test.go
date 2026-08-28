package excel_test

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/v2/dataframe"
	"github.com/dreamsxin/gota/v2/excel"
	"github.com/dreamsxin/gota/v2/series"
	"github.com/xuri/excelize/v2"
)

func recordsEqual(a, b [][]string) bool {
	return reflect.DeepEqual(a, b)
}

// TestExcel_RoundTrip writes a DataFrame to an in-memory XLSX buffer and reads
// it back, verifying that the values survive the round-trip.
func TestExcel_RoundTrip(t *testing.T) {
	orig := dataframe.New(
		series.New([]string{"Alice", "Bob", "Carol"}, series.String, "Name"),
		series.New([]int{30, 25, 35}, series.Int, "Age"),
		series.New([]float64{1000.5, 2000.0, 1500.75}, series.Float, "Salary"),
	)

	// Write to buffer.
	var buf bytes.Buffer
	if err := excel.WriteXLSX(orig, &buf); err != nil {
		t.Fatalf("WriteXLSX: %v", err)
	}

	// Read back.
	reader := bytes.NewReader(buf.Bytes())
	got := excel.ReadXLSX(reader)
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

	// Full round-trip values.
	if !reflect.DeepEqual(got.Records(), orig.Records()) {
		t.Errorf("records mismatch: got %v want %v", got.Records(), orig.Records())
	}
}

// TestExcel_EmptyDataFrame tests that writing an empty-ish DataFrame does not panic.
func TestExcel_EmptyDataFrame(t *testing.T) {
	df := dataframe.New(
		series.New([]string{}, series.String, "X"),
	)
	var buf bytes.Buffer
	if err := excel.WriteXLSX(df, &buf); err != nil {
		t.Fatalf("WriteXLSX empty: %v", err)
	}
	// Buffer should contain a valid XLSX (non-zero bytes).
	if buf.Len() == 0 {
		t.Error("WriteXLSX: empty DataFrame produced zero-byte output")
	}
}

// TestExcel_WriteReadHeaders checks that ReadXLSX reads the header row.
func TestExcel_WriteReadHeaders(t *testing.T) {
	df := dataframe.New(
		series.New([]string{"x", "y"}, series.String, "col1"),
		series.New([]int{1, 2}, series.Int, "col2"),
	)
	var buf bytes.Buffer
	if err := excel.WriteXLSX(df, &buf); err != nil {
		t.Fatalf("WriteXLSX: %v", err)
	}
	got := excel.ReadXLSX(bytes.NewReader(buf.Bytes()))
	if got.Err != nil {
		t.Fatalf("ReadXLSX: %v", got.Err)
	}
	if got.ColIndex("col1") < 0 || got.ColIndex("col2") < 0 {
		t.Errorf("Header not preserved; names: %v", got.Names())
	}
}

func TestWriteXLSX_WithSheetName(t *testing.T) {
	df := dataframe.New(series.New([]string{"a", "b"}, series.String, "name"))

	var buf bytes.Buffer
	if err := excel.WriteXLSX(df, &buf, excel.WithSheetName("Data")); err != nil {
		t.Fatalf("WriteXLSX WithSheetName: %v", err)
	}

	got := excel.ReadXLSX(bytes.NewReader(buf.Bytes()), excel.WithSheet("Data"))
	if got.Err != nil {
		t.Fatalf("ReadXLSX named sheet: %v", got.Err)
	}
	if !reflect.DeepEqual(got.Records(), df.Records()) {
		t.Errorf("named sheet records: got %v want %v", got.Records(), df.Records())
	}
}

func TestWriteXLSX_StyleOptions(t *testing.T) {
	df := dataframe.New(
		series.New([]string{"Alice", "Bob"}, series.String, "name"),
		series.New([]float64{1234.5, 67.89}, series.Float, "amount"),
	)

	var buf bytes.Buffer
	err := excel.WriteXLSX(df, &buf,
		excel.WithBoldHeader(true),
		excel.WithColumnWidths(map[string]float64{"name": 18, "amount": 14}),
		excel.WithNumberFormats(map[string]string{"amount": "#,##0.00"}),
	)
	if err != nil {
		t.Fatalf("WriteXLSX styles: %v", err)
	}

	xl, err := excelize.OpenReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("open xlsx: %v", err)
	}
	defer xl.Close()

	headerStyleID, err := xl.GetCellStyle("Sheet1", "A1")
	if err != nil {
		t.Fatalf("header style: %v", err)
	}
	headerStyle, err := xl.GetStyle(headerStyleID)
	if err != nil {
		t.Fatalf("get header style: %v", err)
	}
	if headerStyle.Font == nil || !headerStyle.Font.Bold {
		t.Fatalf("header style bold: got %#v", headerStyle.Font)
	}

	width, err := xl.GetColWidth("Sheet1", "A")
	if err != nil {
		t.Fatalf("column width: %v", err)
	}
	if math.Abs(width-18) > 0.01 {
		t.Fatalf("column width: got %v want 18", width)
	}

	amountStyleID, err := xl.GetCellStyle("Sheet1", "B2")
	if err != nil {
		t.Fatalf("amount style: %v", err)
	}
	amountStyle, err := xl.GetStyle(amountStyleID)
	if err != nil {
		t.Fatalf("get amount style: %v", err)
	}
	if amountStyle.CustomNumFmt == nil || *amountStyle.CustomNumFmt != "#,##0.00" {
		t.Fatalf("number format: got %#v", amountStyle.CustomNumFmt)
	}
}
