package dataframe

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

func TestParquet_RoundTrip(t *testing.T) {
	orig := New(
		series.New([]string{"Alice", "Bob", "Carol"}, series.String, "name"),
		series.New([]int{30, 25, 35}, series.Int, "age"),
		series.New([]float64{1000.5, 2000, 1500.75}, series.Float, "salary"),
		series.New([]bool{true, false, true}, series.Bool, "active"),
	)

	var buf bytes.Buffer
	if err := orig.WriteParquet(&buf); err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("WriteParquet produced empty output")
	}

	got := ReadParquet(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if got.Err != nil {
		t.Fatalf("ReadParquet: %v", got.Err)
	}
	if !reflect.DeepEqual(got.Names(), orig.Names()) {
		t.Fatalf("names: got %v want %v", got.Names(), orig.Names())
	}
	if !reflect.DeepEqual(got.Types(), orig.Types()) {
		t.Fatalf("types: got %v want %v", got.Types(), orig.Types())
	}
	if !reflect.DeepEqual(got.Records(), orig.Records()) {
		t.Fatalf("records: got %v want %v", got.Records(), orig.Records())
	}
}

func TestParquet_FileRoundTrip(t *testing.T) {
	orig := New(
		series.New([]string{"A", "B"}, series.String, "symbol"),
		series.New([]int{10, 20}, series.Int, "qty"),
	)

	path := t.TempDir() + "/data.parquet"
	if err := orig.WriteParquetFile(path); err != nil {
		t.Fatalf("WriteParquetFile: %v", err)
	}

	got := ReadParquetFile(path)
	if got.Err != nil {
		t.Fatalf("ReadParquetFile: %v", got.Err)
	}
	if !reflect.DeepEqual(got.Records(), orig.Records()) {
		t.Fatalf("records: got %v want %v", got.Records(), orig.Records())
	}
}
