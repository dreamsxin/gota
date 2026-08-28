package dataframe

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/dreamsxin/gota/v2/series"
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

func TestParquet_NullableRoundTrip(t *testing.T) {
	t0 := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	orig := New(
		series.New([]interface{}{"alpha", nil, "", "omega"}, series.String, "text"),
		series.New([]interface{}{1, nil, 0, 3}, series.Int, "integer"),
		series.New([]interface{}{1.5, nil, 0.0, 3.5}, series.Float, "floating"),
		series.New([]interface{}{true, nil, false, false}, series.Bool, "boolean"),
		series.New([]interface{}{t0, nil, t0.Add(time.Hour), t0.Add(2 * time.Hour)}, series.Time, "timestamp"),
	)

	var buf bytes.Buffer
	if err := orig.WriteParquet(&buf); err != nil {
		t.Fatalf("WriteParquet nullable values: %v", err)
	}
	got := ReadParquet(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if got.Err != nil {
		t.Fatalf("ReadParquet nullable values: %v", got.Err)
	}
	if !reflect.DeepEqual(got.Types(), orig.Types()) {
		t.Fatalf("nullable types: got %v want %v", got.Types(), orig.Types())
	}
	if !reflect.DeepEqual(got.Records(), orig.Records()) {
		t.Fatalf("nullable records: got %v want %v", got.Records(), orig.Records())
	}
}
