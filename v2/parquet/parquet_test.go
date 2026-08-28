package parquet_test

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/dreamsxin/gota/v2/dataframe"
	"github.com/dreamsxin/gota/v2/parquet"
	"github.com/dreamsxin/gota/v2/series"
)

func TestParquet_RoundTrip(t *testing.T) {
	orig := dataframe.New(
		series.New([]string{"Alice", "Bob", "Carol"}, series.String, "name"),
		series.New([]int{30, 25, 35}, series.Int, "age"),
		series.New([]float64{1000.5, 2000, 1500.75}, series.Float, "salary"),
		series.New([]bool{true, false, true}, series.Bool, "active"),
	)

	var buf bytes.Buffer
	if err := parquet.WriteParquet(orig, &buf); err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("WriteParquet produced empty output")
	}

	got := parquet.ReadParquet(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
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
	orig := dataframe.New(
		series.New([]string{"A", "B"}, series.String, "symbol"),
		series.New([]int{10, 20}, series.Int, "qty"),
	)

	path := t.TempDir() + "/data.parquet"
	if err := parquet.WriteParquetFile(orig, path); err != nil {
		t.Fatalf("WriteParquetFile: %v", err)
	}

	got := parquet.ReadParquetFile(path)
	if got.Err != nil {
		t.Fatalf("ReadParquetFile: %v", got.Err)
	}
	if !reflect.DeepEqual(got.Records(), orig.Records()) {
		t.Fatalf("records: got %v want %v", got.Records(), orig.Records())
	}
}

func TestParquet_NullableRoundTrip(t *testing.T) {
	t0 := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	orig := dataframe.New(
		series.New([]interface{}{"alpha", nil, "", "omega"}, series.String, "text"),
		series.New([]interface{}{1, nil, 0, 3}, series.Int, "integer"),
		series.New([]interface{}{1.5, nil, 0.0, 3.5}, series.Float, "floating"),
		series.New([]interface{}{true, nil, false, false}, series.Bool, "boolean"),
		series.New([]interface{}{t0, nil, t0.Add(time.Hour), t0.Add(2 * time.Hour)}, series.Time, "timestamp"),
	)

	var buf bytes.Buffer
	if err := parquet.WriteParquet(orig, &buf); err != nil {
		t.Fatalf("WriteParquet nullable values: %v", err)
	}
	got := parquet.ReadParquet(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
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

// FuzzParquetRoundTrip writes fuzzed column values to Parquet and reads them
// back. Names, types, and records must survive the round trip unchanged.
func FuzzParquetRoundTrip(f *testing.F) {
	f.Add("alpha", int64(1), 0.5, true, uint8(3))
	f.Add("", int64(-7), -1.25, false, uint8(0))
	f.Add("NaN", int64(0), 0.0, true, uint8(1))
	f.Add("日本語", int64(1<<40), 1e300, false, uint8(5))

	f.Fuzz(func(t *testing.T, s string, i int64, f1 float64, b bool, n uint8) {
		nrows := int(n % 20)

		names := make([]string, nrows)
		ids := make([]int, nrows)
		vals := make([]float64, nrows)
		flags := make([]bool, nrows)
		for j := 0; j < nrows; j++ {
			names[j] = s + strconv.Itoa(j)
			ids[j] = int(i) + j
			vals[j] = f1 * float64(j+1)
			if j%5 == 4 {
				vals[j] = math.NaN()
			}
			flags[j] = b != (j%2 == 0)
		}

		orig := dataframe.New(
			series.New(names, series.String, "name"),
			series.New(ids, series.Int, "id"),
			series.New(vals, series.Float, "val"),
			series.New(flags, series.Bool, "flag"),
		)

		var buf bytes.Buffer
		if err := parquet.WriteParquet(orig, &buf); err != nil {
			t.Fatalf("WriteParquet: %v", err)
		}
		got := parquet.ReadParquet(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if got.Err != nil {
			t.Fatalf("ReadParquet: %v", got.Err)
		}

		if !reflect.DeepEqual(got.Names(), orig.Names()) {
			t.Fatalf("names: got %v want %v", got.Names(), orig.Names())
		}
		if !reflect.DeepEqual(got.Types(), orig.Types()) {
			t.Fatalf("types: got %v want %v", got.Types(), orig.Types())
		}
		if fmt.Sprint(got.Records()) != fmt.Sprint(orig.Records()) {
			t.Fatalf("records: got %v want %v", got.Records(), orig.Records())
		}
	})
}
