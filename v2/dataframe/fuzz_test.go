package dataframe

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// FuzzQuery feeds arbitrary expressions to the Query parser. The parser must
// never panic, and a successful result must be a row subset of the input.
func FuzzQuery(f *testing.F) {
	seeds := []string{
		"age > 18",
		"status == active",
		"age >= 18 AND age <= 65",
		"country in US,UK,CA",
		"score > 0.5 OR label == good",
		"active == true AND (score > 0.5 OR label == good)",
		`label in "A AND B","x,y"`,
		"income > 100",       // column name contains operator substring "in"
		"bandwidth <= 10",    // column name contains operator substring "and"
		"age >",              // truncated condition
		"(age > 18",          // unbalanced parenthesis
		"age > 18 AND",       // dangling operator
		"unknown_col == 1",   // missing column
		"age in 1,2,3",       // numeric in-list
		"age not in 1,2,3",   // numeric not-in list
		"label == 'it''s'",   // quoted quote
		"age>>18",            // glued operators
		"AND OR ( ) == in ,", // only keywords
		"年龄 > 18",            // non-ASCII
		"\"\" == \"\"",
		"  ",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, expr string) {
		df := New(
			series.New([]int{17, 21, 35, 60}, series.Int, "age"),
			series.New([]float64{0.1, 0.6, 0.9, 0.3}, series.Float, "score"),
			series.New([]string{"good", "bad", "good", "bad"}, series.String, "label"),
			series.New([]string{"US", "UK", "CA", "US"}, series.String, "country"),
			series.New([]string{"active", "active", "inactive", "active"}, series.String, "status"),
			series.New([]bool{true, false, true, true}, series.Bool, "active"),
			series.New([]int{100, 200, 300, 400}, series.Int, "income"),
			series.New([]int{1, 2, 3, 4}, series.Int, "bandwidth"),
		)

		got := df.Query(expr)
		if got.Err != nil {
			return
		}
		nrows, ncols := got.Dims()
		if ncols != 8 {
			t.Fatalf("Query(%q): ncols = %d, want 8", expr, ncols)
		}
		if nrows < 0 || nrows > 4 {
			t.Fatalf("Query(%q): nrows = %d, want within [0, 4]", expr, nrows)
		}
	})
}

// FuzzReadCSVDetectDelimiter builds small CSV documents with a fuzzed
// delimiter and field values, then reads them with delimiter detection.
// The reader must never panic; for well-formed input with a supported
// delimiter the result must have the expected shape.
func FuzzReadCSVDetectDelimiter(f *testing.F) {
	f.Add(uint8(0), "alpha", "1", "true", "beta")
	f.Add(uint8(1), "x", "2", "false", "y")
	f.Add(uint8(2), "", "3", "true", "z")
	f.Add(uint8(3), "NaN", "4", "0", "w")
	f.Add(uint8(4), "a b", "5", "1", "c d")

	candidates := []byte{',', '\t', ';', '|', '~'}

	f.Fuzz(func(t *testing.T, di uint8, a, b, c, d string) {
		delim := candidates[int(di)%len(candidates)]

		fields := []string{a, b, c, d}
		for _, v := range fields {
			// Keep the input inside the unquoted-CSV subset so the
			// shape assertion below stays deterministic.
			if strings.ContainsAny(v, ",\t;|\"\r\n") {
				t.Skip()
			}
		}

		var sb strings.Builder
		sep := string(delim)
		sb.WriteString("A" + sep + "B" + sep + "C" + "\n")
		sb.WriteString(a + sep + b + sep + c + "\n")
		sb.WriteString(d + sep + a + sep + b + "\n")
		sb.WriteString(c + sep + d + sep + a + "\n")

		df := ReadCSV(strings.NewReader(sb.String()), DetectDelimiter(true))

		if delim == '~' {
			// Unsupported delimiter: inference falls back; only the
			// no-panic guarantee applies.
			return
		}
		if df.Err != nil {
			t.Fatalf("ReadCSV delim=%q fields=%q: %v", delim, fields, df.Err)
		}
		nrows, ncols := df.Dims()
		if nrows != 3 || ncols != 3 {
			t.Fatalf("ReadCSV delim=%q fields=%q: dims = (%d, %d), want (3, 3)",
				delim, fields, nrows, ncols)
		}
	})
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

		orig := New(
			series.New(names, series.String, "name"),
			series.New(ids, series.Int, "id"),
			series.New(vals, series.Float, "val"),
			series.New(flags, series.Bool, "flag"),
		)

		var buf bytes.Buffer
		if err := orig.WriteParquet(&buf); err != nil {
			t.Fatalf("WriteParquet: %v", err)
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
		if fmt.Sprint(got.Records()) != fmt.Sprint(orig.Records()) {
			t.Fatalf("records: got %v want %v", got.Records(), orig.Records())
		}
	})
}
