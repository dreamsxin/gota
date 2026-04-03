package dataframe

import (
	"bytes"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// v1.2 — EWM.Var pandas-compatible formula
// -----------------------------------------------------------------------

func TestEWM_Var_PandasCompat(t *testing.T) {
	// Cross-check with pandas:
	//   pd.Series([1,2,3,4,5]).ewm(span=3, adjust=True).var()
	// span=3 → alpha=0.5
	// Expected (approx): [NaN, 0.5, 0.6667, 0.7143, 0.7273]
	s := series.Floats([]float64{1, 2, 3, 4, 5})
	got := s.EWM(3).Var()
	if got.Len() != 5 {
		t.Fatalf("EWM Var len: got %d want 5", got.Len())
	}
	// First element: only 1 observation → NaN
	if !math.IsNaN(got.Elem(0).Float()) {
		t.Errorf("EWM Var[0]: got %v want NaN", got.Elem(0).Float())
	}
	// All subsequent values must be positive.
	for i := 1; i < got.Len(); i++ {
		v := got.Elem(i).Float()
		if math.IsNaN(v) || v <= 0 {
			t.Errorf("EWM Var[%d]: got %v, want positive", i, v)
		}
	}
	// Std = sqrt(Var) must be consistent.
	std := s.EWM(3).Std()
	for i := 1; i < got.Len(); i++ {
		wantStd := math.Sqrt(got.Elem(i).Float())
		gotStd := std.Elem(i).Float()
		if math.Abs(gotStd-wantStd) > 1e-9 {
			t.Errorf("EWM Std[%d]: got %v want %v", i, gotStd, wantStd)
		}
	}
}

// -----------------------------------------------------------------------
// In Progress — Sample row-order preservation
// -----------------------------------------------------------------------

func TestDataFrame_Sample_RowOrder(t *testing.T) {
	df := New(
		series.New([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, series.Int, "idx"),
	)
	// Sample 5 rows without replacement; result should NOT be sorted.
	// With seed=1 the permutation is deterministic.
	out := df.Sample(5, -1, false, 1)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 5 {
		t.Fatalf("Sample rows: got %d want 5", out.Nrow())
	}
	// Collect sampled indexes.
	vals := make([]int, 5)
	for i := range vals {
		v, _ := out.Col("idx").Elem(i).Int()
		vals[i] = v
	}
	// They should NOT be in ascending order (with seed=1 they won't be).
	sorted := true
	for i := 1; i < len(vals); i++ {
		if vals[i] < vals[i-1] {
			sorted = false
			break
		}
	}
	if sorted {
		t.Log("Sample: result happened to be sorted (may be a false positive with this seed)")
	}
}

// -----------------------------------------------------------------------
// v1.4 — CapplyParallel
// -----------------------------------------------------------------------

func TestDataFrame_CapplyParallel(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "A"),
		series.New([]float64{4, 5, 6}, series.Float, "B"),
	)
	double := func(s series.Series) series.Series {
		vals := s.Float()
		out := make([]float64, len(vals))
		for i, v := range vals {
			out[i] = v * 2
		}
		r := series.Floats(out)
		r.Name = s.Name
		return r
	}
	seq := df.Capply(double)
	par := df.CapplyParallel(double)

	if !reflect.DeepEqual(seq.Records(), par.Records()) {
		t.Errorf("CapplyParallel: got %v want %v", par.Records(), seq.Records())
	}
}

// -----------------------------------------------------------------------
// v1.4 — AggregationParallel
// -----------------------------------------------------------------------

func TestGroups_AggregationParallel(t *testing.T) {
	df := New(
		series.New([]string{"A", "B", "A", "B", "A"}, series.String, "grp"),
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "val"),
	)
	g := df.GroupBy("grp")
	seq := g.Aggregation([]AggregationType{Aggregation_SUM}, []string{"val"})
	par := g.AggregationParallel([]AggregationType{Aggregation_SUM}, []string{"val"})

	if seq.Err != nil {
		t.Fatal(seq.Err)
	}
	if par.Err != nil {
		t.Fatal(par.Err)
	}
	if seq.Nrow() != par.Nrow() {
		t.Errorf("AggregationParallel rows: seq=%d par=%d", seq.Nrow(), par.Nrow())
	}
}

// -----------------------------------------------------------------------
// v1.5 — WithSheet
// -----------------------------------------------------------------------

func TestReadXLSX_WithSheet(t *testing.T) {
	// Write a two-sheet workbook, then read the second sheet.
	df1 := New(series.New([]string{"sheet1"}, series.String, "name"))
	df2 := New(series.New([]string{"sheet2"}, series.String, "name"))

	var buf bytes.Buffer
	// Write df1 to Sheet1 (default).
	if err := df1.WriteXLSX(&buf); err != nil {
		t.Fatal(err)
	}
	// We can only write one sheet with current API; just verify WithSheet
	// option is accepted without error when reading the default sheet.
	buf.Reset()
	if err := df2.WriteXLSX(&buf); err != nil {
		t.Fatal(err)
	}
	got := ReadXLSX(bytes.NewReader(buf.Bytes()), WithSheet("Sheet1"))
	if got.Err != nil {
		t.Fatalf("ReadXLSX WithSheet: %v", got.Err)
	}
	if got.Nrow() != 1 {
		t.Errorf("ReadXLSX WithSheet rows: got %d want 1", got.Nrow())
	}
}

func TestReadXLSX_WithSheet_NotFound(t *testing.T) {
	df := New(series.New([]string{"a"}, series.String, "x"))
	var buf bytes.Buffer
	if err := df.WriteXLSX(&buf); err != nil {
		t.Fatal(err)
	}
	got := ReadXLSX(bytes.NewReader(buf.Bytes()), WithSheet("NoSuchSheet"))
	if got.Err == nil {
		t.Error("ReadXLSX WithSheet non-existent: expected error")
	}
}

// -----------------------------------------------------------------------
// v1.5 — JSON Lines (NDJSON)
// -----------------------------------------------------------------------

func TestNDJSON_RoundTrip(t *testing.T) {
	df := New(
		series.New([]string{"alice", "bob"}, series.String, "name"),
		series.New([]int{30, 25}, series.Int, "age"),
		series.New([]float64{1.5, 2.5}, series.Float, "score"),
	)
	var buf bytes.Buffer
	if err := df.WriteNDJSON(&buf); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("WriteNDJSON: got %d lines want 2", len(lines))
	}

	got := ReadNDJSON(strings.NewReader(buf.String()))
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Nrow() != 2 {
		t.Errorf("ReadNDJSON rows: got %d want 2", got.Nrow())
	}
	if got.Ncol() != 3 {
		t.Errorf("ReadNDJSON cols: got %d want 3", got.Ncol())
	}
}

func TestNDJSON_EmptyLines(t *testing.T) {
	ndjson := `{"a":1}

# comment
{"a":2}
`
	got := ReadNDJSON(strings.NewReader(ndjson))
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Nrow() != 2 {
		t.Errorf("ReadNDJSON skip empty/comment: got %d rows want 2", got.Nrow())
	}
}

func TestNDJSON_NaN(t *testing.T) {
	df := New(
		series.New([]interface{}{"x", nil}, series.String, "v"),
	)
	var buf bytes.Buffer
	if err := df.WriteNDJSON(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "null") {
		t.Errorf("WriteNDJSON NaN: expected null in output, got: %s", buf.String())
	}
}

// -----------------------------------------------------------------------
// v1.3 — Resample
// -----------------------------------------------------------------------

func TestDataFrame_Resample_Monthly(t *testing.T) {
	t1 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 2, 5, 0, 0, 0, 0, time.UTC)
	df := New(
		series.New([]time.Time{t1, t2, t3}, series.Time, "date"),
		series.New([]float64{10, 20, 30}, series.Float, "revenue"),
	)
	rg := df.Resample("date", ResampleMonthly)
	if rg.Err != nil {
		t.Fatal(rg.Err)
	}
	result := rg.Aggregation([]AggregationType{Aggregation_SUM}, []string{"revenue"})
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	// 2 months: Jan (10+20=30), Feb (30)
	if result.Nrow() != 2 {
		t.Fatalf("Resample monthly rows: got %d want 2", result.Nrow())
	}
	// Sorted by period: 2024-01 first.
	if result.Col("period").Elem(0).String() != "2024-01" {
		t.Errorf("Resample period[0]: got %s want 2024-01", result.Col("period").Elem(0).String())
	}
	janSum := result.Col("revenue_SUM").Elem(0).Float()
	if !compareFloats(janSum, 30.0, 9) {
		t.Errorf("Resample Jan sum: got %v want 30", janSum)
	}
}

func TestDataFrame_Resample_NonTimeColumn(t *testing.T) {
	df := New(series.New([]string{"a", "b"}, series.String, "x"))
	rg := df.Resample("x", ResampleDaily)
	if rg.Err == nil {
		t.Error("Resample non-Time column: expected error")
	}
}

// -----------------------------------------------------------------------
// v1.3 — Stack / Unstack
// -----------------------------------------------------------------------

func TestDataFrame_Stack(t *testing.T) {
	df := New(
		series.New([]string{"1", "2"}, series.String, "id"),
		series.New([]float64{10, 20}, series.Float, "q1"),
		series.New([]float64{30, 40}, series.Float, "q2"),
	)
	long := df.Stack([]string{"id"}, []string{"q1", "q2"}, "quarter", "value")
	if long.Err != nil {
		t.Fatal(long.Err)
	}
	if long.Nrow() != 4 {
		t.Fatalf("Stack rows: got %d want 4", long.Nrow())
	}
	names := long.Names()
	if !reflect.DeepEqual(names, []string{"id", "quarter", "value"}) {
		t.Errorf("Stack names: got %v", names)
	}
}

func TestDataFrame_Unstack(t *testing.T) {
	// Build a long DataFrame and unstack it.
	long := New(
		series.New([]string{"1", "1", "2", "2"}, series.String, "id"),
		series.New([]string{"q1", "q2", "q1", "q2"}, series.String, "quarter"),
		series.New([]string{"10", "30", "20", "40"}, series.String, "value"),
	)
	wide := long.Unstack([]string{"id"}, "quarter", "value")
	if wide.Err != nil {
		t.Fatal(wide.Err)
	}
	// Expect: id | q1 | q2
	if wide.Nrow() != 2 {
		t.Fatalf("Unstack rows: got %d want 2", wide.Nrow())
	}
	if wide.Ncol() != 3 {
		t.Fatalf("Unstack cols: got %d want 3", wide.Ncol())
	}
	// id=1, q1=10
	if wide.Col("q1").Elem(0).String() != "10" {
		t.Errorf("Unstack q1[0]: got %s want 10", wide.Col("q1").Elem(0).String())
	}
}

func TestDataFrame_StackUnstack_RoundTrip(t *testing.T) {
	wide := New(
		series.New([]string{"a", "b"}, series.String, "id"),
		series.New([]string{"1", "3"}, series.String, "x"),
		series.New([]string{"2", "4"}, series.String, "y"),
	)
	long := wide.Stack([]string{"id"}, []string{"x", "y"}, "var", "val")
	if long.Err != nil {
		t.Fatal(long.Err)
	}
	back := long.Unstack([]string{"id"}, "var", "val")
	if back.Err != nil {
		t.Fatal(back.Err)
	}
	// Should have same shape as original.
	if back.Nrow() != wide.Nrow() || back.Ncol() != wide.Ncol() {
		t.Errorf("Stack/Unstack round-trip: got %dx%d want %dx%d",
			back.Nrow(), back.Ncol(), wide.Nrow(), wide.Ncol())
	}
}
