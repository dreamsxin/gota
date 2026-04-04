package dataframe

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// Head / Tail
// -----------------------------------------------------------------------

func TestDataFrame_Head(t *testing.T) {
	df := New(series.New([]int{1, 2, 3, 4, 5}, series.Int, "x"))
	if df.Head(3).Nrow() != 3 {
		t.Error("Head(3): expected 3 rows")
	}
	if df.Head(0).Nrow() != 0 {
		t.Error("Head(0): expected 0 rows")
	}
	if df.Head(10).Nrow() != 5 {
		t.Error("Head(10) > nrows: expected all 5 rows")
	}
}

func TestDataFrame_Tail(t *testing.T) {
	df := New(series.New([]int{1, 2, 3, 4, 5}, series.Int, "x"))
	tail := df.Tail(2)
	if tail.Nrow() != 2 {
		t.Fatalf("Tail(2): expected 2 rows, got %d", tail.Nrow())
	}
	if tail.Col("x").Elem(0).String() != "4" {
		t.Errorf("Tail(2)[0]: expected 4, got %s", tail.Col("x").Elem(0).String())
	}
}

// -----------------------------------------------------------------------
// Info
// -----------------------------------------------------------------------

func TestDataFrame_Info(t *testing.T) {
	df := New(
		series.New([]interface{}{"a", nil, "c"}, series.String, "name"),
		series.New([]float64{1.0, 2.0, 3.0}, series.Float, "score"),
	)
	var buf bytes.Buffer
	df.Info(&buf)
	out := buf.String()
	if !strings.Contains(out, "3 entries") {
		t.Errorf("Info: expected '3 entries', got:\n%s", out)
	}
	if !strings.Contains(out, "2 non-null") {
		t.Errorf("Info: expected '2 non-null' for name column, got:\n%s", out)
	}
	if !strings.Contains(out, "memory usage") {
		t.Errorf("Info: expected 'memory usage', got:\n%s", out)
	}
	// nil writer should not panic
	df.Info(nil)
	// Stdout should not panic
	df.Info(os.Stdout)
}

// -----------------------------------------------------------------------
// IsNull / NotNull / IsNA / NotNA
// -----------------------------------------------------------------------

func TestDataFrame_IsNull_NotNull(t *testing.T) {
	df := New(
		series.New([]interface{}{1.0, nil, 3.0}, series.Float, "v"),
	)
	nullMask := df.IsNull()
	if nullMask.Err != nil {
		t.Fatal(nullMask.Err)
	}
	b0, _ := nullMask.Col("v").Elem(0).Bool()
	b1, _ := nullMask.Col("v").Elem(1).Bool()
	if b0 || !b1 {
		t.Errorf("IsNull: expected [false true false], got [%v %v]", b0, b1)
	}

	notNull := df.NotNull()
	nb0, _ := notNull.Col("v").Elem(0).Bool()
	nb1, _ := notNull.Col("v").Elem(1).Bool()
	if !nb0 || nb1 {
		t.Errorf("NotNull: expected [true false true], got [%v %v]", nb0, nb1)
	}

	// Aliases
	if df.IsNA().Nrow() != df.IsNull().Nrow() {
		t.Error("IsNA should equal IsNull")
	}
	if df.NotNA().Nrow() != df.NotNull().Nrow() {
		t.Error("NotNA should equal NotNull")
	}
}

// -----------------------------------------------------------------------
// ValueCounts
// -----------------------------------------------------------------------

func TestDataFrame_ValueCounts(t *testing.T) {
	df := New(
		series.New([]string{"a", "b", "a", "c", "b", "a"}, series.String, "cat"),
	)
	vc := df.ValueCounts("cat", false, false)
	if vc.Err != nil {
		t.Fatal(vc.Err)
	}
	// First row should be "a" with count 3 (descending).
	if vc.Col("cat").Elem(0).String() != "a" {
		t.Errorf("ValueCounts[0]: expected 'a', got %s", vc.Col("cat").Elem(0).String())
	}
	if vc.Col("count").Elem(0).Float() != 3 {
		t.Errorf("ValueCounts count[0]: expected 3, got %v", vc.Col("count").Elem(0).Float())
	}

	// Normalize.
	vcN := df.ValueCounts("cat", true, false)
	if vcN.Col("proportion").Elem(0).Float() != 0.5 {
		t.Errorf("ValueCounts normalize[0]: expected 0.5, got %v", vcN.Col("proportion").Elem(0).Float())
	}

	// Ascending.
	vcA := df.ValueCounts("cat", false, true)
	if vcA.Col("count").Elem(0).Float() != 1 {
		t.Errorf("ValueCounts ascending[0]: expected 1, got %v", vcA.Col("count").Elem(0).Float())
	}
}

// -----------------------------------------------------------------------
// NLargest / NSmallest
// -----------------------------------------------------------------------

func TestDataFrame_NLargest_NSmallest(t *testing.T) {
	df := New(
		series.New([]float64{3, 1, 4, 1, 5, 9, 2, 6}, series.Float, "v"),
	)
	top3 := df.NLargest(3, "v")
	if top3.Nrow() != 3 {
		t.Fatalf("NLargest(3) rows: got %d want 3", top3.Nrow())
	}
	if top3.Col("v").Elem(0).Float() != 9 {
		t.Errorf("NLargest[0]: expected 9, got %v", top3.Col("v").Elem(0).Float())
	}

	bot3 := df.NSmallest(3, "v")
	if bot3.Nrow() != 3 {
		t.Fatalf("NSmallest(3) rows: got %d want 3", bot3.Nrow())
	}
	if bot3.Col("v").Elem(0).Float() != 1 {
		t.Errorf("NSmallest[0]: expected 1, got %v", bot3.Col("v").Elem(0).Float())
	}
}

// -----------------------------------------------------------------------
// Sample
// -----------------------------------------------------------------------

func TestDataFrame_Sample_Reproducible(t *testing.T) {
	df := New(series.New([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, series.Int, "x"))
	a := df.Sample(5, -1, false, 42)
	b := df.Sample(5, -1, false, 42)
	if !reflect.DeepEqual(a.Records(), b.Records()) {
		t.Error("Sample: same seed should produce same result")
	}
}

func TestDataFrame_Sample_Frac(t *testing.T) {
	df := New(series.New([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, series.Int, "x"))
	out := df.Sample(-1, 0.3, false, 1)
	if out.Nrow() != 3 {
		t.Errorf("Sample frac=0.3: expected 3 rows, got %d", out.Nrow())
	}
}

func TestDataFrame_Sample_WithReplacement(t *testing.T) {
	df := New(series.New([]int{1, 2, 3}, series.Int, "x"))
	out := df.Sample(10, -1, true, 1)
	if out.Nrow() != 10 {
		t.Errorf("Sample replace: expected 10 rows, got %d", out.Nrow())
	}
}

func TestDataFrame_Sample_Errors(t *testing.T) {
	df := New(series.New([]int{1, 2, 3}, series.Int, "x"))
	if df.Sample(-1, -1, false, 1).Err == nil {
		t.Error("Sample no n/frac: expected error")
	}
	if df.Sample(10, -1, false, 1).Err == nil {
		t.Error("Sample n > population: expected error")
	}
}

// -----------------------------------------------------------------------
// Pipe / PipeWithArgs / ApplyMap
// -----------------------------------------------------------------------

func TestDataFrame_Pipe(t *testing.T) {
	df := New(series.New([]int{1, 2, 3}, series.Int, "x"))
	out := df.Pipe(func(d DataFrame) DataFrame {
		return d.Head(2)
	})
	if out.Nrow() != 2 {
		t.Errorf("Pipe: expected 2 rows, got %d", out.Nrow())
	}
}

func TestDataFrame_PipeWithArgs(t *testing.T) {
	df := New(series.New([]int{1, 2, 3, 4, 5}, series.Int, "x"))
	out := df.PipeWithArgs(func(d DataFrame, args ...interface{}) DataFrame {
		n := args[0].(int)
		return d.Head(n)
	}, 3)
	if out.Nrow() != 3 {
		t.Errorf("PipeWithArgs: expected 3 rows, got %d", out.Nrow())
	}
}

func TestDataFrame_ApplyMap(t *testing.T) {
	df := New(series.New([]string{"hello", "world"}, series.String, "s"))
	out := df.ApplyMap(func(v interface{}) interface{} {
		if s, ok := v.(string); ok {
			return strings.ToUpper(s)
		}
		return v
	})
	if out.Col("s").Elem(0).String() != "HELLO" {
		t.Errorf("ApplyMap: expected HELLO, got %s", out.Col("s").Elem(0).String())
	}
}

// -----------------------------------------------------------------------
// Clip / ClipColumn — NaN preservation
// -----------------------------------------------------------------------

func TestDataFrame_Clip_NaN(t *testing.T) {
	df := New(
		series.New([]interface{}{1.0, nil, 10.0}, series.Float, "v"),
	)
	lo, hi := 2.0, 8.0
	out := df.Clip(&lo, &hi)
	if !out.Col("v").Elem(1).IsNA() {
		t.Error("Clip: NaN should be preserved, not clipped")
	}
	if out.Col("v").Elem(0).Float() != 2.0 {
		t.Errorf("Clip[0]: expected 2.0, got %v", out.Col("v").Elem(0).Float())
	}
	if out.Col("v").Elem(2).Float() != 8.0 {
		t.Errorf("Clip[2]: expected 8.0, got %v", out.Col("v").Elem(2).Float())
	}
}

func TestDataFrame_ClipColumn(t *testing.T) {
	df := New(
		series.New([]float64{-5, 0, 15}, series.Float, "score"),
		series.New([]string{"a", "b", "c"}, series.String, "label"),
	)
	lo, hi := 0.0, 10.0
	out := df.ClipColumn("score", &lo, &hi)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Col("score").Elem(0).Float() != 0 {
		t.Errorf("ClipColumn[0]: expected 0, got %v", out.Col("score").Elem(0).Float())
	}
	if out.Col("score").Elem(2).Float() != 10 {
		t.Errorf("ClipColumn[2]: expected 10, got %v", out.Col("score").Elem(2).Float())
	}
	// label column unchanged
	if out.Col("label").Elem(0).String() != "a" {
		t.Error("ClipColumn: non-target column should be unchanged")
	}
}

// -----------------------------------------------------------------------
// Replace / ReplaceInColumn
// -----------------------------------------------------------------------

func TestDataFrame_Replace(t *testing.T) {
	df := New(
		series.New([]string{"a", "N/A", "b", "N/A"}, series.String, "v"),
	)
	out := df.Replace("N/A", nil)
	if !out.Col("v").Elem(1).IsNA() || !out.Col("v").Elem(3).IsNA() {
		t.Error("Replace: expected NaN at positions 1 and 3")
	}
}

func TestDataFrame_ReplaceInColumn(t *testing.T) {
	df := New(
		series.New([]string{"x", "bad", "y"}, series.String, "a"),
		series.New([]string{"bad", "ok", "bad"}, series.String, "b"),
	)
	out := df.ReplaceInColumn("b", "bad", nil)
	if !out.Col("b").Elem(0).IsNA() {
		t.Error("ReplaceInColumn: b[0] should be NaN")
	}
	if out.Col("a").Elem(0).String() != "x" {
		t.Error("ReplaceInColumn: column a should be unchanged")
	}
}

// -----------------------------------------------------------------------
// Astype
// -----------------------------------------------------------------------

func TestDataFrame_Astype(t *testing.T) {
	df := New(
		series.New([]string{"1", "2", "3"}, series.String, "n"),
		series.New([]string{"1.5", "2.5", "3.5"}, series.String, "f"),
	)
	out := df.Astype(map[string]series.Type{
		"n": series.Int,
		"f": series.Float,
	})
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Col("n").Type() != series.Int {
		t.Errorf("Astype n: expected Int, got %v", out.Col("n").Type())
	}
	if out.Col("f").Type() != series.Float {
		t.Errorf("Astype f: expected Float, got %v", out.Col("f").Type())
	}
}

// -----------------------------------------------------------------------
// Between — validation
// -----------------------------------------------------------------------

func TestDataFrame_Between_Valid(t *testing.T) {
	df := New(series.New([]float64{10, 20, 30}, series.Float, "v"))
	mask := df.Between("v", 15, 25, "both")
	if mask.Err != nil {
		t.Fatal(mask.Err)
	}
	b, _ := mask.Elem(1).Bool()
	if !b {
		t.Error("Between: 20 should be in [15,25]")
	}
}

func TestDataFrame_Between_InvalidBounds(t *testing.T) {
	df := New(series.New([]float64{1, 2, 3}, series.Float, "v"))
	mask := df.Between("v", 10, 5, "both") // left > right
	if mask.Err == nil {
		t.Error("Between left>right: expected error")
	}
}

// -----------------------------------------------------------------------
// IsIn / FilterIsIn
// -----------------------------------------------------------------------

func TestDataFrame_IsIn(t *testing.T) {
	df := New(series.New([]string{"US", "UK", "DE"}, series.String, "country"))
	mask := df.IsIn("country", []interface{}{"US", "UK"})
	if mask.Err != nil {
		t.Fatal(mask.Err)
	}
	b0, _ := mask.Elem(0).Bool()
	b2, _ := mask.Elem(2).Bool()
	if !b0 || b2 {
		t.Errorf("IsIn: expected [true true false], got [%v _ %v]", b0, b2)
	}
}

func TestDataFrame_FilterIsIn(t *testing.T) {
	df := New(
		series.New([]string{"US", "UK", "DE", "CA"}, series.String, "country"),
		series.New([]int{1, 2, 3, 4}, series.Int, "id"),
	)
	out := df.FilterIsIn("country", []interface{}{"US", "CA"})
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 2 {
		t.Errorf("FilterIsIn: expected 2 rows, got %d", out.Nrow())
	}
}

// -----------------------------------------------------------------------
// ExplodeOn — custom separator
// -----------------------------------------------------------------------

func TestDataFrame_ExplodeOn(t *testing.T) {
	df := New(
		series.New([]string{"a|b|c", "d|e"}, series.String, "tags"),
	)
	out := df.ExplodeOn("tags", "|")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 5 {
		t.Fatalf("ExplodeOn rows: got %d want 5", out.Nrow())
	}
	if out.Col("tags").Elem(0).String() != "a" {
		t.Errorf("ExplodeOn[0]: got %s want a", out.Col("tags").Elem(0).String())
	}
}

func TestDataFrame_ExplodeOn_EmptySep(t *testing.T) {
	df := New(series.New([]string{"a"}, series.String, "x"))
	out := df.ExplodeOn("x", "")
	if out.Err == nil {
		t.Error("ExplodeOn empty sep: expected error")
	}
}

// -----------------------------------------------------------------------
// Clip NaN in Series.Clip
// -----------------------------------------------------------------------

func TestSeries_Clip_NaN(t *testing.T) {
	s := series.New([]interface{}{-5.0, nil, 15.0}, series.Float, "v")
	lo, hi := 0.0, 10.0
	out := s.Clip(&lo, &hi)
	if !out.Elem(1).IsNA() {
		t.Error("Series.Clip: NaN should be preserved")
	}
	if out.Elem(0).Float() != 0 {
		t.Errorf("Series.Clip[0]: expected 0, got %v", out.Elem(0).Float())
	}
	if out.Elem(2).Float() != 10 {
		t.Errorf("Series.Clip[2]: expected 10, got %v", out.Elem(2).Float())
	}
}

// -----------------------------------------------------------------------
// Resample — all frequencies
// -----------------------------------------------------------------------

func TestDataFrame_Resample_AllFreqs(t *testing.T) {
	import_time := func(y, m, d, h int) interface{} {
		return nil // placeholder — use series.New with time.Time
	}
	_ = import_time

	// Use string-based time construction via series.New
	times := []string{
		"2024-01-15T10:00:00+00:00",
		"2024-01-15T11:00:00+00:00",
		"2024-01-16T09:00:00+00:00",
		"2024-02-01T08:00:00+00:00",
	}
	df := New(
		series.New(times, series.Time, "ts"),
		series.New([]float64{1, 2, 3, 4}, series.Float, "v"),
	)

	for _, freq := range []ResampleFreq{ResampleHourly, ResampleDaily, ResampleWeekly, ResampleMonthly, ResampleYearly} {
		rg := df.Resample("ts", freq)
		if rg.Err != nil {
			t.Errorf("Resample %s: %v", freq, rg.Err)
			continue
		}
		result := rg.Aggregation([]AggregationType{Aggregation_COUNT}, []string{"v"})
		if result.Err != nil {
			t.Errorf("Resample %s Aggregation: %v", freq, result.Err)
		}
		if result.Nrow() == 0 {
			t.Errorf("Resample %s: expected > 0 rows", freq)
		}
	}
}

// -----------------------------------------------------------------------
// numWorkers — GOMAXPROCS protection
// -----------------------------------------------------------------------

func TestNumWorkers(t *testing.T) {
	n := numWorkers()
	if n < 1 {
		t.Errorf("numWorkers: expected >= 1, got %d", n)
	}
}

// -----------------------------------------------------------------------
// Series.Abs NaN preservation
// -----------------------------------------------------------------------

func TestSeries_Abs_NaN(t *testing.T) {
	s := series.New([]interface{}{-3.0, nil, 4.0}, series.Float, "v")
	out := s.Abs()
	if !out.Elem(1).IsNA() {
		t.Error("Abs: NaN should be preserved")
	}
	if out.Elem(0).Float() != 3.0 {
		t.Errorf("Abs[0]: expected 3.0, got %v", out.Elem(0).Float())
	}
}

// -----------------------------------------------------------------------
// Series.Round edge cases
// -----------------------------------------------------------------------

func TestSeries_Round_NaN(t *testing.T) {
	s := series.New([]interface{}{1.5, nil, 2.5}, series.Float, "v")
	out := s.Round(0)
	if !out.Elem(1).IsNA() {
		t.Error("Round: NaN should be preserved")
	}
}

// -----------------------------------------------------------------------
// Categorical.Rename
// -----------------------------------------------------------------------

func TestCategorical_Rename(t *testing.T) {
	cat := series.NewCategorical([]string{"a", "b"}, "old")
	renamed := cat.Rename("new")
	if renamed.Name != "new" {
		t.Errorf("Rename: expected 'new', got %q", renamed.Name)
	}
	if cat.Name != "old" {
		t.Error("Rename: original should be unchanged")
	}
}

// -----------------------------------------------------------------------
// DataFrame error propagation through new methods
// -----------------------------------------------------------------------

func TestDataFrame_ErrorPropagation(t *testing.T) {
	bad := DataFrame{Err: fmt.Errorf("bad")}
	lo, hi := 0.0, 1.0

	if bad.Head(1).Err == nil {
		t.Error("Head: should propagate error")
	}
	if bad.Tail(1).Err == nil {
		t.Error("Tail: should propagate error")
	}
	if bad.IsNull().Err == nil {
		t.Error("IsNull: should propagate error")
	}
	if bad.NotNull().Err == nil {
		t.Error("NotNull: should propagate error")
	}
	if bad.Clip(&lo, &hi).Err == nil {
		t.Error("Clip: should propagate error")
	}
	if bad.Pipe(func(d DataFrame) DataFrame { return d }).Err == nil {
		t.Error("Pipe: should propagate error")
	}
	if bad.ExplodeOn("x", ",").Err == nil {
		t.Error("ExplodeOn: should propagate error")
	}
}

// -----------------------------------------------------------------------
// WriteXLSXSheet
// -----------------------------------------------------------------------

func TestDataFrame_WriteXLSXSheet(t *testing.T) {
	// WriteXLSXSheet requires *excelize.File — test via WriteXLSXMultiSheet
	df1 := New(series.New([]string{"a", "b"}, series.String, "col"))
	df2 := New(series.New([]int{1, 2, 3}, series.Int, "num"))
	var buf bytes.Buffer
	err := WriteXLSXMultiSheet(&buf, SheetData{"A", df1}, SheetData{"B", df2})
	if err != nil {
		t.Fatal(err)
	}
	// Read back sheet B
	got := ReadXLSX(bytes.NewReader(buf.Bytes()), WithSheet("B"))
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Nrow() != 3 {
		t.Errorf("WriteXLSXSheet B rows: got %d want 3", got.Nrow())
	}
}

// -----------------------------------------------------------------------
// ScanCSV — deep copy integrity (regression)
// -----------------------------------------------------------------------

func TestScanCSV_DeepCopy(t *testing.T) {
	csv := "a,b\n1,x\n2,y\n3,z\n4,w\n"
	var all []string
	err := ScanCSV(strings.NewReader(csv), 2, func(batch DataFrame) error {
		for i := 0; i < batch.Nrow(); i++ {
			all = append(all, batch.Col("a").Elem(i).String())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"1", "2", "3", "4"}
	if !reflect.DeepEqual(all, want) {
		t.Errorf("ScanCSV deep copy: got %v want %v", all, want)
	}
}

// -----------------------------------------------------------------------
// math helpers (not in series package — avoid import cycle)
// -----------------------------------------------------------------------

var _ = math.NaN // ensure math is used
