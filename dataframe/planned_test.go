package dataframe

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// v1.2 — GroupBy.Transform row-order preservation
// -----------------------------------------------------------------------

func TestGroups_Transform_RowOrder(t *testing.T) {
	// Rows: A,1  B,2  A,3  B,4
	// Group A mean = 2, Group B mean = 3
	// After transform, original order must be preserved: [2, 3, 2, 3]
	df := New(
		series.New([]string{"A", "B", "A", "B"}, series.String, "grp"),
		series.New([]float64{1, 2, 3, 4}, series.Float, "val"),
	)
	g := df.GroupBy("grp")
	if g.Err != nil {
		t.Fatal(g.Err)
	}
	transformed, err := g.Transform("val", func(s series.Series) series.Series {
		var sum float64
		for i := 0; i < s.Len(); i++ {
			sum += s.Elem(i).Float()
		}
		mean := sum / float64(s.Len())
		out := make([]float64, s.Len())
		for i := range out {
			out[i] = mean
		}
		return series.Floats(out)
	})
	if err != nil {
		t.Fatal(err)
	}
	if transformed.Len() != 4 {
		t.Fatalf("Transform length: got %d want 4", transformed.Len())
	}
	want := []float64{2, 3, 2, 3}
	for i, w := range want {
		got := transformed.Elem(i).Float()
		if !compareFloats(got, w, 9) {
			t.Errorf("Transform[%d]: got %v want %v", i, got, w)
		}
	}
}

// -----------------------------------------------------------------------
// v1.2 — GroupBy with Time key
// -----------------------------------------------------------------------

func TestGroupBy_TimeKey(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	df := New(
		series.New([]time.Time{t1, t2, t1, t2}, series.Time, "date"),
		series.New([]float64{1, 2, 3, 4}, series.Float, "val"),
	)
	g := df.GroupBy("date")
	if g.Err != nil {
		t.Fatalf("GroupBy Time key: unexpected error: %v", g.Err)
	}
	if len(g.groups) != 2 {
		t.Errorf("GroupBy Time key: expected 2 groups, got %d", len(g.groups))
	}
}

// -----------------------------------------------------------------------
// v1.2 — Describe with Time column
// -----------------------------------------------------------------------

func TestDataFrame_Describe_Time(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "val"),
		series.New([]time.Time{t1, t2, t1}, series.Time, "ts"),
	)
	desc := df.Describe()
	if desc.Err != nil {
		t.Fatalf("Describe with Time: %v", desc.Err)
	}
	// ts column should exist and have min/max populated.
	tsCol := desc.Col("ts")
	if tsCol.Err != nil {
		t.Fatalf("Describe: ts column missing: %v", tsCol.Err)
	}
	// Row 5 (index 5) is "min", row 9 is "max" (0-indexed, after count/nunique).
	minVal := tsCol.Elem(5).String()
	maxVal := tsCol.Elem(9).String()
	if minVal == "-" {
		t.Error("Describe Time: min should not be '-'")
	}
	if maxVal == "-" {
		t.Error("Describe Time: max should not be '-'")
	}
	if minVal > maxVal {
		t.Errorf("Describe Time: min %s > max %s", minVal, maxVal)
	}
}

// -----------------------------------------------------------------------
// v1.3 — DataFrame.Shift
// -----------------------------------------------------------------------

func TestDataFrame_Shift_Down(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "A"),
	)
	out := df.Shift(2)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 5 {
		t.Fatalf("Shift(2) rows: got %d want 5", out.Nrow())
	}
	// First 2 rows should be NaN.
	if !out.Col("A").Elem(0).IsNA() || !out.Col("A").Elem(1).IsNA() {
		t.Error("Shift(2): first 2 rows should be NaN")
	}
	// Row 2 should be original row 0 = 1.0
	if !compareFloats(out.Col("A").Elem(2).Float(), 1.0, 9) {
		t.Errorf("Shift(2): row 2 = %v want 1.0", out.Col("A").Elem(2).Float())
	}
}

func TestDataFrame_Shift_Up(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "A"),
	)
	out := df.Shift(-1)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	// Row 0 should be original row 1 = 2.0
	if !compareFloats(out.Col("A").Elem(0).Float(), 2.0, 9) {
		t.Errorf("Shift(-1): row 0 = %v want 2.0", out.Col("A").Elem(0).Float())
	}
	// Last row should be NaN.
	if !out.Col("A").Elem(4).IsNA() {
		t.Error("Shift(-1): last row should be NaN")
	}
}

func TestDataFrame_Shift_Zero(t *testing.T) {
	df := New(series.New([]float64{1, 2, 3}, series.Float, "A"))
	out := df.Shift(0)
	if !reflect.DeepEqual(df.Records(), out.Records()) {
		t.Error("Shift(0): should return identical DataFrame")
	}
}

func TestDataFrame_Shift_Subset(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "A"),
		series.New([]float64{4, 5, 6}, series.Float, "B"),
	)
	out := df.Shift(1, "A")
	// A shifted, B unchanged.
	if !out.Col("A").Elem(0).IsNA() {
		t.Error("Shift subset: A[0] should be NaN")
	}
	if compareFloats(out.Col("B").Elem(0).Float(), 4.0, 9) == false {
		t.Errorf("Shift subset: B[0] should be 4.0, got %v", out.Col("B").Elem(0).Float())
	}
}

// -----------------------------------------------------------------------
// v1.3 — DataFrame.Assign
// -----------------------------------------------------------------------

func TestDataFrame_Assign_NewColumn(t *testing.T) {
	df := New(
		series.New([]float64{10, 20, 30}, series.Float, "revenue"),
		series.New([]float64{3, 5, 8}, series.Float, "cost"),
	)
	out := df.Assign("profit", func(d DataFrame) series.Series {
		rev := d.Col("revenue").Float()
		cost := d.Col("cost").Float()
		out := make([]float64, len(rev))
		for i := range rev {
			out[i] = rev[i] - cost[i]
		}
		return series.Floats(out)
	})
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Ncol() != 3 {
		t.Fatalf("Assign new col: ncol=%d want 3", out.Ncol())
	}
	want := []float64{7, 15, 22}
	for i, w := range want {
		got := out.Col("profit").Elem(i).Float()
		if !compareFloats(got, w, 9) {
			t.Errorf("Assign profit[%d]: got %v want %v", i, got, w)
		}
	}
}

func TestDataFrame_Assign_ReplaceColumn(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "A"),
	)
	out := df.Assign("A", func(d DataFrame) series.Series {
		vals := d.Col("A").Float()
		out := make([]float64, len(vals))
		for i, v := range vals {
			out[i] = v * 2
		}
		return series.Floats(out)
	})
	if out.Ncol() != 1 {
		t.Fatalf("Assign replace: ncol=%d want 1", out.Ncol())
	}
	want := []float64{2, 4, 6}
	for i, w := range want {
		got := out.Col("A").Elem(i).Float()
		if !compareFloats(got, w, 9) {
			t.Errorf("Assign replace A[%d]: got %v want %v", i, got, w)
		}
	}
}

// -----------------------------------------------------------------------
// v1.3 — DataFrame.Explode
// -----------------------------------------------------------------------

func TestDataFrame_Explode_Basic(t *testing.T) {
	df := New(
		series.New([]string{"1", "2"}, series.String, "id"),
		series.New([]string{"go,python", "rust"}, series.String, "tags"),
	)
	out := df.Explode("tags")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	// Row 0: (1, go), Row 1: (1, python), Row 2: (2, rust)
	if out.Nrow() != 3 {
		t.Fatalf("Explode rows: got %d want 3", out.Nrow())
	}
	if out.Col("tags").Elem(0).String() != "go" {
		t.Errorf("Explode[0] tags: got %s want go", out.Col("tags").Elem(0).String())
	}
	if out.Col("id").Elem(1).String() != "1" {
		t.Errorf("Explode[1] id: got %s want 1", out.Col("id").Elem(1).String())
	}
	if out.Col("tags").Elem(2).String() != "rust" {
		t.Errorf("Explode[2] tags: got %s want rust", out.Col("tags").Elem(2).String())
	}
}

func TestDataFrame_Explode_InvalidColumn(t *testing.T) {
	df := New(series.New([]string{"a"}, series.String, "x"))
	out := df.Explode("no_such")
	if out.Err == nil {
		t.Error("Explode invalid column: expected error")
	}
}

// -----------------------------------------------------------------------
// v1.2 — Rolling.StdDev Welford O(n) correctness
// -----------------------------------------------------------------------

func TestDataFrame_RollingStdDev_Welford(t *testing.T) {
	// Verify that the new Welford implementation matches expected values.
	df := New(
		series.New([]float64{2, 4, 4, 4, 5, 5, 7, 9}, series.Float, "A"),
	)
	// window=3, expected stddev values (ddof=1):
	// [NaN, NaN, 1.1547, 0.0, 0.5774, 0.5774, 1.1547, 2.0]
	s := df.Col("A")
	got := s.Rolling(3).StdDev()
	want := []float64{math.NaN(), math.NaN(), 1.1547005, 0.0, 0.5773503, 0.5773503, 1.1547005, 2.0}
	for i, w := range want {
		g := got.Elem(i).Float()
		if math.IsNaN(w) {
			if !math.IsNaN(g) {
				t.Errorf("StdDev[%d]: got %v want NaN", i, g)
			}
			continue
		}
		if math.Abs(g-w) > 1e-5 {
			t.Errorf("StdDev[%d]: got %v want %v", i, g, w)
		}
	}
}
