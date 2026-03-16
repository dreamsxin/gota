package dataframe

import (
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------

func dfColFloats(df DataFrame, col string) []float64 {
	s := df.Col(col)
	out := make([]float64, s.Len())
	for i := range out {
		out[i] = s.Elem(i).Float()
	}
	return out
}

func dfFloatSliceEq(t *testing.T, tag string, got, want []float64, eps float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: length mismatch got=%d want=%d", tag, len(got), len(want))
		return
	}
	for i := range want {
		g, w := got[i], want[i]
		if math.IsNaN(w) && math.IsNaN(g) {
			continue
		}
		if math.IsNaN(w) || math.IsNaN(g) || math.Abs(g-w) > eps {
			t.Errorf("%s[%d]: got %v, want %v", tag, i, g, w)
		}
	}
}

// -----------------------------------------------------------------------
// CumSum
// -----------------------------------------------------------------------

func TestDataFrame_CumSum(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4}, series.Float, "A"),
		series.New([]string{"a", "b", "c", "d"}, series.String, "B"),
	)
	out := df.CumSum()
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	wantA := []float64{1, 3, 6, 10}
	dfFloatSliceEq(t, "CumSum A", dfColFloats(out, "A"), wantA, 1e-9)
	// String column unchanged.
	if !reflect.DeepEqual(out.Col("B").Records(), df.Col("B").Records()) {
		t.Errorf("CumSum: string column B should be unchanged")
	}
}

func TestDataFrame_CumSum_Subset(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "A"),
		series.New([]float64{10, 20, 30}, series.Float, "B"),
	)
	out := df.CumSum("A") // only column A
	dfFloatSliceEq(t, "CumSum subset A", dfColFloats(out, "A"), []float64{1, 3, 6}, 1e-9)
	// B should be unchanged.
	dfFloatSliceEq(t, "CumSum subset B (unchanged)", dfColFloats(out, "B"), []float64{10, 20, 30}, 1e-9)
}

// -----------------------------------------------------------------------
// CumProd
// -----------------------------------------------------------------------

func TestDataFrame_CumProd(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4}, series.Float, "A"),
	)
	out := df.CumProd()
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	dfFloatSliceEq(t, "CumProd A", dfColFloats(out, "A"), []float64{1, 2, 6, 24}, 1e-9)
}

// -----------------------------------------------------------------------
// Diff
// -----------------------------------------------------------------------

func TestDataFrame_Diff(t *testing.T) {
	df := New(
		series.New([]float64{1, 3, 6, 10, 15}, series.Float, "A"),
	)
	out := df.Diff(1)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	want := []float64{math.NaN(), 2, 3, 4, 5}
	dfFloatSliceEq(t, "Diff(1) A", dfColFloats(out, "A"), want, 1e-9)
}

func TestDataFrame_Diff_Periods2(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "A"),
	)
	out := df.Diff(2)
	want := []float64{math.NaN(), math.NaN(), 2, 2, 2}
	dfFloatSliceEq(t, "Diff(2) A", dfColFloats(out, "A"), want, 1e-9)
}

// -----------------------------------------------------------------------
// PctChange
// -----------------------------------------------------------------------

func TestDataFrame_PctChange(t *testing.T) {
	df := New(
		series.New([]float64{100, 110, 121}, series.Float, "A"),
	)
	out := df.PctChange(1)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	want := []float64{math.NaN(), 0.1, 0.1}
	dfFloatSliceEq(t, "PctChange(1) A", dfColFloats(out, "A"), want, 1e-6)
}

// -----------------------------------------------------------------------
// FillNAStrategyLimit
// -----------------------------------------------------------------------

func TestDataFrame_FillNAStrategyLimit_Ffill(t *testing.T) {
	df := New(
		series.New([]interface{}{1.0, nil, nil, nil, 5.0}, series.Float, "A"),
	)
	out := df.FillNAStrategyLimit(NAFillForward, 2)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	want := []float64{1.0, 1.0, 1.0, math.NaN(), 5.0}
	dfFloatSliceEq(t, "FillNAStrategyLimit ffill limit=2", dfColFloats(out, "A"), want, 1e-9)
}

func TestDataFrame_FillNAStrategyLimit_Bfill(t *testing.T) {
	df := New(
		series.New([]interface{}{nil, nil, nil, 4.0, 5.0}, series.Float, "A"),
	)
	out := df.FillNAStrategyLimit(NAFillBackward, 1)
	want := []float64{math.NaN(), math.NaN(), 4.0, 4.0, 5.0}
	dfFloatSliceEq(t, "FillNAStrategyLimit bfill limit=1", dfColFloats(out, "A"), want, 1e-9)
}

// -----------------------------------------------------------------------
// Corr / Cov
// -----------------------------------------------------------------------

func TestDataFrame_Corr(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "A"),
		series.New([]float64{5, 4, 3, 2, 1}, series.Float, "B"),
	)
	corrDF := df.Corr()
	if corrDF.Err != nil {
		t.Fatal(corrDF.Err)
	}
	// Expect 2 rows, columns: "" (label), A, B
	if corrDF.Nrow() != 2 {
		t.Errorf("Corr rows: got %d want 2", corrDF.Nrow())
	}
	// A-A = 1.0
	aaIdx := corrDF.ColIndex("A")
	if aaIdx < 0 {
		t.Fatal("Corr: column A not found in result")
	}
	aaVal := corrDF.Col("A").Elem(0).Float()
	if !compareFloats(aaVal, 1.0, 10) {
		t.Errorf("Corr A-A: got %v, want 1.0", aaVal)
	}
	// A-B = -1.0
	abVal := corrDF.Col("B").Elem(0).Float()
	if !compareFloats(abVal, -1.0, 10) {
		t.Errorf("Corr A-B: got %v, want -1.0", abVal)
	}
}

func TestDataFrame_Cov(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "A"),
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "B"),
	)
	covDF := df.Cov()
	if covDF.Err != nil {
		t.Fatal(covDF.Err)
	}
	// Var(A) = 2.5 (sample)
	aaVal := covDF.Col("A").Elem(0).Float()
	if !compareFloats(aaVal, 2.5, 6) {
		t.Errorf("Cov A-A: got %v, want 2.5", aaVal)
	}
	// Cov(A,B) = Var(A) since A==B
	abVal := covDF.Col("B").Elem(0).Float()
	if !compareFloats(abVal, 2.5, 6) {
		t.Errorf("Cov A-B: got %v, want 2.5", abVal)
	}
}

// -----------------------------------------------------------------------
// Melt
// -----------------------------------------------------------------------

func TestDataFrame_Melt_Basic(t *testing.T) {
	df := New(
		series.New([]string{"x", "y"}, series.String, "id"),
		series.New([]float64{1, 2}, series.Float, "v1"),
		series.New([]float64{3, 4}, series.Float, "v2"),
	)
	out := df.Melt([]string{"id"}, []string{"v1", "v2"}, "variable", "value")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	// Expect 4 rows: (x,v1,1), (x,v2,3), (y,v1,2), (y,v2,4)
	if out.Nrow() != 4 {
		t.Errorf("Melt rows: got %d want 4", out.Nrow())
	}
	// Column names: id, variable, value
	names := out.Names()
	if !reflect.DeepEqual(names, []string{"id", "variable", "value"}) {
		t.Errorf("Melt names: got %v want [id variable value]", names)
	}
}

func TestDataFrame_Melt_AllValueVars(t *testing.T) {
	// When valueVars is empty, all non-id columns become value vars.
	df := New(
		series.New([]string{"a", "b"}, series.String, "key"),
		series.New([]float64{10, 20}, series.Float, "X"),
		series.New([]float64{30, 40}, series.Float, "Y"),
	)
	out := df.Melt([]string{"key"}, nil, "", "")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 4 {
		t.Errorf("Melt all-valueVars rows: got %d want 4", out.Nrow())
	}
	// Default column names
	names := out.Names()
	if names[1] != "variable" || names[2] != "value" {
		t.Errorf("Melt default names: got %v", names)
	}
}

func TestDataFrame_Melt_InvalidColumn(t *testing.T) {
	df := New(
		series.New([]string{"a"}, series.String, "id"),
	)
	out := df.Melt([]string{"no_such"}, nil, "", "")
	if out.Err == nil {
		t.Error("Melt with invalid id column: expected error, got nil")
	}
}

// -----------------------------------------------------------------------
// GroupBy.Apply
// -----------------------------------------------------------------------

func TestGroups_Apply(t *testing.T) {
	df := New(
		series.New([]string{"A", "B", "A", "B"}, series.String, "grp"),
		series.New([]float64{1, 2, 3, 4}, series.Float, "val"),
	)
	g := df.GroupBy("grp")
	if g.Err != nil {
		t.Fatal(g.Err)
	}
	// Apply: return only the first row of each group.
	result := g.Apply(func(sub DataFrame) DataFrame {
		return sub.Subset([]int{0})
	})
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	// Expect 2 rows (one per group).
	if result.Nrow() != 2 {
		t.Errorf("Groups.Apply rows: got %d want 2", result.Nrow())
	}
}

// -----------------------------------------------------------------------
// GroupBy.Transform
// -----------------------------------------------------------------------

func TestGroups_Transform(t *testing.T) {
	df := New(
		series.New([]string{"A", "A", "B", "B"}, series.String, "grp"),
		series.New([]float64{1, 3, 2, 4}, series.Float, "val"),
	)
	g := df.GroupBy("grp")
	if g.Err != nil {
		t.Fatal(g.Err)
	}
	// Transform: replace values with their group mean.
	transformed, err := g.Transform("val", func(s series.Series) series.Series {
		var sum float64
		for i := 0; i < s.Len(); i++ {
			sum += s.Elem(i).Float()
		}
		mean := sum / float64(s.Len())
		vals := make([]float64, s.Len())
		for i := range vals {
			vals[i] = mean
		}
		return series.Floats(vals)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Group A mean=2, Group B mean=3  → 4 rows
	if transformed.Len() != 4 {
		t.Errorf("Groups.Transform length: got %d want 4", transformed.Len())
	}
	vals := make([]float64, transformed.Len())
	for i := range vals {
		vals[i] = transformed.Elem(i).Float()
	}
	sort.Float64s(vals)
	// After sorting: [2, 2, 3, 3]
	want := []float64{2, 2, 3, 3}
	for i, w := range want {
		if !compareFloats(vals[i], w, 9) {
			t.Errorf("Groups.Transform sorted[%d]: got %v want %v", i, vals[i], w)
		}
	}
}
