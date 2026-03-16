package series

import (
	"math"
	"testing"
)

// -----------------------------------------------------------------------
// helper
// -----------------------------------------------------------------------

// floatSliceEq compares two float64 slices element-wise,
// treating NaN==NaN as equal. eps is absolute tolerance.
func floatSliceEq(t *testing.T, tag string, got, want []float64, eps float64) {
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

func seriesFloats(s Series) []float64 {
	out := make([]float64, s.Len())
	for i := range out {
		out[i] = s.Elem(i).Float()
	}
	return out
}

// -----------------------------------------------------------------------
// CumSum
// -----------------------------------------------------------------------

func TestSeries_CumSum(t *testing.T) {
	tests := []struct {
		name   string
		input  Series
		expect []float64
	}{
		{
			"int series",
			Ints([]int{1, 2, 3, 4, 5}),
			[]float64{1, 3, 6, 10, 15},
		},
		{
			"float series",
			Floats([]float64{1.5, 2.5, 3.0}),
			[]float64{1.5, 4.0, 7.0},
		},
		{
			"with NaN propagates",
			New([]interface{}{1.0, nil, 3.0}, Float, "x"),
			[]float64{1.0, math.NaN(), math.NaN()},
		},
		{
			"empty",
			Floats([]float64{}),
			[]float64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := seriesFloats(tt.input.CumSum())
			floatSliceEq(t, "CumSum", got, tt.expect, 1e-9)
		})
	}
}

// -----------------------------------------------------------------------
// CumProd
// -----------------------------------------------------------------------

func TestSeries_CumProd(t *testing.T) {
	tests := []struct {
		name   string
		input  Series
		expect []float64
	}{
		{
			"integers",
			Ints([]int{1, 2, 3, 4, 5}),
			[]float64{1, 2, 6, 24, 120},
		},
		{
			"with NaN propagates",
			New([]interface{}{2.0, nil, 3.0}, Float, "x"),
			[]float64{2.0, math.NaN(), math.NaN()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := seriesFloats(tt.input.CumProd())
			floatSliceEq(t, "CumProd", got, tt.expect, 1e-9)
		})
	}
}

// -----------------------------------------------------------------------
// CumMax / CumMin
// -----------------------------------------------------------------------

func TestSeries_CumMax(t *testing.T) {
	s := Ints([]int{3, 1, 4, 1, 5, 9, 2, 6})
	want := []float64{3, 3, 4, 4, 5, 9, 9, 9}
	floatSliceEq(t, "CumMax", seriesFloats(s.CumMax()), want, 1e-9)
}

func TestSeries_CumMin(t *testing.T) {
	s := Ints([]int{3, 1, 4, 1, 5, 9, 2, 6})
	want := []float64{3, 1, 1, 1, 1, 1, 1, 1}
	floatSliceEq(t, "CumMin", seriesFloats(s.CumMin()), want, 1e-9)
}

func TestSeries_CumMax_NaN(t *testing.T) {
	s := New([]interface{}{2.0, nil, 5.0, 1.0}, Float, "x")
	// NaN row stays NaN, preceding max carries forward
	want := []float64{2.0, math.NaN(), 5.0, 5.0}
	floatSliceEq(t, "CumMax NaN", seriesFloats(s.CumMax()), want, 1e-9)
}

// -----------------------------------------------------------------------
// Diff
// -----------------------------------------------------------------------

func TestSeries_Diff(t *testing.T) {
	tests := []struct {
		name    string
		input   Series
		periods int
		expect  []float64
	}{
		{
			"periods=1",
			Ints([]int{1, 3, 6, 10, 15}),
			1,
			[]float64{math.NaN(), 2, 3, 4, 5},
		},
		{
			"periods=2",
			Ints([]int{1, 2, 3, 4, 5}),
			2,
			[]float64{math.NaN(), math.NaN(), 2, 2, 2},
		},
		{
			"periods=-1 (lead)",
			Ints([]int{1, 2, 3, 4, 5}),
			-1,
			[]float64{-1, -1, -1, -1, math.NaN()},
		},
		{
			// i=3: s[3]=4.0, s[2]=3.0 (both non-NaN) → diff = 1.0
			// i=2: s[2]=3.0, s[1]=NaN → NaN
			"with NaN",
			New([]interface{}{1.0, nil, 3.0, 4.0}, Float, "x"),
			1,
			[]float64{math.NaN(), math.NaN(), math.NaN(), 1.0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := seriesFloats(tt.input.Diff(tt.periods))
			floatSliceEq(t, "Diff", got, tt.expect, 1e-9)
		})
	}
}

// -----------------------------------------------------------------------
// PctChange
// -----------------------------------------------------------------------

func TestSeries_PctChange(t *testing.T) {
	tests := []struct {
		name    string
		input   Series
		periods int
		expect  []float64
	}{
		{
			"basic",
			Floats([]float64{100, 110, 121, 133.1}),
			1,
			[]float64{math.NaN(), 0.1, 0.1, 0.1},
		},
		{
			"zero base → NaN",
			Floats([]float64{0, 1, 2}),
			1,
			[]float64{math.NaN(), math.NaN(), 1.0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := seriesFloats(tt.input.PctChange(tt.periods))
			floatSliceEq(t, "PctChange", got, tt.expect, 1e-6)
		})
	}
}

// -----------------------------------------------------------------------
// FillNaNForwardLimit / FillNaNBackwardLimit
// -----------------------------------------------------------------------

func TestSeries_FillNaNForwardLimit(t *testing.T) {
	s := New([]interface{}{1.0, nil, nil, nil, 5.0}, Float, "x")
	// limit=2: fills first two NaN, third stays NaN
	got := seriesFloats(s.FillNaNForwardLimit(2))
	want := []float64{1.0, 1.0, 1.0, math.NaN(), 5.0}
	floatSliceEq(t, "FillNaNForwardLimit(2)", got, want, 1e-9)

	// limit=0 means no limit, fills all
	got0 := seriesFloats(s.FillNaNForwardLimit(0))
	want0 := []float64{1.0, 1.0, 1.0, 1.0, 5.0}
	floatSliceEq(t, "FillNaNForwardLimit(0)", got0, want0, 1e-9)
}

func TestSeries_FillNaNBackwardLimit(t *testing.T) {
	s := New([]interface{}{nil, nil, nil, 4.0, 5.0}, Float, "x")
	// limit=1: fills only the NaN immediately before 4.0
	got := seriesFloats(s.FillNaNBackwardLimit(1))
	want := []float64{math.NaN(), math.NaN(), 4.0, 4.0, 5.0}
	floatSliceEq(t, "FillNaNBackwardLimit(1)", got, want, 1e-9)
}

// -----------------------------------------------------------------------
// Corr
// -----------------------------------------------------------------------

func TestSeries_Corr(t *testing.T) {
	a := Floats([]float64{1, 2, 3, 4, 5})
	b := Floats([]float64{5, 4, 3, 2, 1})
	c := Floats([]float64{1, 2, 3, 4, 5})

	// Perfect negative correlation.
	if r := a.Corr(b); !compareFloats(r, -1.0, 10) {
		t.Errorf("Corr(a,b): got %v, want -1.0", r)
	}
	// Perfect positive correlation.
	if r := a.Corr(c); !compareFloats(r, 1.0, 10) {
		t.Errorf("Corr(a,c): got %v, want 1.0", r)
	}
	// Different lengths → NaN.
	d := Floats([]float64{1, 2})
	if r := a.Corr(d); !math.IsNaN(r) {
		t.Errorf("Corr(different lengths): got %v, want NaN", r)
	}
}

// -----------------------------------------------------------------------
// Cov
// -----------------------------------------------------------------------

func TestSeries_Cov(t *testing.T) {
	a := Floats([]float64{2, 4, 4, 4, 5, 5, 7, 9})
	// Self-covariance = sample variance.
	variance := a.Cov(a)
	// Manually: mean=5, sum of squared diffs=32, sample var=32/7≈4.571
	expected := 32.0 / 7.0
	if !compareFloats(variance, expected, 6) {
		t.Errorf("Cov(a,a): got %v, want %v", variance, expected)
	}
	// NaN pairs skipped.
	x := New([]interface{}{1.0, nil, 3.0}, Float, "x")
	y := New([]interface{}{2.0, nil, 4.0}, Float, "y")
	cov := x.Cov(y)
	if math.IsNaN(cov) {
		t.Errorf("Cov with NaN pairs: got NaN, expected a real value")
	}
}


