package series

import (
	"math"
	"testing"
)

// -----------------------------------------------------------------------
// EWM – Mean
// -----------------------------------------------------------------------

func TestEWM_Mean_Adjusted(t *testing.T) {
	// Cross-check with pandas:
	//   pd.Series([1,2,3,4,5]).ewm(span=3, adjust=True).mean()
	// span=3  → alpha = 0.5
	// [1.0, 1.666..., 2.428..., 3.266..., 4.161...]
	s := Floats([]float64{1, 2, 3, 4, 5})
	got := seriesFloats(s.EWM(3).Mean())
	want := []float64{1.0, 1.0 + 2.0/3.0, 2.4285714, 3.2666667, 4.1612903}

	if len(got) != len(want) {
		t.Fatalf("EWM Mean adjusted: length mismatch got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if !rollingFloatEq(got[i], want[i], 1e-4) {
			t.Errorf("EWM Mean adjusted[%d]: got %.7f want %.7f", i, got[i], want[i])
		}
	}
}

func TestEWM_Mean_NonAdjusted(t *testing.T) {
	// span=2, alpha=2/3, adjust=False
	// ewma[0]=1, ewma[1]= (2/3)*2+(1/3)*1 = 5/3, ...
	s := Floats([]float64{1, 2, 3})
	got := seriesFloats(s.EWM(2).Adjust(false).Mean())
	want := []float64{1.0, 5.0 / 3.0, (2.0/3.0)*3.0 + (1.0/3.0)*(5.0/3.0)}
	if len(got) != len(want) {
		t.Fatalf("EWM Mean non-adjusted: length mismatch")
	}
	for i := range want {
		if !rollingFloatEq(got[i], want[i], 1e-9) {
			t.Errorf("EWM Mean non-adjusted[%d]: got %.9f want %.9f", i, got[i], want[i])
		}
	}
}

func TestEWM_Mean_MinPeriods(t *testing.T) {
	// minPeriods=2: first element should be NaN
	s := Floats([]float64{1, 2, 3})
	got := seriesFloats(s.EWM(3).MinPeriods(2).Mean())
	if !math.IsNaN(got[0]) {
		t.Errorf("EWM Mean minPeriods=2: got[0]=%v, want NaN", got[0])
	}
	if math.IsNaN(got[1]) {
		t.Errorf("EWM Mean minPeriods=2: got[1]=NaN, want a real number")
	}
}

func TestEWM_Mean_WithNaN(t *testing.T) {
	// NaN in the middle: non-adjusted mode, ignoreNA=false → resets ewma
	s := New([]interface{}{1.0, nil, 3.0}, Float, "x")
	got := seriesFloats(s.EWM(2).Adjust(false).Mean())
	if len(got) != 3 {
		t.Fatalf("EWM Mean with NaN: length mismatch")
	}
	// First value is 1, second is NaN (NaN input resets), third should be 3
	if !rollingFloatEq(got[0], 1.0, 1e-9) {
		t.Errorf("EWM Mean NaN[0]: got %v want 1.0", got[0])
	}
	// Slot 2 (NaN input) → ewma resets to NaN
	if !math.IsNaN(got[1]) {
		t.Errorf("EWM Mean NaN[1]: got %v want NaN", got[1])
	}
}

func TestEWMAlpha(t *testing.T) {
	// Direct alpha construction: same result as EWM(span) when span derived
	// alpha = 0.5 → span = 3
	s := Floats([]float64{1, 2, 3, 4, 5})
	bySpan := seriesFloats(s.EWM(3).Mean())
	byAlpha := seriesFloats(s.EWMAlpha(0.5).Mean())
	for i := range bySpan {
		if !rollingFloatEq(bySpan[i], byAlpha[i], 1e-9) {
			t.Errorf("EWMAlpha[%d]: span=%v alpha=%v", i, bySpan[i], byAlpha[i])
		}
	}
}

// -----------------------------------------------------------------------
// EWM – Std
// -----------------------------------------------------------------------

func TestEWM_Std(t *testing.T) {
	// std = sqrt(var); just sanity-check non-NaN values are >= 0
	s := Floats([]float64{1, 2, 3, 4, 5, 6, 7})
	std := seriesFloats(s.EWM(3).Std())
	for i, v := range std {
		if !math.IsNaN(v) && v < 0 {
			t.Errorf("EWM Std[%d]: got negative value %v", i, v)
		}
	}
}

// -----------------------------------------------------------------------
// EWM – Var (non-negativity)
// -----------------------------------------------------------------------

func TestEWM_Var(t *testing.T) {
	s := Floats([]float64{2, 4, 4, 4, 5, 5, 7, 9})
	vv := seriesFloats(s.EWM(3).Var())
	for i, v := range vv {
		if !math.IsNaN(v) && v < 0 {
			t.Errorf("EWM Var[%d]: got negative value %v", i, v)
		}
	}
}
