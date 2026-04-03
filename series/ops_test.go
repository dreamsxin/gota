package series

import (
	"math"
	"reflect"
	"testing"
)

// -----------------------------------------------------------------------
// Series.Clip
// -----------------------------------------------------------------------

func TestSeries_Clip_Both(t *testing.T) {
	s := Floats([]float64{-5, 0, 3, 10, 15})
	lo, hi := 0.0, 10.0
	got := s.Clip(&lo, &hi).Float()
	want := []float64{0, 0, 3, 10, 10}
	floatSliceEq(t, "Clip both", got, want, 1e-9)
}

func TestSeries_Clip_LowerOnly(t *testing.T) {
	s := Floats([]float64{-1, 2, 5})
	lo := 0.0
	got := s.Clip(&lo, nil).Float()
	want := []float64{0, 2, 5}
	floatSliceEq(t, "Clip lower only", got, want, 1e-9)
}

func TestSeries_Clip_UpperOnly(t *testing.T) {
	s := Floats([]float64{1, 5, 100})
	hi := 10.0
	got := s.Clip(nil, &hi).Float()
	want := []float64{1, 5, 10}
	floatSliceEq(t, "Clip upper only", got, want, 1e-9)
}

func TestSeries_Clip_NonNumeric(t *testing.T) {
	s := Strings([]string{"a", "b"})
	lo, hi := 0.0, 1.0
	got := s.Clip(&lo, &hi)
	// Non-numeric: returned unchanged.
	if !reflect.DeepEqual(got.Records(), s.Records()) {
		t.Errorf("Clip non-numeric: got %v want %v", got.Records(), s.Records())
	}
}

// -----------------------------------------------------------------------
// Series.Replace
// -----------------------------------------------------------------------

func TestSeries_Replace_String(t *testing.T) {
	s := Strings([]string{"a", "N/A", "b", "N/A"})
	got := s.Replace("N/A", nil)
	if !got.Elem(1).IsNA() || !got.Elem(3).IsNA() {
		t.Error("Replace: expected NaN at positions 1 and 3")
	}
	if got.Elem(0).String() != "a" {
		t.Errorf("Replace: elem[0] should be 'a', got %s", got.Elem(0).String())
	}
}

func TestSeries_Replace_Int(t *testing.T) {
	s := Ints([]int{1, 0, 2, 0})
	got := s.Replace(0, nil)
	if !got.Elem(1).IsNA() || !got.Elem(3).IsNA() {
		t.Error("Replace int: expected NaN at positions 1 and 3")
	}
}

// -----------------------------------------------------------------------
// Series.Between
// -----------------------------------------------------------------------

func TestSeries_Between_Both(t *testing.T) {
	s := Floats([]float64{10, 18, 30, 65, 70})
	got, _ := s.Between(18, 65, "both").Bool()
	want := []bool{false, true, true, true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Between both: got %v want %v", got, want)
	}
}

func TestSeries_Between_Neither(t *testing.T) {
	s := Floats([]float64{18, 19, 64, 65})
	got, _ := s.Between(18, 65, "neither").Bool()
	want := []bool{false, true, true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Between neither: got %v want %v", got, want)
	}
}

func TestSeries_Between_NaN(t *testing.T) {
	s := Floats([]float64{math.NaN(), 5, 10})
	got, _ := s.Between(0, 10, "both").Bool()
	if got[0] {
		t.Error("Between: NaN should produce false")
	}
}

// -----------------------------------------------------------------------
// Series.IsIn
// -----------------------------------------------------------------------

func TestSeries_IsIn_String(t *testing.T) {
	s := Strings([]string{"US", "UK", "DE", "CA"})
	got, _ := s.IsIn([]interface{}{"US", "UK", "CA"}).Bool()
	want := []bool{true, true, false, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IsIn string: got %v want %v", got, want)
	}
}

func TestSeries_IsIn_Int(t *testing.T) {
	s := Ints([]int{1, 2, 3, 4, 5})
	got, _ := s.IsIn([]interface{}{2, 4}).Bool()
	want := []bool{false, true, false, true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IsIn int: got %v want %v", got, want)
	}
}
