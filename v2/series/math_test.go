package series

import (
	"math"
	"testing"
)

func TestSeries_Abs(t *testing.T) {
	s := Floats([]float64{-3, 0, 4, -1.5, math.NaN()})
	got := s.Abs().Float()
	want := []float64{3, 0, 4, 1.5, math.NaN()}
	floatSliceEq(t, "Abs", got, want, 1e-9)
}

func TestSeries_Round(t *testing.T) {
	s := Floats([]float64{1.234, 2.567, -1.5, math.NaN()})
	got := s.Round(2).Float()
	want := []float64{1.23, 2.57, -1.5, math.NaN()}
	floatSliceEq(t, "Round(2)", got, want, 1e-9)

	got0 := s.Round(0).Float()
	want0 := []float64{1, 3, -2, math.NaN()}
	floatSliceEq(t, "Round(0)", got0, want0, 1e-9)
}

func TestSeries_Sign(t *testing.T) {
	s := Floats([]float64{-5, 0, 3, math.NaN()})
	got := s.Sign().Float()
	want := []float64{-1, 0, 1, math.NaN()}
	floatSliceEq(t, "Sign", got, want, 1e-9)
}

func TestSeries_Pow(t *testing.T) {
	s := Floats([]float64{2, 3, 4})
	got := s.Pow(2).Float()
	want := []float64{4, 9, 16}
	floatSliceEq(t, "Pow(2)", got, want, 1e-9)
}

func TestSeries_Sqrt(t *testing.T) {
	s := Floats([]float64{4, 9, 16, -1})
	got := s.Sqrt().Float()
	want := []float64{2, 3, 4, math.NaN()}
	floatSliceEq(t, "Sqrt", got, want, 1e-9)
}

func TestSeries_Log(t *testing.T) {
	s := Floats([]float64{1, math.E, 0})
	got := s.Log().Float()
	floatSliceEq(t, "Log", got, []float64{0, 1, math.Inf(-1)}, 1e-9)
}

func TestSeries_Exp(t *testing.T) {
	s := Floats([]float64{0, 1, 2})
	got := s.Exp().Float()
	want := []float64{1, math.E, math.E * math.E}
	floatSliceEq(t, "Exp", got, want, 1e-6)
}

func TestSeries_Log10(t *testing.T) {
	s := Floats([]float64{1, 10, 100})
	got := s.Log10().Float()
	want := []float64{0, 1, 2}
	floatSliceEq(t, "Log10", got, want, 1e-9)
}

func BenchmarkSeries_Abs(b *testing.B) {
	vals := make([]float64, 100000)
	for i := range vals {
		vals[i] = float64(i) - 50000
	}
	s := Floats(vals)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Abs()
	}
}

func BenchmarkSeries_Round(b *testing.B) {
	vals := make([]float64, 100000)
	for i := range vals {
		vals[i] = float64(i) * 0.123456
	}
	s := Floats(vals)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Round(2)
	}
}
