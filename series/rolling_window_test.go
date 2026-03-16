package series

import (
	"math"
	"strings"
	"testing"
)

// rollingFloatEq compares two float64 values with a relative+absolute epsilon.
// NaN == NaN returns true for test purposes.
func rollingFloatEq(a, b, eps float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsNaN(a) || math.IsNaN(b) {
		return false
	}
	diff := math.Abs(a - b)
	return diff <= eps || diff <= eps*math.Max(math.Abs(a), math.Abs(b))
}

func TestSeries_RollingMean(t *testing.T) {
	tests := []struct {
		window   int
		series   Series
		expected Series
	}{
		{
			3,
			Ints([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			Floats([]float64{math.NaN(), math.NaN(), 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}),
		},
		{
			2,
			Floats([]float64{1.0, 2.0, 3.0}),
			Floats([]float64{math.NaN(), 1.5, 2.5}),
		},
		{
			0,
			Floats([]float64{}),
			Floats([]float64{}),
		},
	}

	for testnum, test := range tests {
		expected := test.expected
		received := test.series.Rolling(test.window).Mean()

		for i := 0; i < expected.Len(); i++ {
			if strings.Compare(expected.Elem(i).String(),
				received.Elem(i).String()) != 0 {
				t.Errorf(
					"Test:%v\nExpected:\n%v\nReceived:\n%v",
					testnum, expected, received,
				)
			}
		}
	}
}

func TestSeries_RollingStdDev(t *testing.T) {
	const eps = 1e-13 // allow last-few-ULP floating-point rounding differences

	tests := []struct {
		window   int
		series   Series
		expected []float64
	}{
		{
			3,
			Ints([]int{5, 5, 6, 7, 5, 5, 5}),
			[]float64{math.NaN(), math.NaN(), 0.5773502691896258, 1.0, 1.0, 1.1547005383792515, 0.0},
		},
		{
			2,
			Floats([]float64{1.0, 2.0, 3.0}),
			[]float64{math.NaN(), 0.7071067811865476, 0.7071067811865476},
		},
		{
			0,
			Floats([]float64{}),
			[]float64{},
		},
	}

	for testnum, test := range tests {
		received := test.series.Rolling(test.window).StdDev()

		if received.Len() != len(test.expected) {
			t.Errorf("Test:%v length mismatch: expected %d got %d",
				testnum, len(test.expected), received.Len())
			continue
		}
		for i, expVal := range test.expected {
			gotVal := received.Elem(i).Float()
			if !rollingFloatEq(expVal, gotVal, eps) {
				t.Errorf(
					"Test:%v index %d: expected %v got %v",
					testnum, i, expVal, gotVal,
				)
			}
		}
	}
}

func TestSeries_RollingSum(t *testing.T) {
	tests := []struct {
		window   int
		series   Series
		expected []float64
	}{
		{
			3,
			Ints([]int{1, 2, 3, 4, 5}),
			[]float64{math.NaN(), math.NaN(), 6.0, 9.0, 12.0},
		},
		{
			2,
			Floats([]float64{1.0, 2.0, 3.0, 4.0}),
			[]float64{math.NaN(), 3.0, 5.0, 7.0},
		},
	}
	for testnum, test := range tests {
		received := test.series.Rolling(test.window).Sum()
		if received.Len() != len(test.expected) {
			t.Errorf("Test:%v length mismatch", testnum)
			continue
		}
		for i, expVal := range test.expected {
			gotVal := received.Elem(i).Float()
			if !rollingFloatEq(expVal, gotVal, 1e-13) {
				t.Errorf("Test:%v[%d] expected %v got %v", testnum, i, expVal, gotVal)
			}
		}
	}
}

func TestSeries_RollingMin(t *testing.T) {
	tests := []struct {
		window   int
		series   Series
		expected []float64
	}{
		{
			3,
			Ints([]int{3, 1, 4, 1, 5, 9, 2, 6}),
			[]float64{math.NaN(), math.NaN(), 1.0, 1.0, 1.0, 1.0, 2.0, 2.0},
		},
		{
			2,
			Floats([]float64{5.0, 3.0, 8.0, 2.0}),
			[]float64{math.NaN(), 3.0, 3.0, 2.0},
		},
	}
	for testnum, test := range tests {
		received := test.series.Rolling(test.window).Min()
		if received.Len() != len(test.expected) {
			t.Errorf("Test:%v length mismatch", testnum)
			continue
		}
		for i, expVal := range test.expected {
			gotVal := received.Elem(i).Float()
			if !rollingFloatEq(expVal, gotVal, 1e-13) {
				t.Errorf("Test:%v[%d] expected %v got %v", testnum, i, expVal, gotVal)
			}
		}
	}
}

func TestSeries_RollingMax(t *testing.T) {
	tests := []struct {
		window   int
		series   Series
		expected []float64
	}{
		{
			3,
			Ints([]int{3, 1, 4, 1, 5, 9, 2, 6}),
			[]float64{math.NaN(), math.NaN(), 4.0, 4.0, 5.0, 9.0, 9.0, 9.0},
		},
		{
			2,
			Floats([]float64{5.0, 3.0, 8.0, 2.0}),
			[]float64{math.NaN(), 5.0, 8.0, 8.0},
		},
	}
	for testnum, test := range tests {
		received := test.series.Rolling(test.window).Max()
		if received.Len() != len(test.expected) {
			t.Errorf("Test:%v length mismatch", testnum)
			continue
		}
		for i, expVal := range test.expected {
			gotVal := received.Elem(i).Float()
			if !rollingFloatEq(expVal, gotVal, 1e-13) {
				t.Errorf("Test:%v[%d] expected %v got %v", testnum, i, expVal, gotVal)
			}
		}
	}
}

func TestSeries_RollingApply(t *testing.T) {
	// Apply: compute product of the window
	product := func(vals []float64) float64 {
		p := 1.0
		for _, v := range vals {
			p *= v
		}
		return p
	}
	s := Ints([]int{1, 2, 3, 4, 5})
	received := s.Rolling(3).Apply(product)
	expected := []float64{math.NaN(), math.NaN(), 6.0, 24.0, 60.0}
	if received.Len() != len(expected) {
		t.Fatalf("length mismatch: expected %d got %d", len(expected), received.Len())
	}
	for i, expVal := range expected {
		gotVal := received.Elem(i).Float()
		if !rollingFloatEq(expVal, gotVal, 1e-13) {
			t.Errorf("[%d] expected %v got %v", i, expVal, gotVal)
		}
	}
}

func TestSeries_RollingMinPeriods(t *testing.T) {
	// With minPeriods=1, first non-NaN window should produce a result
	s := Ints([]int{1, 2, 3, 4, 5})
	received := s.Rolling(3).MinPeriods(1).Mean()
	expected := []float64{1.0, 1.5, 2.0, 3.0, 4.0}
	if received.Len() != len(expected) {
		t.Fatalf("length mismatch: expected %d got %d", len(expected), received.Len())
	}
	for i, expVal := range expected {
		gotVal := received.Elem(i).Float()
		if !rollingFloatEq(expVal, gotVal, 1e-13) {
			t.Errorf("[%d] expected %v got %v", i, expVal, gotVal)
		}
	}
}
