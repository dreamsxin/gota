package mat

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

func TestMatrix_Mul(t *testing.T) {
	table := []struct {
		a        series.Series
		b        series.Series
		expected series.Series
	}{
		{
			series.Ints([]int{0, 2, 1, 5, 9}),
			series.Ints([]int{0, 2, 1, 5, 9}),
			series.Ints([]int{0, 4, 1, 25, 81}),
		},
		// Float operands should produce Float result.
		{
			series.Floats([]float64{2.0, 3.0}),
			series.Floats([]float64{4.0, 5.0}),
			series.Floats([]float64{8.0, 15.0}),
		},
		// Mixed int/float → Float.
		{
			series.Ints([]int{2, 3}),
			series.Floats([]float64{1.5, 2.0}),
			series.Floats([]float64{3.0, 6.0}),
		},
	}
	for testnum, test := range table {
		a := test.a
		b := test.b

		expected := test.expected.Records()
		received := Mul(a, b).Records()
		t.Log(received)
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestMatrix_Sub(t *testing.T) {
	table := []struct {
		a        series.Series
		b        series.Series
		expected series.Series
	}{
		{
			series.Ints([]int{5, 8, 10}),
			series.Ints([]int{2, 3, 4}),
			series.Ints([]int{3, 5, 6}),
		},
		{
			series.Floats([]float64{5.5, 8.0}),
			series.Floats([]float64{2.5, 3.0}),
			series.Floats([]float64{3.0, 5.0}),
		},
		// Negative result.
		{
			series.Ints([]int{1, 2}),
			series.Ints([]int{3, 5}),
			series.Ints([]int{-2, -3}),
		},
	}
	for testnum, test := range table {
		expected := test.expected.Records()
		received := Sub(test.a, test.b).Records()
		t.Log(received)
		if !reflect.DeepEqual(expected, received) {
			t.Errorf("Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received)
		}
	}
}

func TestMatrix_Div(t *testing.T) {
	table := []struct {
		a        series.Series
		b        series.Series
		expected series.Series
	}{
		{
			series.Ints([]int{0, 2, 1, 5, 9}),
			series.Ints([]int{0, 1, 2, 2, 4}),
			series.Floats([]float64{0, 2, 0.5, 2.5, 2.25}),
		},
		// Float division.
		{
			series.Floats([]float64{9.0, 4.0}),
			series.Floats([]float64{3.0, 2.0}),
			series.Floats([]float64{3.0, 2.0}),
		},
		// Division by zero → 0.
		{
			series.Floats([]float64{5.0, 3.0}),
			series.Floats([]float64{0.0, 1.5}),
			series.Floats([]float64{0.0, 2.0}),
		},
	}
	for testnum, test := range table {
		a := test.a
		b := test.b

		expected := test.expected.Records()
		received := Div(a, b).Records()
		t.Log(received)
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestMatrix_Cal(t *testing.T) {
	table := []struct {
		a        series.Series
		b        series.Series
		expected series.Series
	}{
		{
			series.Ints([]int{0, 2, 1, 5}),
			series.Ints([]int{0, 1, 2, 2, 4}),
			series.Floats([]float64{0, 2, 0.5, 2.5, 0}),
		},
	}
	for testnum, test := range table {
		a := test.a
		b := test.b

		expected := test.expected.Records()
		received := Cal(a, b, TypeDiv, ModeZero).Records()
		t.Log(received)
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestMatrix_Add(t *testing.T) {
	table := []struct {
		a        series.Series
		b        series.Series
		expected series.Series
	}{
		// Equal length float.
		{
			series.Floats([]float64{0, 2, 1, 5, 9}),
			series.Floats([]float64{0, 1, 2, 2}),
			series.Floats([]float64{0, 3, 3, 7, 9}),
		},
		// b longer than a → extra b values appended.
		{
			series.Floats([]float64{1.0, 2.0}),
			series.Floats([]float64{10.0, 20.0, 30.0}),
			series.Floats([]float64{11.0, 22.0, 30.0}),
		},
		// Int add.
		{
			series.Ints([]int{1, 2, 3}),
			series.Ints([]int{4, 5, 6}),
			series.Ints([]int{5, 7, 9}),
		},
		// Int with a longer.
		{
			series.Ints([]int{1, 2, 3, 4}),
			series.Ints([]int{10, 20}),
			series.Ints([]int{11, 22, 3, 4}),
		},
	}
	for testnum, test := range table {
		a := test.a
		b := test.b

		expected := test.expected.Records()
		received := Add(a, b).Records()
		t.Log(received)
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}

func TestMatrix_Cal_ModeOne(t *testing.T) {
	// ModeOne: short series padded with 1 before operation.
	a := series.Ints([]int{2, 3, 4})
	b := series.Ints([]int{10, 10}) // shorter

	// Cal with ModeOne pads b to len(a) with value 1.
	got := Cal(a, b, TypeMul, ModeOne)
	expected := series.Ints([]int{20, 30, 4}).Records()
	if !reflect.DeepEqual(expected, got.Records()) {
		t.Errorf("Cal ModeOne Mul: expected %v got %v", expected, got.Records())
	}
}

