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
