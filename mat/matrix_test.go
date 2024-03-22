package mat

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

func TestMatrix_Dot(t *testing.T) {
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
		received := Dot(a, b).Records()
		t.Log(received)
		if !reflect.DeepEqual(expected, received) {
			t.Errorf(
				"Test:%v\nExpected:\n%v\nReceived:\n%v",
				testnum, expected, received,
			)
		}
	}
}
