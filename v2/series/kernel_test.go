package series_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// Golden tables for the Milestone 2 comparison kernels. Missing values never
// satisfy a comparison (either side), matching the historical Element-based
// semantics the kernel replaces.

func TestCompareMask_Scalar_Golden(t *testing.T) {
	ints := series.Ints([]interface{}{1, 2, nil, 4})
	floats := series.Floats([]float64{1.5, 2.5, math.NaN(), 4.5})
	strs := series.Strings([]interface{}{"a", "b", nil, "d"})
	bools := series.Bools([]interface{}{true, false, nil, true})

	tests := []struct {
		name       string
		series     series.Series
		comparator series.Comparator
		comparando interface{}
		want       []int // selected rows, ascending
	}{
		{"int eq", ints, series.Eq, 2, []int{1}},
		{"int neq", ints, series.Neq, 2, []int{0, 3}},
		{"int gt", ints, series.Greater, 1, []int{1, 3}},
		{"int gteq", ints, series.GreaterEq, 2, []int{1, 3}},
		{"int lt", ints, series.Less, 4, []int{0, 1}},
		{"int lteq", ints, series.LessEq, 2, []int{0, 1}},
		{"int eq missing operand", ints, series.Eq, "NaN", []int{}},
		{"float gt", floats, series.Greater, 2.0, []int{1, 3}},
		{"float eq nan operand", floats, series.Eq, math.NaN(), []int{}},
		{"string eq", strs, series.Eq, "b", []int{1}},
		{"string lt", strs, series.Less, "c", []int{0, 1}},
		{"bool eq true", bools, series.Eq, true, []int{0, 3}},
		{"bool lt", bools, series.Less, true, []int{1}},
		{"int in", ints, series.In, []int{2, 4, 9}, []int{1, 3}},
		{"int out", ints, series.Out, []int{2, 4, 9}, []int{0}},
		{"string in", strs, series.In, []string{"a", "d"}, []int{0, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mask, err := tc.series.CompareMask(tc.comparator, tc.comparando)
			if err != nil {
				t.Fatalf("CompareMask: %v", err)
			}
			got := mask.Rows()
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("rows: got %v want %v", got, tc.want)
			}
		})
	}
}

func TestCompareMask_MultiElement_Golden(t *testing.T) {
	a := series.Ints([]interface{}{1, 2, nil, 4})
	b := series.Ints([]interface{}{1, 5, 3, nil})
	mask, err := a.CompareMask(series.Less, b)
	if err != nil {
		t.Fatalf("CompareMask: %v", err)
	}
	// row 0: 1<1 false; row 1: 2<5 true; row 2 missing; row 3 operand missing.
	if got, want := mask.Rows(), []int{1}; !reflect.DeepEqual(got, want) {
		t.Errorf("rows: got %v want %v", got, want)
	}
}

func TestCompareMask_CompFunc(t *testing.T) {
	s := series.Strings([]string{"aa1", "bb", "aa2"})
	mask, err := s.CompareMask(series.CompFunc, func(v series.Series, i int) bool {
		val, _ := v.Val(i).(string)
		return len(val) >= 2 && val[:2] == "aa"
	})
	if err != nil {
		t.Fatalf("CompareMask: %v", err)
	}
	if got, want := mask.Rows(), []int{0, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("rows: got %v want %v", got, want)
	}
}

func TestCompareMask_CombineWordwise(t *testing.T) {
	s := series.Ints([]int{1, 2, 3, 4, 5})
	gt2, err := s.CompareMask(series.Greater, 2)
	if err != nil {
		t.Fatal(err)
	}
	lt5, err := s.CompareMask(series.Less, 5)
	if err != nil {
		t.Fatal(err)
	}
	and := series.NewMask(s.Len())
	and.OrInto(gt2)
	and.AndInto(lt5)
	if got, want := and.Rows(), []int{2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("and rows: got %v want %v", got, want)
	}
	or := series.NewMask(s.Len())
	or.OrInto(gt2)
	or.OrInto(lt5)
	if got, want := or.Rows(), []int{0, 1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("or rows: got %v want %v", got, want)
	}
}

func TestCompare_KeepsBoolSeriesSignature(t *testing.T) {
	s := series.Ints([]int{1, 2, 3})
	got := s.Compare(series.Greater, 1)
	if got.Type() != series.Bool {
		t.Fatalf("type: got %v want Bool", got.Type())
	}
	if got.Records()[0] != "false" || got.Records()[1] != "true" {
		t.Errorf("records: got %v", got.Records())
	}
}

func TestMapFloat64_Golden(t *testing.T) {
	s := series.Floats([]float64{1, math.NaN(), 3})
	got := series.MapFloat64(s, func(v float64) float64 { return v * 2 })
	if got.Records()[0] != "2.000000" || got.Records()[2] != "6.000000" {
		t.Errorf("mapped: got %v", got.Records())
	}
	if !got.IsNA(1) {
		t.Errorf("missing must stay missing, got %v", got.Records())
	}
	// Non-Float input converts through FloatAt.
	fromInt := series.MapFloat64(series.Ints([]int{1, 2}), func(v float64) float64 { return v + 0.5 })
	if fromInt.Type() != series.Float || fromInt.Records()[0] != "1.500000" {
		t.Errorf("cross-type map: got %v", fromInt.Records())
	}
}

func TestMapInt64_Golden(t *testing.T) {
	s := series.Ints([]interface{}{1, nil, 3})
	got := series.MapInt64(s, func(v int64) int64 { return v * 10 })
	if got.Records()[0] != "10" || got.Records()[2] != "30" || !got.IsNA(1) {
		t.Errorf("mapped: got %v", got.Records())
	}
}

func TestGatherRows_NegativeBecomesMissing(t *testing.T) {
	s := series.Ints([]int{7, 8, 9})
	got := s.GatherRows([]int{2, -1, 0})
	if got.Records()[0] != "9" || got.Records()[2] != "7" || !got.IsNA(1) {
		t.Errorf("gather: got %v", got.Records())
	}
}

func TestCombineRows_Golden(t *testing.T) {
	a := series.Ints([]int{1, 2})
	b := series.Ints([]int{10, 20})
	got := series.CombineRows(a, b, []int{0, -1, 1}, []int{-1, 1, -1})
	want := []string{"1", "20", "2"}
	if !reflect.DeepEqual(got.Records(), want) {
		t.Errorf("combine: got %v want %v", got.Records(), want)
	}
}

func TestRowLess_RowGreater(t *testing.T) {
	s := series.Ints([]int{3, 1, 2})
	if !series.RowLess(s, 1, 0) || series.RowLess(s, 0, 1) {
		t.Errorf("RowLess wrong")
	}
	if !series.RowGreater(s, 0, 2) || series.RowGreater(s, 2, 0) {
		t.Errorf("RowGreater wrong")
	}
}

func BenchmarkSeries_Compare(b *testing.B) {
	s := series.Ints(generateInts(100000))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mask, err := s.CompareMask(series.Greater, 50)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += float64(len(mask.Rows()))
	}
}
