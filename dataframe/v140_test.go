package dataframe

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

func TestDataFrameRolling(t *testing.T) {
	df := New(
		series.New([]string{"a", "b", "c"}, series.String, "key"),
		series.New([]int{1, 2, 3}, series.Int, "num"),
		series.New([]float64{1, 2, 4}, series.Float, "val"),
	)

	got := df.Rolling(2).Mean()
	if got.Err != nil {
		t.Fatalf("Rolling.Mean: %v", got.Err)
	}
	if !reflect.DeepEqual(got.Names(), df.Names()) {
		t.Fatalf("names = %v, want %v", got.Names(), df.Names())
	}
	// String column carried through unchanged; numeric columns rolled.
	if key := got.Col("key").Records(); !reflect.DeepEqual(key, []string{"a", "b", "c"}) {
		t.Fatalf("key = %v", key)
	}
	num := got.Col("num").Records()
	if num[0] != "NaN" || num[2] != "2.500000" {
		t.Fatalf("rolling mean num = %v", num)
	}

	sel := df.Rolling(2).Sum("val")
	if got := sel.Col("num").Records(); !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
		t.Fatalf("unselected column should pass through: %v", got)
	}
	if got := sel.Col("val").Records(); got[2] != "6.000000" {
		t.Fatalf("rolling sum val = %v", got)
	}

	if err := df.Rolling(0).Mean(); err.Err == nil {
		t.Fatal("window < 1 should error")
	}
	bad := df.Rolling(2).Mean("missing")
	if bad.Err == nil || !errors.Is(bad.Err, ErrColumnNotFound) {
		t.Fatalf("missing column: %v", bad.Err)
	}
	nonNum := df.Rolling(2).Mean("key")
	if nonNum.Err == nil || !errors.Is(nonNum.Err, ErrTypeMismatch) {
		t.Fatalf("non-numeric column: %v", nonNum.Err)
	}
}

func TestDataFrameEWM(t *testing.T) {
	df := New(
		series.New([]string{"a", "b", "c"}, series.String, "key"),
		series.New([]float64{1, 2, 3}, series.Float, "price"),
	)

	got := df.EWM(3).Mean()
	if got.Err != nil {
		t.Fatalf("EWM.Mean: %v", got.Err)
	}
	if !reflect.DeepEqual(got.Names(), df.Names()) {
		t.Fatalf("names = %v", got.Names())
	}
	// First EWM value equals the first observation.
	price := got.Col("price").Records()
	if price[0] != "1.000000" {
		t.Fatalf("ewm mean = %v", price)
	}

	alpha := df.EWMAlpha(0.5).Mean("price")
	if alpha.Col("key").Records()[0] != "a" {
		t.Fatal("EWMAlpha should carry other columns through")
	}
	std := df.EWM(3).Std("price")
	if std.Err != nil {
		t.Fatalf("EWM.Std: %v", std.Err)
	}
	bad := df.EWM(3).Mean("missing")
	if bad.Err == nil || !errors.Is(bad.Err, ErrColumnNotFound) {
		t.Fatalf("missing column: %v", bad.Err)
	}
}

func TestConcatVariadic(t *testing.T) {
	a := New(series.New([]int{1, 2}, series.Int, "x"))
	b := New(series.New([]int{3}, series.Int, "x"))
	c := New(series.New([]int{4, 5}, series.Int, "x"))

	got := Concat(a, b, c)
	if got.Err != nil {
		t.Fatalf("Concat: %v", got.Err)
	}
	if want := []string{"1", "2", "3", "4", "5"}; !reflect.DeepEqual(got.Col("x").Records(), want) {
		t.Fatalf("Concat records = %v", got)
	}

	// Unmatched columns fill with NaN, matching DataFrame.Concat.
	d := New(series.New([]int{9}, series.Int, "y"))
	got = Concat(a, d)
	if got.Col("y").Records()[0] != "NaN" || got.Col("x").Records()[2] != "NaN" {
		t.Fatalf("Concat unmatched = %v", got.Records())
	}

	// Inputs are not modified by the concatenation.
	if a.Nrow() != 2 {
		t.Fatalf("Concat mutated input: %d rows", a.Nrow())
	}

	if got := Concat(); got.Err == nil {
		t.Fatal("Concat with no arguments should error")
	}

	single := Concat(a)
	if single.Err != nil || !reflect.DeepEqual(single.Records(), a.Records()) {
		t.Fatalf("Concat of one frame = %v, err %v", single.Records(), single.Err)
	}
}

func TestConcatColumnsVariadic(t *testing.T) {
	a := New(series.New([]int{1, 2}, series.Int, "x"))
	b := New(series.New([]string{"p", "q"}, series.String, "y"))
	c := New(series.New([]float64{1.5, 2.5}, series.Float, "z"))

	got := ConcatColumns(a, b, c)
	if got.Err != nil {
		t.Fatalf("ConcatColumns: %v", got.Err)
	}
	if want := []string{"x", "y", "z"}; !reflect.DeepEqual(got.Names(), want) {
		t.Fatalf("names = %v, want %v", got.Names(), want)
	}

	// Row mismatch propagates an error.
	short := New(series.New([]int{1}, series.Int, "w"))
	if got := ConcatColumns(a, short); got.Err == nil {
		t.Fatal("row mismatch should error")
	}
	if got := ConcatColumns(); got.Err == nil {
		t.Fatal("ConcatColumns with no arguments should error")
	}
}

func TestFillNaNAliases(t *testing.T) {
	df := New(series.New([]interface{}{1.0, nil, 3.0}, series.Float, "x"))

	alias := df.FillNaNStrategy(NAFillForward)
	if alias.Err != nil {
		t.Fatalf("FillNaNStrategy: %v", alias.Err)
	}
	if got := alias.Col("x").Records(); got[1] != "1.000000" {
		t.Fatalf("FillNaNStrategy = %v", got)
	}

	aliasLimit := df.Copy().FillNaNStrategyLimit(NAFillBackward, 1)
	if aliasLimit.Err != nil {
		t.Fatalf("FillNaNStrategyLimit: %v", aliasLimit.Err)
	}
}
