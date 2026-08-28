package series_test

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// Golden tests for the Milestone 3 dictionary-encoded column storage.
// Dictionary columns must behave exactly like String columns, with the
// dictionary identity visible only through DType().

func testDictSeries(t *testing.T) (series.Series, series.Series) {
	t.Helper()
	vals := []interface{}{"US", "UK", nil, "DE", "UK"}
	str := series.Strings(vals)
	cat, err := series.CategoricalFromSeries(str)
	if err != nil {
		t.Fatal(err)
	}
	return str, cat.ToDictionarySeries()
}

func TestDictionary_MatchesStringSemantics(t *testing.T) {
	str, dict := testDictSeries(t)

	if got, want := dict.Records(), str.Records(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Records: got %v want %v", got, want)
	}
	if got, want := dict.IsNaN(), str.IsNaN(); !reflect.DeepEqual(got, want) {
		t.Fatalf("IsNaN: got %v want %v", got, want)
	}
	if got, want := dict.Val(0), "US"; got != want {
		t.Fatalf("Val(0) = %v want %v", got, want)
	}
	if got := dict.Val(2); got != nil {
		t.Fatalf("Val(2) = %v want nil", got)
	}

	// Comparison masks agree with the String column.
	for _, cmp := range []struct {
		op  series.Comparator
		val interface{}
	}{
		{series.Eq, "UK"},
		{series.Neq, "UK"},
		{series.In, []string{"US", "DE"}},
	} {
		mStr, err := str.CompareMask(cmp.op, cmp.val)
		if err != nil {
			t.Fatal(err)
		}
		mDict, err := dict.CompareMask(cmp.op, cmp.val)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(mDict.Rows(), mStr.Rows()) {
			t.Errorf("CompareMask(%v %v): dict %v != string %v", cmp.op, cmp.val, mDict.Rows(), mStr.Rows())
		}
	}
}

func TestDictionary_GatherAndShift(t *testing.T) {
	_, dict := testDictSeries(t)

	sub := dict.Subset([]int{4, 2, 0})
	if got, want := sub.Records(), []string{"UK", "NaN", "US"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Subset: got %v want %v", got, want)
	}
	// Gathered dictionary keeps its DType.
	if sub.DType().Physical() != series.PhysDictionary {
		t.Fatalf("Subset DType = %v", sub.DType())
	}

	shifted := dict.Shift(1)
	if got, want := shifted.Records(), []string{"NaN", "US", "UK", "NaN", "DE"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Shift(1): got %v want %v", got, want)
	}
}

func TestDictionary_FillAndStringOps(t *testing.T) {
	_, dict := testDictSeries(t)

	filled := dict.FillNaNForward()
	if got, want := filled.Records(), []string{"US", "UK", "UK", "DE", "UK"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FillNaNForward: got %v want %v", got, want)
	}

	// String operations materialize into plain String columns.
	up := dict.Upper()
	if up.DType().Physical() != series.PhysUtf8 {
		t.Fatalf("Upper DType = %v, want utf8", up.DType())
	}
	if got, want := up.Records(), []string{"US", "UK", "NaN", "DE", "UK"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Upper: got %v want %v", got, want)
	}
}

func TestDictionary_FactorizeFastPath(t *testing.T) {
	_, dict := testDictSeries(t)
	labels, codes, counts, ok := dict.Factorize()
	if !ok {
		t.Fatal("dictionary columns must factorize")
	}
	// First-seen order: US, UK, <nil>, DE.
	if want := []string{"US", "UK", "<nil>", "DE"}; !reflect.DeepEqual(labels, want) {
		t.Fatalf("labels = %v want %v", labels, want)
	}
	if want := []int{0, 1, 2, 3, 1}; !reflect.DeepEqual(codes, want) {
		t.Fatalf("codes = %v want %v", codes, want)
	}
	if want := []int{1, 2, 1, 1}; !reflect.DeepEqual(counts, want) {
		t.Fatalf("counts = %v want %v", counts, want)
	}
}

func TestDictionary_CategoricalRoundTrip(t *testing.T) {
	vals := []interface{}{"a", "b", nil, "a"}
	str := series.Strings(vals)
	cat, err := series.CategoricalFromSeries(str)
	if err != nil {
		t.Fatal(err)
	}
	dict := cat.ToDictionarySeries()
	back, err := series.CategoricalFromSeries(dict)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := back.ToSeries().Records(), str.Records(); !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip: got %v want %v", got, want)
	}
}

func TestDictionary_AppendEncodesNewCategories(t *testing.T) {
	dict := series.NewCategorical([]string{"a", "b"}, "c").ToDictionarySeries()
	dict.Append(series.Strings([]string{"b", "z"}))
	if got, want := dict.Records(), []string{"a", "b", "b", "z"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Records: got %v want %v", got, want)
	}
	cats, _ := series.DictionaryCategories(dict.DType())
	if want := []string{"a", "b", "z"}; !reflect.DeepEqual(cats, want) {
		t.Fatalf("categories = %v want %v", cats, want)
	}
}
