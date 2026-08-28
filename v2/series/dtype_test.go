package series_test

import (
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// Golden tests for the Milestone 3 DType system (RFC §4).

func TestDType_PhysicalSingletons(t *testing.T) {
	tests := []struct {
		seriesType series.Type
		dtype      series.DType
		phys       series.PhysicalType
	}{
		{series.Int, series.DTInt64, series.PhysInt64},
		{series.Float, series.DTFloat64, series.PhysFloat64},
		{series.String, series.DTUtf8, series.PhysUtf8},
		{series.Bool, series.DTBool, series.PhysBool},
		{series.Time, series.DTTime, series.PhysTime},
	}
	for _, tc := range tests {
		if got := series.DTypeOf(tc.seriesType); got != tc.dtype {
			t.Errorf("DTypeOf(%v) = %v, want %v", tc.seriesType, got, tc.dtype)
		}
		if tc.dtype.Physical() != tc.phys {
			t.Errorf("Physical() = %v, want %v", tc.dtype.Physical(), tc.phys)
		}
		if tc.dtype.Metadata() != nil {
			t.Errorf("physical DType metadata must be nil, got %v", tc.dtype.Metadata())
		}
	}
}

func TestDType_SeriesDType(t *testing.T) {
	if got := series.Ints([]int{1, 2}).DType(); got != series.DTInt64 {
		t.Errorf("Int series DType = %v", got)
	}
	if got := series.Strings([]string{"a"}).DType(); got != series.DTUtf8 {
		t.Errorf("String series DType = %v", got)
	}
}

func TestDType_Dictionary(t *testing.T) {
	dt := series.NewDictionaryDType([]string{"DE", "UK", "US"}, true)
	if dt.Physical() != series.PhysDictionary {
		t.Fatalf("Physical = %v, want dictionary", dt.Physical())
	}
	meta := dt.Metadata()
	if meta["cardinality"] != "3" || meta["ordered"] != "true" {
		t.Fatalf("metadata = %v", meta)
	}
	cats, ok := series.DictionaryCategories(dt)
	if !ok || len(cats) != 3 || cats[0] != "DE" {
		t.Fatalf("categories = %v, %v", cats, ok)
	}
	// Non-dictionary DTypes report no categories.
	if _, ok := series.DictionaryCategories(series.DTUtf8); ok {
		t.Fatal("physical DType must not expose categories")
	}
}

func TestDType_DictionarySeries(t *testing.T) {
	cat := series.NewCategorical([]string{"US", "UK", "US", "", "DE"}, "country")
	s := cat.ToDictionarySeries()
	if s.Type() != series.String {
		t.Fatalf("dictionary series Type = %v, want string", s.Type())
	}
	if s.DType().Physical() != series.PhysDictionary {
		t.Fatalf("dictionary series DType = %v", s.DType())
	}
	cats, ok := series.DictionaryCategories(s.DType())
	if !ok || len(cats) != 3 {
		t.Fatalf("series categories = %v, %v", cats, ok)
	}
}
