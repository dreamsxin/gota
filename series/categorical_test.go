package series

import (
	"reflect"
	"testing"
)

func TestCategorical_Basic(t *testing.T) {
	cat := NewCategorical([]string{"US", "UK", "US", "DE", "UK"}, "country")

	if cat.Len() != 5 {
		t.Fatalf("Len: got %d want 5", cat.Len())
	}
	if cat.NCategories() != 3 {
		t.Fatalf("NCategories: got %d want 3", cat.NCategories())
	}
	// Dictionary is sorted.
	if !reflect.DeepEqual(cat.Categories(), []string{"DE", "UK", "US"}) {
		t.Errorf("Categories: got %v", cat.Categories())
	}
	if cat.Get(0) != "US" {
		t.Errorf("Get(0): got %q want US", cat.Get(0))
	}
	if cat.Get(1) != "UK" {
		t.Errorf("Get(1): got %q want UK", cat.Get(1))
	}
}

func TestCategorical_NaN(t *testing.T) {
	cat := NewCategorical([]string{"a", "", "b", ""}, "v")
	if !cat.IsNA(1) || !cat.IsNA(3) {
		t.Error("IsNA: expected true at positions 1 and 3")
	}
	if cat.IsNA(0) || cat.IsNA(2) {
		t.Error("IsNA: expected false at positions 0 and 2")
	}
}

func TestCategorical_ToSeries(t *testing.T) {
	cat := NewCategorical([]string{"x", "y", "x"}, "col")
	s := cat.ToSeries()
	if s.Err != nil {
		t.Fatal(s.Err)
	}
	if s.Len() != 3 {
		t.Fatalf("ToSeries len: got %d want 3", s.Len())
	}
	if s.Elem(0).String() != "x" {
		t.Errorf("ToSeries[0]: got %q want x", s.Elem(0).String())
	}
}

func TestCategorical_FromSeries(t *testing.T) {
	s := Strings([]string{"a", "b", "a", "c"})
	s.Name = "label"
	cat, err := CategoricalFromSeries(s)
	if err != nil {
		t.Fatal(err)
	}
	if cat.NCategories() != 3 {
		t.Errorf("FromSeries NCategories: got %d want 3", cat.NCategories())
	}
	if cat.Name != "label" {
		t.Errorf("FromSeries Name: got %q want label", cat.Name)
	}
}

func TestCategorical_FromSeries_NonString(t *testing.T) {
	s := Ints([]int{1, 2, 3})
	_, err := CategoricalFromSeries(s)
	if err == nil {
		t.Error("CategoricalFromSeries non-string: expected error")
	}
}

func TestCategorical_ValueCounts(t *testing.T) {
	cat := NewCategorical([]string{"a", "b", "a", "a", "b"}, "v")
	vc := cat.ValueCounts()
	if vc["a"] != 3 {
		t.Errorf("ValueCounts a: got %d want 3", vc["a"])
	}
	if vc["b"] != 2 {
		t.Errorf("ValueCounts b: got %d want 2", vc["b"])
	}
}

func TestCategorical_SetValue(t *testing.T) {
	cat := NewCategorical([]string{"a", "b"}, "v")
	if err := cat.SetValue(0, "b"); err != nil {
		t.Fatal(err)
	}
	if cat.Get(0) != "b" {
		t.Errorf("SetValue: got %q want b", cat.Get(0))
	}
	// Unknown category.
	if err := cat.SetValue(0, "z"); err == nil {
		t.Error("SetValue unknown: expected error")
	}
	// AddCategory then set.
	cat.AddCategory("z")
	if err := cat.SetValue(0, "z"); err != nil {
		t.Fatalf("SetValue after AddCategory: %v", err)
	}
	if cat.Get(0) != "z" {
		t.Errorf("SetValue after AddCategory: got %q want z", cat.Get(0))
	}
}

func TestCategorical_Filter(t *testing.T) {
	cat := NewCategorical([]string{"a", "b", "a", "c"}, "v")
	mask := []bool{true, false, true, false}
	filtered, err := cat.Filter(mask)
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Len() != 2 {
		t.Fatalf("Filter len: got %d want 2", filtered.Len())
	}
	if filtered.Get(0) != "a" || filtered.Get(1) != "a" {
		t.Errorf("Filter values: got [%s %s] want [a a]", filtered.Get(0), filtered.Get(1))
	}
}

func TestCategorical_MemoryBytes(t *testing.T) {
	// 1000 rows, 3 categories → codes use 4000 bytes + small dict
	vals := make([]string, 1000)
	for i := range vals {
		switch i % 3 {
		case 0:
			vals[i] = "US"
		case 1:
			vals[i] = "UK"
		case 2:
			vals[i] = "DE"
		}
	}
	cat := NewCategorical(vals, "country")
	mem := cat.MemoryBytes()
	// Should be much less than 1000 * avg_string_len.
	if mem > 10000 {
		t.Errorf("MemoryBytes: got %d, expected < 10000 for 1000 rows with 3 categories", mem)
	}
}

func TestCategorical_RoundTrip(t *testing.T) {
	orig := Strings([]string{"x", "y", "x", "z", "y"})
	orig.Name = "col"
	cat, err := CategoricalFromSeries(orig)
	if err != nil {
		t.Fatal(err)
	}
	back := cat.ToSeries()
	if !reflect.DeepEqual(orig.Records(), back.Records()) {
		t.Errorf("RoundTrip: got %v want %v", back.Records(), orig.Records())
	}
}
