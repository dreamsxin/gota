package series_test

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// TestInt_ZeroVersusMissing is the Milestone 4 semantic anchor: with
// validity-bitmap storage, an Int column distinguishes the value 0 from a
// missing value (v1 conflated them through NaN sentinels).
func TestInt_ZeroVersusMissing(t *testing.T) {
	s := series.Ints([]interface{}{0, nil, 0, 7})

	if s.IsNA(0) {
		t.Fatal("row 0 holds the value 0, not missing")
	}
	if !s.IsNA(1) {
		t.Fatal("row 1 must be missing")
	}
	if got, want := s.Val(0), 0; got != want {
		t.Fatalf("Val(0) = %v, want %v", got, want)
	}
	if got := s.Val(1); got != nil {
		t.Fatalf("Val(1) = %v, want nil", got)
	}
	if got, want := s.Record(0), "0"; got != want {
		t.Fatalf("Record(0) = %q, want %q", got, want)
	}
	if got, want := s.Record(1), "NaN"; got != want {
		t.Fatalf("Record(1) = %q, want %q", got, want)
	}
	// Equality against 0 selects exactly the zero-valued rows, never the
	// missing one.
	mask, err := s.CompareMask(series.Eq, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := mask.Rows(), []int{0, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Eq 0 rows = %v, want %v", got, want)
	}
	// NUnique counts {0, 7} only.
	if got := s.NUnique(); got != 2 {
		t.Fatalf("NUnique = %d, want 2", got)
	}
}
