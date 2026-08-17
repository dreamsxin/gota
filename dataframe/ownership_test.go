package dataframe

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// These tests pin the ownership contract documented in README ("Chaining
// operations"): most DataFrame methods return a new DataFrame; Set, FillNaN,
// and SetNames mutate shared column storage; Series mutation methods and
// NewNoCopy alias their inputs. Copy protects in every case.

var ownershipBase = New(
	series.New([]string{"a", "b", "a", "c"}, series.String, "key"),
	series.New([]int{4, 5, 2, 7}, series.Int, "num"),
)

func TestOwnershipTransformationsReturnNew(t *testing.T) {
	transforms := map[string]func(DataFrame) DataFrame{
		"Subset":   func(d DataFrame) DataFrame { return d.Subset([]int{0, 2}) },
		"SliceRow": func(d DataFrame) DataFrame { return d.SliceRow(1, 3) },
		"Select":   func(d DataFrame) DataFrame { return d.Select([]string{"key"}) },
		"Drop":     func(d DataFrame) DataFrame { return d.Drop([]string{"num"}) },
		"Filter": func(d DataFrame) DataFrame {
			return d.Filter(F{Colname: "key", Comparator: series.Eq, Comparando: "a"})
		},
		"Arrange":  func(d DataFrame) DataFrame { return d.Arrange(Sort("num")) },
		"Mutate":   func(d DataFrame) DataFrame { return d.Mutate(series.New([]int{1, 2, 3, 4}, series.Int, "num")) },
		"Head":     func(d DataFrame) DataFrame { return d.Head(2) },
		"Tail":     func(d DataFrame) DataFrame { return d.Tail(2) },
		"Diff":     func(d DataFrame) DataFrame { return d.Diff(1) },
		"Shift":    func(d DataFrame) DataFrame { return d.Shift(1) },
		"Rename":   func(d DataFrame) DataFrame { return d.Rename("k", "key") },
		"Describe": func(d DataFrame) DataFrame { return d.Describe() },
		"DropNA":   func(d DataFrame) DataFrame { return d.DropNA(NAHowAny) },
		"Copy":     func(d DataFrame) DataFrame { return d.Copy() },
		"Capply": func(d DataFrame) DataFrame {
			return d.Capply(func(s series.Series) series.Series { return series.Floats(s.Mean()) })
		},
		"NLargest":    func(d DataFrame) DataFrame { return d.NLargest(2, "num") },
		"ValueCounts": func(d DataFrame) DataFrame { return d.ValueCounts("key", false, false) },
	}

	wantRecords := ownershipBase.Records()
	wantNames := ownershipBase.Names()

	for name, fn := range transforms {
		t.Run(name, func(t *testing.T) {
			out := fn(ownershipBase)
			if out.Err != nil {
				t.Fatalf("%s: %v", name, out.Err)
			}
			if got := ownershipBase.Records(); !reflect.DeepEqual(got, wantRecords) {
				t.Fatalf("%s mutated source records: %v", name, got)
			}
			if got := ownershipBase.Names(); !reflect.DeepEqual(got, wantNames) {
				t.Fatalf("%s mutated source names: %v", name, got)
			}
		})
	}
}

func TestOwnershipSharedColumnMutation(t *testing.T) {
	// A struct copy shares column storage: Set on the copy is visible
	// through the original.
	df := ownershipBase.Copy()
	alias := df
	set := df.Set([]int{0}, LoadRecords([][]string{
		{"key", "num"},
		{"z", "99"},
	}))
	if set.Err != nil {
		t.Fatalf("Set: %v", set.Err)
	}
	if got := alias.Records()[1]; !reflect.DeepEqual(got, []string{"z", "99"}) {
		t.Fatalf("Set should mutate shared storage, row 0 = %v", got)
	}

	// Copy before Set protects the original.
	orig := ownershipBase.Copy()
	protected := orig.Copy().Set([]int{0}, LoadRecords([][]string{
		{"key", "num"},
		{"z", "99"},
	}))
	if protected.Err != nil {
		t.Fatalf("Set: %v", protected.Err)
	}
	if !reflect.DeepEqual(orig.Records(), ownershipBase.Records()) {
		t.Fatalf("Set on a Copy mutated the original: %v", orig.Records())
	}
}

func TestOwnershipFillNaBMutatesSharedStorage(t *testing.T) {
	df := New(
		series.New([]interface{}{1.0, nil, 3.0}, series.Float, "x"),
	)
	alias := df
	filled := df.FillNaN("x", series.Floats(9))
	if filled.Err != nil {
		t.Fatalf("FillNaN: %v", filled.Err)
	}
	if got := alias.Records(); !reflect.DeepEqual(got, [][]string{{"x"}, {"1.000000"}, {"9.000000"}, {"3.000000"}}) {
		t.Fatalf("FillNaN should mutate shared storage, got %v", got)
	}
}

func TestOwnershipSetNamesMutatesSharedStorage(t *testing.T) {
	df := ownershipBase.Copy()
	alias := df
	if err := df.SetNames("k", "n"); err != nil {
		t.Fatalf("SetNames: %v", err)
	}
	if got := alias.Names(); !reflect.DeepEqual(got, []string{"k", "n"}) {
		t.Fatalf("SetNames should mutate shared storage, got %v", got)
	}
}

func TestOwnershipSeriesMutation(t *testing.T) {
	s := series.New([]int{1, 2}, series.Int, "x")
	s.Append(3) // pointer receiver mutates in place
	if got := s.Records(); !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
		t.Fatalf("Append mutated self: %v", got)
	}

	protected := s.Copy()
	protected.Append(4)
	if got := s.Records(); !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
		t.Fatalf("Append on a Copy leaked into the original: %v", got)
	}
}

func TestOwnershipNewNoCopyAliases(t *testing.T) {
	s := series.New([]int{1, 2}, series.Int, "x")

	copyDF := New(s)
	aliasDF := NewNoCopy(s)

	// Series.Set writes through the shared element storage.
	mutated := s.Set([]int{0}, series.New([]int{9}, series.Int, "v"))
	if mutated.Err != nil {
		t.Fatalf("Set: %v", mutated.Err)
	}

	if got := copyDF.Records(); !reflect.DeepEqual(got, [][]string{{"x"}, {"1"}, {"2"}}) {
		t.Fatalf("New copies its Series, but Set leaked into the copy: %v", got)
	}
	if got := aliasDF.Records(); !reflect.DeepEqual(got, [][]string{{"x"}, {"9"}, {"2"}}) {
		t.Fatalf("NewNoCopy must alias its Series storage, records = %v", got)
	}
}

func TestOwnershipStickyError(t *testing.T) {
	a := ownershipBase.Select([]string{"missing"})
	if a.Err == nil {
		t.Fatal("Select with unknown column should set Err")
	}

	// Subsequent chain operations are no-ops that keep the error.
	for name, fn := range map[string]func(DataFrame) DataFrame{
		"Filter": func(d DataFrame) DataFrame {
			return d.Filter(F{Colname: "key", Comparator: series.Eq, Comparando: "a"})
		},
		"Select":  func(d DataFrame) DataFrame { return d.Select([]string{"key"}) },
		"Head":    func(d DataFrame) DataFrame { return d.Head(2) },
		"Arrange": func(d DataFrame) DataFrame { return d.Arrange(Sort("num")) },
		"Subset":  func(d DataFrame) DataFrame { return d.Subset([]int{0}) },
	} {
		out := fn(a)
		if out.Err == nil {
			t.Fatalf("%s after error should keep Err", name)
		}
		if out.Nrow() != 0 || out.Ncol() != 0 {
			nrows, ncols := out.Dims()
			t.Fatalf("%s after error should be a no-op, dims = (%d, %d)", name, nrows, ncols)
		}
	}

	// The source is unaffected by the failed chain.
	if !reflect.DeepEqual(ownershipBase.Records(), New(
		series.New([]string{"a", "b", "a", "c"}, series.String, "key"),
		series.New([]int{4, 5, 2, 7}, series.Int, "num"),
	).Records()) {
		t.Fatal("failed chain mutated the source")
	}
}
