package dataframe

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// Index – basic operations
// -----------------------------------------------------------------------

func TestIndex_NewAndBasics(t *testing.T) {
	labels := []string{"a", "b", "c", "d"}
	idx := NewIndex(labels)

	if idx.Len() != 4 {
		t.Errorf("Len: got %d want 4", idx.Len())
	}
	if idx.Label(0) != "a" {
		t.Errorf("Label(0): got %q want %q", idx.Label(0), "a")
	}
	if idx.Label(3) != "d" {
		t.Errorf("Label(3): got %q want %q", idx.Label(3), "d")
	}
	// Out-of-bounds returns "".
	if idx.Label(-1) != "" {
		t.Errorf("Label(-1): expected empty string")
	}
	if idx.Label(4) != "" {
		t.Errorf("Label(4): expected empty string")
	}
	if !reflect.DeepEqual(idx.Labels(), labels) {
		t.Errorf("Labels(): got %v want %v", idx.Labels(), labels)
	}
}

func TestIndex_Contains(t *testing.T) {
	idx := NewIndex([]string{"x", "y", "z"})
	if !idx.Contains("x") {
		t.Error("Contains('x'): expected true")
	}
	if idx.Contains("w") {
		t.Error("Contains('w'): expected false")
	}
}

func TestIndex_Get(t *testing.T) {
	idx := NewIndex([]string{"a", "b", "a", "c"})
	positions := idx.Get("a")
	if !reflect.DeepEqual(positions, []int{0, 2}) {
		t.Errorf("Get('a'): got %v want [0 2]", positions)
	}
	if len(idx.Get("missing")) != 0 {
		t.Error("Get('missing'): expected nil/empty")
	}
}

func TestIndex_GetFirst(t *testing.T) {
	idx := NewIndex([]string{"a", "b", "a"})
	if idx.GetFirst("a") != 0 {
		t.Errorf("GetFirst('a'): got %d want 0", idx.GetFirst("a"))
	}
	if idx.GetFirst("missing") != -1 {
		t.Errorf("GetFirst('missing'): got %d want -1", idx.GetFirst("missing"))
	}
}

func TestIndex_Slice(t *testing.T) {
	idx := NewIndex([]string{"a", "b", "c", "d", "e"})
	pos, err := idx.Slice("b", "d")
	if err != nil {
		t.Fatalf("Slice('b','d'): %v", err)
	}
	if !reflect.DeepEqual(pos, []int{1, 2, 3}) {
		t.Errorf("Slice('b','d'): got %v want [1 2 3]", pos)
	}

	// Non-existent label.
	_, err = idx.Slice("x", "d")
	if err == nil {
		t.Error("Slice with missing start: expected error")
	}

	// start > end.
	_, err = idx.Slice("d", "b")
	if err == nil {
		t.Error("Slice start > end: expected error")
	}
}

func TestIndex_IsUnique(t *testing.T) {
	unique := NewIndex([]string{"a", "b", "c"})
	if !unique.IsUnique() {
		t.Error("IsUnique: expected true for distinct labels")
	}
	dup := NewIndex([]string{"a", "b", "a"})
	if dup.IsUnique() {
		t.Error("IsUnique: expected false for duplicate labels")
	}
}

func TestIndex_String(t *testing.T) {
	idx := NewIndex([]string{"x", "y"})
	s := idx.String()
	if s == "" {
		t.Error("Index.String(): returned empty string")
	}
}

// -----------------------------------------------------------------------
// MultiIndex
// -----------------------------------------------------------------------

func TestMultiIndex_Basic(t *testing.T) {
	mi, err := NewMultiIndex(
		[]string{"US", "US", "EU", "EU"},
		[]string{"2020", "2021", "2020", "2021"},
	)
	if err != nil {
		t.Fatalf("NewMultiIndex: %v", err)
	}
	if mi.NLevels() != 2 {
		t.Errorf("NLevels: got %d want 2", mi.NLevels())
	}
	if mi.Len() != 4 {
		t.Errorf("Len: got %d want 4", mi.Len())
	}
}

func TestMultiIndex_Get_FullKey(t *testing.T) {
	mi, _ := NewMultiIndex(
		[]string{"US", "US", "EU", "EU"},
		[]string{"2020", "2021", "2020", "2021"},
	)
	pos := mi.Get("US", "2021")
	if !reflect.DeepEqual(pos, []int{1}) {
		t.Errorf("MultiIndex.Get(US,2021): got %v want [1]", pos)
	}
}

func TestMultiIndex_Get_PartialKey(t *testing.T) {
	mi, _ := NewMultiIndex(
		[]string{"US", "US", "EU", "EU"},
		[]string{"2020", "2021", "2020", "2021"},
	)
	// Partial key "US" should match rows 0 and 1.
	pos := mi.Get("US")
	if len(pos) != 2 {
		t.Errorf("MultiIndex partial key 'US': got %v want 2 matches", pos)
	}
}

func TestMultiIndex_LengthMismatch(t *testing.T) {
	_, err := NewMultiIndex(
		[]string{"A", "B", "C"},
		[]string{"x", "y"}, // different length
	)
	if err == nil {
		t.Error("NewMultiIndex length mismatch: expected error, got nil")
	}
}

func TestMultiIndex_NoLevels(t *testing.T) {
	_, err := NewMultiIndex()
	if err == nil {
		t.Error("NewMultiIndex no levels: expected error, got nil")
	}
}

// -----------------------------------------------------------------------
// IndexedDataFrame
// -----------------------------------------------------------------------

func TestIndexedDataFrame_WithIndex(t *testing.T) {
	df := New(
		series.New([]float64{10, 20, 30}, series.Float, "val"),
	)
	idx := NewIndex([]string{"a", "b", "c"})
	idf, err := df.WithIndex(idx)
	if err != nil {
		t.Fatalf("WithIndex: %v", err)
	}
	if idf.Index().Len() != 3 {
		t.Errorf("WithIndex Index.Len: got %d want 3", idf.Index().Len())
	}
}

func TestIndexedDataFrame_WithIndex_LengthMismatch(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "val"),
	)
	idx := NewIndex([]string{"only_two", "labels"})
	_, err := df.WithIndex(idx)
	if err == nil {
		t.Error("WithIndex length mismatch: expected error")
	}
}

func TestIndexedDataFrame_Loc(t *testing.T) {
	df := New(
		series.New([]float64{10, 20, 30, 40}, series.Float, "val"),
	)
	idx := NewIndex([]string{"a", "b", "c", "b"})
	idf, _ := df.WithIndex(idx)

	// Loc "b" → rows 1 and 3.
	sub := idf.Loc("b")
	if sub.Err != nil {
		t.Fatalf("Loc('b'): %v", sub.Err)
	}
	if sub.Nrow() != 2 {
		t.Errorf("Loc('b') rows: got %d want 2", sub.Nrow())
	}
	vals := dfColFloats(sub, "val")
	for _, v := range vals {
		if v != 20 && v != 40 {
			t.Errorf("Loc('b') unexpected value %v", v)
		}
	}

	// Loc non-existent label → error.
	bad := idf.Loc("z")
	if bad.Err == nil {
		t.Error("Loc('z'): expected error for missing label")
	}
}

func TestIndexedDataFrame_LocSlice(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "val"),
	)
	idx := NewIndex([]string{"a", "b", "c", "d", "e"})
	idf, _ := df.WithIndex(idx)

	sub := idf.LocSlice("b", "d")
	if sub.Err != nil {
		t.Fatalf("LocSlice: %v", sub.Err)
	}
	if sub.Nrow() != 3 {
		t.Errorf("LocSlice rows: got %d want 3", sub.Nrow())
	}
}

func TestIndexedDataFrame_ResetIndex(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "val"),
	)
	idx := NewIndex([]string{"a", "b", "c"})
	idf, _ := df.WithIndex(idx)

	out := idf.ResetIndex("row")
	if out.Err != nil {
		t.Fatalf("ResetIndex: %v", out.Err)
	}
	// Should have 2 columns: "row" and "val".
	if out.Ncol() != 2 {
		t.Errorf("ResetIndex cols: got %d want 2", out.Ncol())
	}
	if out.ColIndex("row") < 0 {
		t.Error("ResetIndex: column 'row' not found")
	}
	if out.ColIndex("val") < 0 {
		t.Error("ResetIndex: column 'val' not found")
	}
	// First label should be "a".
	if v := out.Col("row").Elem(0).String(); v != "a" {
		t.Errorf("ResetIndex row[0]: got %q want %q", v, "a")
	}
}

func TestIndexedDataFrame_WithColumnIndex(t *testing.T) {
	df := New(
		series.New([]string{"r1", "r2", "r3"}, series.String, "idx"),
		series.New([]float64{10, 20, 30}, series.Float, "val"),
	)
	idf, err := df.WithColumnIndex("idx")
	if err != nil {
		t.Fatalf("WithColumnIndex: %v", err)
	}
	// "idx" column dropped from inner DataFrame.
	if idf.DataFrame().Ncol() != 1 {
		t.Errorf("WithColumnIndex: inner df should have 1 col, got %d", idf.DataFrame().Ncol())
	}
	if idf.Index().Label(1) != "r2" {
		t.Errorf("WithColumnIndex index label[1]: got %q want r2", idf.Index().Label(1))
	}
}

// -----------------------------------------------------------------------
// MultiIndexedDataFrame
// -----------------------------------------------------------------------

func TestMultiIndexedDataFrame_Loc(t *testing.T) {
	df := New(
		series.New([]float64{100, 200, 300, 400}, series.Float, "val"),
	)
	mi, _ := NewMultiIndex(
		[]string{"US", "US", "EU", "EU"},
		[]string{"2020", "2021", "2020", "2021"},
	)
	midf, err := df.WithMultiIndex(mi)
	if err != nil {
		t.Fatalf("WithMultiIndex: %v", err)
	}

	// Full key.
	sub := midf.Loc("EU", "2021")
	if sub.Err != nil {
		t.Fatalf("MultiIndex Loc full key: %v", sub.Err)
	}
	if sub.Nrow() != 1 {
		t.Errorf("MultiIndex Loc rows: got %d want 1", sub.Nrow())
	}
	if dfColFloats(sub, "val")[0] != 400 {
		t.Errorf("MultiIndex Loc value: got %v want 400", dfColFloats(sub, "val")[0])
	}

	// Partial key.
	sub2 := midf.Loc("US")
	if sub2.Err != nil {
		t.Fatalf("MultiIndex Loc partial key: %v", sub2.Err)
	}
	if sub2.Nrow() != 2 {
		t.Errorf("MultiIndex Loc partial rows: got %d want 2", sub2.Nrow())
	}

	// Missing key.
	bad := midf.Loc("AU")
	if bad.Err == nil {
		t.Error("MultiIndex Loc missing key: expected error")
	}
}
