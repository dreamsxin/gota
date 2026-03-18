package dataframe_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dreamsxin/gota/dataframe"
	"github.com/dreamsxin/gota/series"
)

// ============================================================================
// Head & Tail Tests
// ============================================================================

func TestDataFrame_Head(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"1", "2"},
		{"3", "4"},
		{"5", "6"},
		{"7", "8"},
		{"9", "10"},
	})

	// Test normal case
	head := df.Head(3)
	if head.Nrow() != 3 {
		t.Errorf("Expected 3 rows, got %d", head.Nrow())
	}

	// Test n > nrows
	head = df.Head(100)
	if head.Nrow() != 5 {
		t.Errorf("Expected 5 rows when n > nrows, got %d", head.Nrow())
	}

	// Test n = 0
	head = df.Head(0)
	if head.Nrow() != 0 {
		t.Errorf("Expected 0 rows when n=0, got %d", head.Nrow())
	}
}

func TestDataFrame_Tail(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"1", "2"},
		{"3", "4"},
		{"5", "6"},
		{"7", "8"},
		{"9", "10"},
	})

	// Test normal case
	tail := df.Tail(3)
	if tail.Nrow() != 3 {
		t.Errorf("Expected 3 rows, got %d", tail.Nrow())
	}

	// Check last values
	if tail.Elem(2, 0).String() != "9" {
		t.Errorf("Expected last value to be 9, got %s", tail.Elem(2, 0).String())
	}

	// Test n > nrows
	tail = df.Tail(100)
	if tail.Nrow() != 5 {
		t.Errorf("Expected 5 rows when n > nrows, got %d", tail.Nrow())
	}

	// Test n = 0
	tail = df.Tail(0)
	if tail.Nrow() != 0 {
		t.Errorf("Expected 0 rows when n=0, got %d", tail.Nrow())
	}
}

// ============================================================================
// Info Tests
// ============================================================================

func TestDataFrame_Info(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B", "C"},
		{"1", "2.5", "hello"},
		{"2", "3.5", "world"},
		{"NaN", "4.5", "test"},
	})

	var buf bytes.Buffer
	df.Info(&buf)

	output := buf.String()
	
	// Check output contains expected information
	if !strings.Contains(output, "3 entries") {
		t.Errorf("Info should contain row count")
	}
	if !strings.Contains(output, "3 columns") {
		t.Errorf("Info should contain column count")
	}
	if !strings.Contains(output, "A") || !strings.Contains(output, "B") || !strings.Contains(output, "C") {
		t.Errorf("Info should contain column names")
	}
}

// ============================================================================
// IsNull & NotNull Tests
// ============================================================================

func TestDataFrame_IsNull(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"1", "2"},
		{"NaN", "4"},
		{"3", "NaN"},
	})

	mask := df.IsNull()
	
	if mask.Nrow() != 3 || mask.Ncol() != 2 {
		t.Errorf("IsNull should return same dimensions")
	}

	// Check [1,0] is true (NaN in column A, row 1)
	val, _ := mask.Elem(1, 0).Bool()
	if !val {
		t.Errorf("Expected true for NaN value")
	}

	// Check [0,0] is false (non-NaN value)
	val, _ = mask.Elem(0, 0).Bool()
	if val {
		t.Errorf("Expected false for non-NaN value")
	}
}

func TestDataFrame_NotNull(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"1", "2"},
		{"NaN", "4"},
		{"3", "NaN"},
	})

	mask := df.NotNull()
	
	// Check [0,0] is true (non-NaN value)
	val, _ := mask.Elem(0, 0).Bool()
	if !val {
		t.Errorf("Expected true for non-NaN value")
	}

	// Check [1,0] is false (NaN value)
	val, _ = mask.Elem(1, 0).Bool()
	if val {
		t.Errorf("Expected false for NaN value")
	}
}

// ============================================================================
// ValueCounts Tests
// ============================================================================

func TestDataFrame_ValueCounts(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "1"},
		{"y", "2"},
		{"x", "3"},
		{"z", "4"},
		{"x", "5"},
	})

	// Test count mode
	vc := df.ValueCounts("A", false, false)
	
	if vc.Nrow() != 3 {
		t.Errorf("Expected 3 unique values, got %d", vc.Nrow())
	}

	// x should have count 3
	countCol := vc.Col("count")
	if countCol.Elem(0).Float() != 3 {
		t.Errorf("Expected x count to be 3, got %f", countCol.Elem(0).Float())
	}

	// Test normalize mode
	vcNorm := df.ValueCounts("A", true, false)
	propCol := vcNorm.Col("proportion")
	if propCol.Elem(0).Float() != 0.6 {
		t.Errorf("Expected x proportion to be 0.6, got %f", propCol.Elem(0).Float())
	}
}

// ============================================================================
// NLargest & NSmallest Tests
// ============================================================================

func TestDataFrame_NLargest(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "1"},
		{"y", "5"},
		{"z", "3"},
		{"w", "9"},
	})

	top3 := df.NLargest(3, "B")
	
	if top3.Nrow() != 3 {
		t.Errorf("Expected 3 rows, got %d", top3.Nrow())
	}

	// First row should have B=9
	if top3.Elem(0, 1).Float() != 9 {
		t.Errorf("Expected largest value 9, got %f", top3.Elem(0, 1).Float())
	}
}

func TestDataFrame_NSmallest(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "1"},
		{"y", "5"},
		{"z", "3"},
		{"w", "9"},
	})

	bottom3 := df.NSmallest(3, "B")
	
	if bottom3.Nrow() != 3 {
		t.Errorf("Expected 3 rows, got %d", bottom3.Nrow())
	}

	// First row should have B=1
	if bottom3.Elem(0, 1).Float() != 1 {
		t.Errorf("Expected smallest value 1, got %f", bottom3.Elem(0, 1).Float())
	}
}

// ============================================================================
// Sample Tests
// ============================================================================

func TestDataFrame_Sample(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"1", "2"},
		{"3", "4"},
		{"5", "6"},
		{"7", "8"},
		{"9", "10"},
	})

	// Test sampling n rows
	sample := df.Sample(3, -1, false, 42)
	if sample.Nrow() != 3 {
		t.Errorf("Expected 3 rows, got %d", sample.Nrow())
	}

	// Test sampling fraction
	sample = df.Sample(-1, 0.5, false, 42)
	if sample.Nrow() != 2 {
		t.Errorf("Expected 2 rows (50%% of 5), got %d", sample.Nrow())
	}

	// Test sampling with replacement
	sample = df.Sample(10, -1, true, 42)
	if sample.Nrow() != 10 {
		t.Errorf("Expected 10 rows with replacement, got %d", sample.Nrow())
	}
}

// ============================================================================
// Pipe Tests
// ============================================================================

func TestDataFrame_Pipe(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"1", "2"},
		{"3", "4"},
		{"5", "6"},
	})

	// Test simple pipe
	result := df.Pipe(func(d dataframe.DataFrame) dataframe.DataFrame {
		return d.Select([]string{"A"})
	})

	if result.Ncol() != 1 {
		t.Errorf("Pipe should have applied function")
	}

	// Test pipe with args
	result = df.PipeWithArgs(func(d dataframe.DataFrame, args ...interface{}) dataframe.DataFrame {
		colname := args[0].(string)
		return d.Select([]string{colname})
	}, "B")

	if result.Ncol() != 1 || result.Names()[0] != "B" {
		t.Errorf("PipeWithArgs should have applied function with args")
	}
}

// ============================================================================
// Clip Tests
// ============================================================================

func TestDataFrame_Clip(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"-5", "150"},
		{"50", "200"},
		{"150", "50"},
	})

	lower := 0.0
	upper := 100.0
	
	clipped := df.Clip(&lower, &upper)

	// Check values are clipped
	if clipped.Elem(0, 0).Float() != 0 {
		t.Errorf("Expected -5 to be clipped to 0, got %f", clipped.Elem(0, 0).Float())
	}
	if clipped.Elem(0, 1).Float() != 100 {
		t.Errorf("Expected 150 to be clipped to 100, got %f", clipped.Elem(0, 1).Float())
	}
	if clipped.Elem(1, 0).Float() != 50 {
		t.Errorf("Expected 50 to remain 50, got %f", clipped.Elem(1, 0).Float())
	}
}

func TestDataFrame_ClipColumn(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"-5", "150"},
		{"50", "200"},
	})

	lower := 0.0
	upper := 100.0
	
	clipped := df.ClipColumn("A", &lower, &upper)

	// Check column A is clipped
	if clipped.Elem(0, 0).Float() != 0 {
		t.Errorf("Expected -5 to be clipped to 0, got %f", clipped.Elem(0, 0).Float())
	}

	// Check column B is unchanged
	if clipped.Elem(0, 1).Float() != 150 {
		t.Errorf("Expected column B to be unchanged, got %f", clipped.Elem(0, 1).Float())
	}
}

// ============================================================================
// Replace Tests
// ============================================================================

func TestDataFrame_Replace(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "1"},
		{"y", "2"},
		{"x", "3"},
	})

	replaced := df.Replace("x", "z")

	// Check x values are replaced
	if replaced.Elem(0, 0).String() != "z" {
		t.Errorf("Expected x to be replaced with z, got %s", replaced.Elem(0, 0).String())
	}
	if replaced.Elem(2, 0).String() != "z" {
		t.Errorf("Expected x to be replaced with z, got %s", replaced.Elem(2, 0).String())
	}

	// Check y is unchanged
	if replaced.Elem(1, 0).String() != "y" {
		t.Errorf("Expected y to be unchanged, got %s", replaced.Elem(1, 0).String())
	}
}

func TestDataFrame_ReplaceInColumn(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "1"},
		{"y", "2"},
		{"x", "3"},
	})

	replaced := df.ReplaceInColumn("A", "x", "z")

	// Check column A is replaced
	if replaced.Elem(0, 0).String() != "z" {
		t.Errorf("Expected x to be replaced with z, got %s", replaced.Elem(0, 0).String())
	}

	// Check column B is unchanged
	if replaced.Elem(0, 1).String() != "1" {
		t.Errorf("Expected column B to be unchanged, got %s", replaced.Elem(0, 1).String())
	}
}

// ============================================================================
// Astype Tests
// ============================================================================

func TestDataFrame_Astype(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"1", "2.5"},
		{"3", "4.5"},
		{"5", "6.5"},
	})

	// Convert A from string to int
	converted := df.Astype(map[string]series.Type{
		"A": series.Int,
	})

	// Check type conversion
	if converted.Col("A").Type() != series.Int {
		t.Errorf("Expected column A to be Int, got %v", converted.Col("A").Type())
	}
}

// ============================================================================
// Between Tests
// ============================================================================

func TestDataFrame_Between(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "5"},
		{"y", "15"},
		{"z", "25"},
	})

	// Test inclusive bounds
	mask := df.Between("B", 10, 20, "both")
	
	val, _ := mask.Elem(0).Bool()
	if val {
		t.Errorf("Expected 5 to be outside [10,20]")
	}
	val, _ = mask.Elem(1).Bool()
	if !val {
		t.Errorf("Expected 15 to be inside [10,20]")
	}
	val, _ = mask.Elem(2).Bool()
	if val {
		t.Errorf("Expected 25 to be outside [10,20]")
	}
}

// ============================================================================
// IsIn Tests
// ============================================================================

func TestDataFrame_IsIn(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "1"},
		{"y", "2"},
		{"z", "3"},
		{"w", "4"},
	})

	mask := df.IsIn("A", []interface{}{"x", "z"})

	val, _ := mask.Elem(0).Bool()
	if !val {
		t.Errorf("Expected x to be in set")
	}
	val, _ = mask.Elem(1).Bool()
	if val {
		t.Errorf("Expected y to not be in set")
	}
	val, _ = mask.Elem(2).Bool()
	if !val {
		t.Errorf("Expected z to be in set")
	}
	val, _ = mask.Elem(3).Bool()
	if val {
		t.Errorf("Expected w to not be in set")
	}
}

func TestDataFrame_FilterIsIn(t *testing.T) {
	df := dataframe.LoadRecords([][]string{
		{"A", "B"},
		{"x", "1"},
		{"y", "2"},
		{"z", "3"},
		{"w", "4"},
	})

	filtered := df.FilterIsIn("A", []interface{}{"x", "z"})

	if filtered.Nrow() != 2 {
		t.Errorf("Expected 2 rows after filtering, got %d", filtered.Nrow())
	}

	if filtered.Elem(0, 0).String() != "x" {
		t.Errorf("Expected first row to be x, got %s", filtered.Elem(0, 0).String())
	}
}
