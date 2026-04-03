package dataframe

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// Bug fix: Query — column names containing operator substrings
// -----------------------------------------------------------------------

func TestDataFrame_Query_ColumnNameContainsOperator(t *testing.T) {
	// Column "income" contains "in"; "order_count" contains "or".
	// These must not be misidentified as operators.
	df := New(
		series.New([]float64{50000, 80000, 30000}, series.Float, "income"),
		series.New([]int{5, 10, 2}, series.Int, "order_count"),
	)
	out := df.Query("income > 40000")
	if out.Err != nil {
		t.Fatalf("Query column with 'in': %v", out.Err)
	}
	if out.Nrow() != 2 {
		t.Errorf("Query income > 40000: got %d rows want 2", out.Nrow())
	}
}

func TestDataFrame_Query_ColumnNameContainsAND(t *testing.T) {
	// Column "bandwidth" contains "and".
	df := New(
		series.New([]float64{100, 200, 50}, series.Float, "bandwidth"),
	)
	out := df.Query("bandwidth >= 100")
	if out.Err != nil {
		t.Fatalf("Query column with 'and': %v", out.Err)
	}
	if out.Nrow() != 2 {
		t.Errorf("Query bandwidth >= 100: got %d rows want 2", out.Nrow())
	}
}

// -----------------------------------------------------------------------
// Bug fix: GetGroups — hidden index column stripped
// -----------------------------------------------------------------------

func TestGroups_GetGroups_NoHiddenColumn(t *testing.T) {
	df := New(
		series.New([]string{"A", "B", "A"}, series.String, "grp"),
		series.New([]float64{1, 2, 3}, series.Float, "val"),
	)
	groups := df.GroupBy("grp").GetGroups()
	for key, gdf := range groups {
		if gdf.ColIndex("__groupby_row_idx__") >= 0 {
			t.Errorf("GetGroups[%s]: hidden column leaked into result", key)
		}
		// Should only have the original columns.
		if gdf.Ncol() != 2 {
			t.Errorf("GetGroups[%s]: ncol=%d want 2", key, gdf.Ncol())
		}
	}
}

// -----------------------------------------------------------------------
// Bug fix: Apply — hidden index column stripped before user function
// -----------------------------------------------------------------------

func TestGroups_Apply_NoHiddenColumn(t *testing.T) {
	df := New(
		series.New([]string{"A", "B", "A"}, series.String, "grp"),
		series.New([]float64{1, 2, 3}, series.Float, "val"),
	)
	result := df.GroupBy("grp").Apply(func(g DataFrame) DataFrame {
		// If hidden column leaked, Ncol would be 3 instead of 2.
		if g.Ncol() != 2 {
			panic("hidden column leaked into Apply callback")
		}
		return g
	})
	if result.Err != nil {
		t.Fatal(result.Err)
	}
}

// -----------------------------------------------------------------------
// Bug fix: Unstack — empty idVars returns error
// -----------------------------------------------------------------------

func TestDataFrame_Unstack_EmptyIdVars(t *testing.T) {
	df := New(
		series.New([]string{"q1", "q2"}, series.String, "quarter"),
		series.New([]string{"10", "20"}, series.String, "value"),
	)
	out := df.Unstack([]string{}, "quarter", "value")
	if out.Err == nil {
		t.Error("Unstack empty idVars: expected error")
	}
}

// -----------------------------------------------------------------------
// Bug fix: ScanCSV — batch data not corrupted across calls
// -----------------------------------------------------------------------

func TestScanCSV_BatchDataIntegrity(t *testing.T) {
	// 6 rows, batch=2 → 3 batches. Verify each batch has correct data.
	csv := "id\n1\n2\n3\n4\n5\n6\n"
	var results []string
	err := ScanCSV(strings.NewReader(csv), 2, func(batch DataFrame) error {
		for i := 0; i < batch.Nrow(); i++ {
			results = append(results, batch.Col("id").Elem(i).String())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"1", "2", "3", "4", "5", "6"}
	if !reflect.DeepEqual(results, want) {
		t.Errorf("ScanCSV batch integrity: got %v want %v", results, want)
	}
}

// -----------------------------------------------------------------------
// Bug fix: Query — AND/OR with column names containing those words
// -----------------------------------------------------------------------

func TestDataFrame_Query_AND_OR_Compound(t *testing.T) {
	df := New(
		series.New([]float64{10, 20, 30, 40}, series.Float, "score"),
		series.New([]string{"A", "B", "A", "B"}, series.String, "group"),
	)
	// AND: score > 15 AND group == A → only row 2 (30, A)
	out := df.Query("score > 15 AND group == A")
	if out.Err != nil {
		t.Fatalf("Query AND: %v", out.Err)
	}
	if out.Nrow() != 1 {
		t.Errorf("Query AND rows: got %d want 1", out.Nrow())
	}

	// OR: score < 15 OR group == B → rows 0(10,A), 1(20,B), 3(40,B)
	out2 := df.Query("score < 15 OR group == B")
	if out2.Err != nil {
		t.Fatalf("Query OR: %v", out2.Err)
	}
	if out2.Nrow() != 3 {
		t.Errorf("Query OR rows: got %d want 3", out2.Nrow())
	}
}
