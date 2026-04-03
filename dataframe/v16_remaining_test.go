package dataframe

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// v1.4 — RapplyParallel
// -----------------------------------------------------------------------

func TestDataFrame_RapplyParallel(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3, 4, 5}, series.Float, "A"),
		series.New([]float64{10, 20, 30, 40, 50}, series.Float, "B"),
	)
	// Sum each row.
	sumRow := func(s series.Series) series.Series {
		var total float64
		for i := 0; i < s.Len(); i++ {
			total += s.Elem(i).Float()
		}
		return series.Floats(total)
	}
	seq := df.Rapply(sumRow)
	par := df.RapplyParallel(sumRow)

	if seq.Err != nil {
		t.Fatal(seq.Err)
	}
	if par.Err != nil {
		t.Fatal(par.Err)
	}
	if seq.Nrow() != par.Nrow() {
		t.Fatalf("RapplyParallel rows: seq=%d par=%d", seq.Nrow(), par.Nrow())
	}
	// Values must match.
	for i := 0; i < seq.Nrow(); i++ {
		sv := seq.Elem(i, 0).Float()
		pv := par.Elem(i, 0).Float()
		if math.Abs(sv-pv) > 1e-9 {
			t.Errorf("RapplyParallel[%d]: seq=%v par=%v", i, sv, pv)
		}
	}
}

func TestDataFrame_RapplyParallel_LargeDataset(t *testing.T) {
	// Stress test with 1000 rows to exercise goroutine pool.
	n := 1000
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = float64(i)
	}
	df := New(series.New(vals, series.Float, "x"))
	double := func(s series.Series) series.Series {
		v := s.Elem(0).Float()
		return series.Floats(v * 2)
	}
	out := df.RapplyParallel(double)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != n {
		t.Fatalf("RapplyParallel large: rows=%d want %d", out.Nrow(), n)
	}
	// Spot-check last row.
	last := out.Elem(n-1, 0).Float()
	if !compareFloats(last, float64(n-1)*2, 9) {
		t.Errorf("RapplyParallel large last: got %v want %v", last, float64(n-1)*2)
	}
}

// -----------------------------------------------------------------------
// v1.5 — WriteSQL named placeholders
// -----------------------------------------------------------------------

func TestWriteSQL_DollarPlaceholder(t *testing.T) {
	// SQLite doesn't support $1 style, but we can verify the generated SQL
	// by checking that the placeholder builder produces the right strings.
	ph := buildPlaceholder(SQLPlaceholderDollar, 1)
	if ph != "$1" {
		t.Errorf("dollar placeholder: got %q want $1", ph)
	}
	ph3 := buildPlaceholder(SQLPlaceholderDollar, 3)
	if ph3 != "$3" {
		t.Errorf("dollar placeholder 3: got %q want $3", ph3)
	}
}

func TestWriteSQL_AtPlaceholder(t *testing.T) {
	ph := buildPlaceholder(SQLPlaceholderAt, 2)
	if ph != "@p2" {
		t.Errorf("at placeholder: got %q want @p2", ph)
	}
}

func TestWriteSQL_QuestionPlaceholder(t *testing.T) {
	ph := buildPlaceholder(SQLPlaceholderQuestion, 99)
	if ph != "?" {
		t.Errorf("question placeholder: got %q want ?", ph)
	}
}

func TestWriteSQL_DefaultPlaceholder_SQLite(t *testing.T) {
	// Verify that the default (?) style still works end-to-end with SQLite.
	db := openTestDB(t)
	defer db.Close()
	df := New(series.New([]string{"a", "b"}, series.String, "name"))
	if err := df.WriteSQL(db, "ph_test", WithCreateTable(true)); err != nil {
		t.Fatalf("WriteSQL default placeholder: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM ph_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("WriteSQL default placeholder count: got %d want 2", count)
	}
}

// -----------------------------------------------------------------------
// v1.5 — ScanCSV streaming
// -----------------------------------------------------------------------

func TestScanCSV_Basic(t *testing.T) {
	csv := "name,age\nalice,30\nbob,25\ncarol,35\ndave,40\neve,22\n"
	var batches []DataFrame
	err := ScanCSV(strings.NewReader(csv), 2, func(batch DataFrame) error {
		batches = append(batches, batch)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// 5 data rows, batch=2 → 3 batches (2+2+1)
	if len(batches) != 3 {
		t.Fatalf("ScanCSV batches: got %d want 3", len(batches))
	}
	if batches[0].Nrow() != 2 {
		t.Errorf("ScanCSV batch[0] rows: got %d want 2", batches[0].Nrow())
	}
	if batches[2].Nrow() != 1 {
		t.Errorf("ScanCSV batch[2] rows: got %d want 1", batches[2].Nrow())
	}
}

func TestScanCSV_TotalRows(t *testing.T) {
	// Build a 100-row CSV.
	var sb strings.Builder
	sb.WriteString("id,value\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&sb, "%d,%d\n", i, i*2)
	}
	total := 0
	err := ScanCSV(strings.NewReader(sb.String()), 10, func(batch DataFrame) error {
		total += batch.Nrow()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 100 {
		t.Errorf("ScanCSV total rows: got %d want 100", total)
	}
}

func TestScanCSV_EarlyStop(t *testing.T) {
	csv := "x\n1\n2\n3\n4\n5\n"
	count := 0
	err := ScanCSV(strings.NewReader(csv), 1, func(batch DataFrame) error {
		count++
		if count >= 2 {
			return fmt.Errorf("stop")
		}
		return nil
	})
	if err == nil || err.Error() != "stop" {
		t.Errorf("ScanCSV early stop: got err=%v want 'stop'", err)
	}
	if count != 2 {
		t.Errorf("ScanCSV early stop count: got %d want 2", count)
	}
}

func TestScanCSV_ZeroBatchSize(t *testing.T) {
	// batchSize=0 → single call with all rows.
	csv := "a,b\n1,2\n3,4\n5,6\n"
	calls := 0
	err := ScanCSV(strings.NewReader(csv), 0, func(batch DataFrame) error {
		calls++
		if batch.Nrow() != 3 {
			t.Errorf("ScanCSV zero batch: got %d rows want 3", batch.Nrow())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("ScanCSV zero batch calls: got %d want 1", calls)
	}
}

// -----------------------------------------------------------------------
// v1.3 — DataFrame.Query
// -----------------------------------------------------------------------

func TestDataFrame_Query_NumericGT(t *testing.T) {
	df := New(
		series.New([]string{"a", "b", "c", "d"}, series.String, "name"),
		series.New([]float64{10, 25, 5, 40}, series.Float, "score"),
	)
	out := df.Query("score > 15")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 2 {
		t.Fatalf("Query > rows: got %d want 2", out.Nrow())
	}
	names := out.Col("name").Records()
	if !reflect.DeepEqual(names, []string{"b", "d"}) {
		t.Errorf("Query > names: got %v want [b d]", names)
	}
}

func TestDataFrame_Query_StringEq(t *testing.T) {
	df := New(
		series.New([]string{"US", "UK", "DE", "US"}, series.String, "country"),
		series.New([]int{1, 2, 3, 4}, series.Int, "id"),
	)
	out := df.Query("country == US")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 2 {
		t.Fatalf("Query == rows: got %d want 2", out.Nrow())
	}
}

func TestDataFrame_Query_In(t *testing.T) {
	df := New(
		series.New([]string{"US", "UK", "DE", "CA", "FR"}, series.String, "country"),
	)
	out := df.Query("country in US,UK,CA")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 3 {
		t.Fatalf("Query in rows: got %d want 3", out.Nrow())
	}
}

func TestDataFrame_Query_NotIn(t *testing.T) {
	df := New(
		series.New([]string{"US", "UK", "DE", "CA"}, series.String, "country"),
	)
	out := df.Query("country not in US,UK")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 2 {
		t.Fatalf("Query not in rows: got %d want 2", out.Nrow())
	}
}

func TestDataFrame_Query_AND(t *testing.T) {
	df := New(
		series.New([]float64{10, 20, 30, 40, 50}, series.Float, "age"),
		series.New([]string{"M", "F", "M", "F", "M"}, series.String, "gender"),
	)
	out := df.Query("age >= 20 AND gender == M")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	// age>=20 AND gender==M: rows 2(30,M) and 4(50,M)
	if out.Nrow() != 2 {
		t.Fatalf("Query AND rows: got %d want 2", out.Nrow())
	}
}

func TestDataFrame_Query_OR(t *testing.T) {
	df := New(
		series.New([]float64{5, 15, 25, 35}, series.Float, "score"),
	)
	out := df.Query("score < 10 OR score > 30")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 2 {
		t.Fatalf("Query OR rows: got %d want 2", out.Nrow())
	}
}

func TestDataFrame_Query_EmptyExpr(t *testing.T) {
	df := New(series.New([]int{1, 2, 3}, series.Int, "x"))
	out := df.Query("")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 3 {
		t.Errorf("Query empty: got %d rows want 3", out.Nrow())
	}
}

func TestDataFrame_Query_InvalidColumn(t *testing.T) {
	df := New(series.New([]int{1, 2}, series.Int, "x"))
	out := df.Query("no_such > 0")
	if out.Err == nil {
		t.Error("Query invalid column: expected error")
	}
}

func TestDataFrame_Query_NEQ(t *testing.T) {
	df := New(
		series.New([]string{"a", "b", "a", "c"}, series.String, "v"),
	)
	out := df.Query("v != a")
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if out.Nrow() != 2 {
		t.Fatalf("Query != rows: got %d want 2", out.Nrow())
	}
}
