package dataframe

import (
	"database/sql"
	"testing"

	"github.com/dreamsxin/gota/series"
	_ "modernc.org/sqlite" // pure-Go SQLite driver, no CGO required
)

// openTestDB returns an in-memory SQLite database for testing.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("openTestDB: %v", err)
	}
	return db
}

// -----------------------------------------------------------------------
// FromSQL
// -----------------------------------------------------------------------

func TestFromSQL_Basic(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE users (
		name TEXT,
		age  INTEGER,
		score REAL
	)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec(`INSERT INTO users VALUES ('Alice', 30, 9.5), ('Bob', 25, 8.0), ('Carol', 35, 7.5)`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.Query("SELECT name, age, score FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	df := FromSQL(rows)
	if df.Err != nil {
		t.Fatalf("FromSQL: %v", df.Err)
	}

	if df.Nrow() != 3 {
		t.Errorf("FromSQL rows: got %d want 3", df.Nrow())
	}
	if df.Ncol() != 3 {
		t.Errorf("FromSQL cols: got %d want 3", df.Ncol())
	}
	expected := [][]string{
		{"name", "age", "score"},
		{"Alice", "30", "9.500000"},
		{"Bob", "25", "8.000000"},
		{"Carol", "35", "7.500000"},
	}
	if got := df.Records(); !recordsEqual(got, expected) {
		t.Errorf("FromSQL records: got %v want %v", got, expected)
	}
}

func TestFromSQL_NullValues(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE t (val REAL)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = db.Exec(`INSERT INTO t VALUES (1.0), (NULL), (3.0)`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.Query("SELECT val FROM t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	df := FromSQL(rows)
	if df.Err != nil {
		t.Fatalf("FromSQL: %v", df.Err)
	}
	if df.Nrow() != 3 {
		t.Errorf("FromSQL NULLs rows: got %d want 3", df.Nrow())
	}
	// Row 1 (NULL) should appear as NaN in Float column.
	v := df.Col("val").Elem(1)
	if !v.IsNA() {
		t.Errorf("FromSQL NULLs: expected NaN at row 1, got %v", v.Val())
	}
}

func TestFromSQL_NilRows(t *testing.T) {
	df := FromSQL(nil)
	if df.Err == nil {
		t.Error("FromSQL(nil): expected error, got nil")
	}
}

// -----------------------------------------------------------------------
// WriteSQL
// -----------------------------------------------------------------------

func TestWriteSQL_Basic(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df := New(
		series.New([]string{"Alice", "Bob"}, series.String, "name"),
		series.New([]int{30, 25}, series.Int, "age"),
	)

	// Create table and insert.
	err := df.WriteSQL(db, "people", WithCreateTable(true))
	if err != nil {
		t.Fatalf("WriteSQL: %v", err)
	}

	// Verify row count.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM people").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("WriteSQL row count: got %d want 2", count)
	}
}

func TestWriteSQL_RoundTrip(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	orig := New(
		series.New([]string{"x", "y", "z"}, series.String, "label"),
		series.New([]float64{1.1, 2.2, 3.3}, series.Float, "value"),
	)

	if err := orig.WriteSQL(db, "data", WithCreateTable(true)); err != nil {
		t.Fatalf("WriteSQL: %v", err)
	}

	rows, err := db.Query("SELECT label, value FROM data ORDER BY label")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := FromSQL(rows)
	if got.Err != nil {
		t.Fatalf("FromSQL: %v", got.Err)
	}

	if got.Nrow() != orig.Nrow() {
		t.Errorf("SQL round-trip rows: got %d want %d", got.Nrow(), orig.Nrow())
	}
	expected := [][]string{
		{"label", "value"},
		{"x", "1.100000"},
		{"y", "2.200000"},
		{"z", "3.300000"},
	}
	if gotRecords := got.Records(); !recordsEqual(gotRecords, expected) {
		t.Errorf("SQL round-trip records: got %v want %v", gotRecords, expected)
	}
}

func recordsEqual(got, want [][]string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			return false
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				return false
			}
		}
	}
	return true
}

func TestWriteSQL_TruncateFirst(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df := New(
		series.New([]string{"a"}, series.String, "name"),
	)
	// First insert.
	if err := df.WriteSQL(db, "items", WithCreateTable(true)); err != nil {
		t.Fatalf("first WriteSQL: %v", err)
	}
	// Second insert with TruncateFirst → should replace, not append.
	if err := df.WriteSQL(db, "items", WithTruncateFirst(true)); err != nil {
		t.Fatalf("second WriteSQL: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("WriteSQL TruncateFirst: got %d rows want 1", count)
	}
}

func TestWriteSQL_BatchSize(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// 10 rows, batch=3 → requires multiple INSERT statements.
	names := make([]string, 10)
	for i := range names {
		names[i] = "row"
	}
	df := New(
		series.New(names, series.String, "name"),
	)
	if err := df.WriteSQL(db, "batch_test", WithCreateTable(true), WithBatchSize(3)); err != nil {
		t.Fatalf("WriteSQL batch: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM batch_test").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 10 {
		t.Errorf("WriteSQL batch count: got %d want 10", count)
	}
}

func TestWriteSQL_UpsertSQLite(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE people (
		id INTEGER PRIMARY KEY,
		name TEXT,
		score REAL
	)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	initial := New(
		series.New([]int{1, 2}, series.Int, "id"),
		series.New([]string{"Alice", "Bob"}, series.String, "name"),
		series.New([]float64{9.5, 8.0}, series.Float, "score"),
	)
	if err := initial.WriteSQL(db, "people"); err != nil {
		t.Fatalf("initial WriteSQL: %v", err)
	}

	update := New(
		series.New([]int{2, 3}, series.Int, "id"),
		series.New([]string{"Bobby", "Carol"}, series.String, "name"),
		series.New([]float64{8.8, 7.5}, series.Float, "score"),
	)
	if err := update.WriteSQL(db, "people", WithUpsert("id")); err != nil {
		t.Fatalf("upsert WriteSQL: %v", err)
	}

	rows, err := db.Query("SELECT id, name, score FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := FromSQL(rows)
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	want := [][]string{
		{"id", "name", "score"},
		{"1", "Alice", "9.500000"},
		{"2", "Bobby", "8.800000"},
		{"3", "Carol", "7.500000"},
	}
	if records := got.Records(); !recordsEqual(records, want) {
		t.Fatalf("upsert records: got %v want %v", records, want)
	}
}

func TestWriteSQL_UpsertUpdateColumns(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE people (
		id INTEGER PRIMARY KEY,
		name TEXT,
		score REAL
	)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO people VALUES (1, 'Alice', 9.5)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	update := New(
		series.New([]int{1}, series.Int, "id"),
		series.New([]string{"Alicia"}, series.String, "name"),
		series.New([]float64{10.0}, series.Float, "score"),
	)
	if err := update.WriteSQL(db, "people", WithUpsert("id"), WithUpsertUpdateColumns("score")); err != nil {
		t.Fatalf("upsert WriteSQL: %v", err)
	}

	var name string
	var score float64
	if err := db.QueryRow("SELECT name, score FROM people WHERE id = 1").Scan(&name, &score); err != nil {
		t.Fatalf("query: %v", err)
	}
	if name != "Alice" || score != 10.0 {
		t.Fatalf("upsert selected columns: got name=%s score=%v", name, score)
	}
}

func TestWriteSQL_UpsertMissingConflictColumn(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df := New(series.New([]int{1}, series.Int, "id"))
	err := df.WriteSQL(db, "items", WithCreateTable(true), WithUpsert("missing"))
	if err == nil {
		t.Fatal("expected missing conflict column error")
	}
}
