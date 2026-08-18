package dataframe

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dreamsxin/gota/series"
)

var errDF = New(
	series.New([]string{"a", "b"}, series.String, "key"),
	series.New([]int{1, 2}, series.Int, "num"),
)

// TestSentinelErrors pins errors.Is matching for the main failure paths.
// The sentinel errors are matched without changing any message text, so
// callers can branch on error kind while existing string assertions keep
// working.
func TestSentinelErrors(t *testing.T) {
	table := []struct {
		name string
		err  error
		want error
	}{
		{"New empty", New().Err, ErrEmptyDataFrame},
		{"NewNoCopy empty", NewNoCopy().Err, ErrEmptyDataFrame},
		{"LoadRecords empty", LoadRecords(nil).Err, ErrEmptyDataFrame},
		{"LoadRecords header only", LoadRecords([][]string{{"A"}}).Err, ErrEmptyDataFrame},
		{"New dimension mismatch", New(
			series.New([]int{1, 2}, series.Int, "a"),
			series.New([]int{1}, series.Int, "b"),
		).Err, ErrLengthMismatch},
		{"Col unknown", errDF.Col("missing").Err, ErrColumnNotFound},
		{"Select unknown name", errDF.Select([]string{"missing"}).Err, ErrColumnNotFound},
		{"Drop unknown name", errDF.Drop([]string{"missing"}).Err, ErrColumnNotFound},
		{"Select out of range", errDF.Select([]int{99}).Err, ErrIndexOutOfRange},
		{"Set column count mismatch", errDF.Set([]int{0}, LoadRecords([][]string{
			{"key", "num", "extra"},
			{"a", "1", "x"},
		})).Err, ErrLengthMismatch},
		{"ClipColumn unknown", errDF.ClipColumn("missing", nil, nil).Err, ErrColumnNotFound},
		{"ReplaceInColumn unknown", errDF.ReplaceInColumn("missing", "a", "b").Err, ErrColumnNotFound},
		{"ExplodeOn unknown", errDF.ExplodeOn("missing", ",").Err, ErrColumnNotFound},
		{"RenameAll unknown", func() error {
			_, err := errDF.RenameAll(map[string]string{"missing": "x"})
			return err
		}(), ErrColumnNotFound},
		{"InnerJoin no keys", errDF.InnerJoin(errDF).Err, ErrEmptyKeys},
		{"LeftJoin no keys", errDF.LeftJoin(errDF).Err, ErrEmptyKeys},
		{"RightJoin no keys", errDF.RightJoin(errDF).Err, ErrEmptyKeys},
		{"OuterJoin no keys", errDF.OuterJoin(errDF).Err, ErrEmptyKeys},
		{"InnerJoin unknown key", errDF.InnerJoin(errDF, "missing").Err, ErrKeyNotFound},
		{"LeftJoin unknown key", errDF.LeftJoin(errDF, "missing").Err, ErrKeyNotFound},
		{"OuterJoin unknown key", errDF.OuterJoin(errDF, "missing").Err, ErrKeyNotFound},
		{"Aggregation invalid method", errDF.GroupBy("key").Aggregation(
			[]AggregationType{AggregationType(999)}, []string{"num"},
		).Err, ErrInvalidAggregation},
		{"Index Loc unknown label", func() error {
			idf, err := errDF.WithIndex(NewIndex([]string{"x", "y"}))
			if err != nil {
				t.Fatalf("WithIndex: %v", err)
			}
			return idf.Loc("z").Err
		}(), ErrKeyNotFound},
		{"Index LocSlice unknown label", func() error {
			idf, err := errDF.WithIndex(NewIndex([]string{"x", "y"}))
			if err != nil {
				t.Fatalf("WithIndex: %v", err)
			}
			return idf.LocSlice("z", "x").Err
		}(), ErrKeyNotFound},
		{"Query unknown column", errDF.Query("missing > 1").Err, ErrColumnNotFound},
		{"WriteXLSX unknown style column", func() error {
			var buf bytes.Buffer
			return errDF.WriteXLSX(&buf, WithXLSXColumnWidths(map[string]float64{"missing": 18}))
		}(), ErrColumnNotFound},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !errors.Is(tc.err, tc.want) {
				t.Fatalf("errors.Is(%q, %v) = false", tc.err, tc.want)
			}
		})
	}
}

// TestSentinelErrorsPreserveMessages guards the v1 compatibility promise:
// wrapping adds error-kind matching only, never new message text.
func TestSentinelErrorsPreserveMessages(t *testing.T) {
	table := []struct {
		got  error
		want string
	}{
		{New().Err, "empty DataFrame"},
		{errDF.Col("missing").Err, "unknown column name"},
		{errDF.Select([]string{"missing"}).Err, `can't select columns: column name "missing" not found`},
		{errDF.Drop([]string{"missing"}).Err, `can't drop columns: column name "missing" not found`}, {errDF.InnerJoin(errDF).Err, "join keys not specified"},
		{errDF.InnerJoin(errDF, "missing").Err, `can't find key "missing" on left DataFrame` + "\n" + `can't find key "missing" on right DataFrame`},
	}
	for _, tc := range table {
		if tc.got == nil {
			t.Fatalf("expected error with message %q, got nil", tc.want)
		}
		if tc.got.Error() != tc.want {
			t.Fatalf("message = %q, want %q", tc.got.Error(), tc.want)
		}
	}
}

// TestSentinelErrorsSticky verifies error-kind matching survives chained
// no-op operations on a sticky-error DataFrame.
func TestSentinelErrorsSticky(t *testing.T) {
	a := errDF.Select([]string{"missing"})
	if !errors.Is(a.Err, ErrColumnNotFound) {
		t.Fatalf("initial error should match ErrColumnNotFound, got %v", a.Err)
	}
	b := a.Filter(F{Colname: "key", Comparator: series.Eq, Comparando: "a"}).Head(1)
	if !errors.Is(b.Err, ErrColumnNotFound) {
		t.Fatalf("sticky error should survive chaining, got %v", b.Err)
	}
}
