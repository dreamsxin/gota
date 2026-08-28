package dataframe

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// Golden-output tests for the Milestone 2 batch kernels: filter mask
// combination, single-pass arrange, typed join keys, and BatchTransform.

func milestoneFilterFrame() DataFrame {
	return New(
		series.New([]int{1, 2, 3, 4, 5, 6}, series.Int, "A"),
		series.New([]string{"x", "y", "x", "y", "x", "y"}, series.String, "B"),
	)
}

func TestMilestone2_Filter_Golden(t *testing.T) {
	df := milestoneFilterFrame()

	// Or within one call.
	or := df.Filter(
		F{Colname: "A", Comparator: series.Eq, Comparando: 1},
		F{Colname: "A", Comparator: series.Eq, Comparando: 6},
	)
	if got, want := or.Records(), [][]string{
		{"A", "B"}, {"1", "x"}, {"6", "y"},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("or filter: got %v want %v", got, want)
	}

	// And via FilterAggregation.
	and := df.FilterAggregation(And,
		F{Colname: "A", Comparator: series.Greater, Comparando: 2},
		F{Colname: "B", Comparator: series.Eq, Comparando: "y"},
	)
	if got, want := and.Records(), [][]string{
		{"A", "B"}, {"4", "y"}, {"6", "y"},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("and filter: got %v want %v", got, want)
	}

	// Chained Filter calls act as And between calls.
	chained := df.Filter(F{Colname: "A", Comparator: series.LessEq, Comparando: 4}).
		Filter(F{Colname: "B", Comparator: series.Eq, Comparando: "x"})
	if got, want := chained.Records(), [][]string{
		{"A", "B"}, {"1", "x"}, {"3", "x"},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("chained filter: got %v want %v", got, want)
	}
}

func TestMilestone2_Arrange_Golden(t *testing.T) {
	df := New(
		series.New([]interface{}{2, 1, nil, 1, 2}, series.Int, "K"),
		series.New([]string{"e", "d", "c", "b", "a"}, series.String, "V"),
	)

	// Multi-key stable ordering: K ascending, then V ascending within ties.
	sorted := df.Arrange(Sort("K"), Sort("V"))
	if got, want := sorted.Records(), [][]string{
		{"K", "V"}, {"1", "b"}, {"1", "d"}, {"2", "a"}, {"2", "e"}, {"NaN", "c"},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("multi-key arrange: got %v want %v", got, want)
	}

	// Reverse primary key keeps missing rows last.
	rev := df.Arrange(RevSort("K"))
	got := rev.Col("K").Records()
	want := []string{"2", "2", "1", "1", "NaN"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("reverse arrange: got %v want %v", got, want)
	}
}

func TestMilestone2_Join_TypedAndStringPathsAgree(t *testing.T) {
	a := New(
		series.New([]int{1, 2, 3}, series.Int, "K"),
		series.New([]string{"a1", "a2", "a3"}, series.String, "VA"),
	)
	b := New(
		series.New([]int{2, 3, 4}, series.Int, "K"),
		series.New([]string{"b2", "b3", "b4"}, series.String, "VB"),
	)

	// Single Int key: typed hash path.
	typed := a.InnerJoin(b, "K")

	// Force the string-key path with a composite key (constant second key).
	a2 := a.Mutate(series.New([]int{7, 7, 7}, series.Int, "C"))
	b2 := b.Mutate(series.New([]int{7, 7, 7}, series.Int, "C"))
	stringPath := a2.InnerJoin(b2, "K", "C")

	typedRows := typed.Records()
	stringRows := make([][]string, len(typedRows))
	for i, row := range stringPath.Records() {
		// Drop the composite helper column (position varies by name rules);
		// keep K/VA/VB by name.
		var kept []string
		for j, name := range stringPath.Names() {
			if name == "K" || name == "VA" || name == "VB" {
				kept = append(kept, row[j])
			}
		}
		stringRows[i] = kept
	}
	typedKeep := make([][]string, len(typedRows))
	for i, row := range typedRows {
		var kept []string
		for j, name := range typed.Names() {
			if name == "K" || name == "VA" || name == "VB" {
				kept = append(kept, row[j])
			}
		}
		typedKeep[i] = kept
	}
	if !reflect.DeepEqual(typedKeep, stringRows) {
		t.Errorf("typed path %v != string path %v", typedKeep, stringRows)
	}

	// Golden content of the typed inner join.
	if got, want := typedKeep, [][]string{
		{"K", "VA", "VB"}, {"2", "a2", "b2"}, {"3", "a3", "b3"},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("inner join: got %v want %v", got, want)
	}
}

func TestMilestone2_Join_LeftOuterMissingSides(t *testing.T) {
	a := New(
		series.New([]int{1, 2}, series.Int, "K"),
		series.New([]string{"a1", "a2"}, series.String, "VA"),
	)
	b := New(
		series.New([]int{2, 3}, series.Int, "K"),
		series.New([]string{"b2", "b3"}, series.String, "VB"),
	)
	left := a.LeftJoin(b, "K")
	if got, want := left.Records(), [][]string{
		{"K", "VA", "VB"}, {"1", "a1", "NaN"}, {"2", "a2", "b2"},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("left join: got %v want %v", got, want)
	}
	outer := a.OuterJoin(b, "K")
	if got, want := outer.Records(), [][]string{
		{"K", "VA", "VB"}, {"1", "a1", "NaN"}, {"2", "a2", "b2"}, {"3", "NaN", "b3"},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("outer join: got %v want %v", got, want)
	}
}

func TestMilestone2_ApplyBatch_Golden(t *testing.T) {
	df := New(
		series.New([]float64{1, 2, 3}, series.Float, "A"),
		series.New([]int{10, 20, 30}, series.Int, "B"),
	)
	double := BatchTransform(func(cols []series.Series) ([]series.Series, error) {
		out := make([]series.Series, len(cols))
		for i, c := range cols {
			switch c.Type() {
			case series.Float:
				out[i] = series.MapFloat64(c, func(v float64) float64 { return v * 2 })
			case series.Int:
				out[i] = series.MapInt64(c, func(v int64) int64 { return v * 2 })
			default:
				out[i] = c
			}
		}
		return out, nil
	})
	got := df.ApplyBatch(double)
	if got.Err != nil {
		t.Fatalf("ApplyBatch: %v", got.Err)
	}
	if want := [][]string{
		{"A", "B"}, {"2.000000", "20"}, {"4.000000", "40"}, {"6.000000", "60"},
	}; !reflect.DeepEqual(got.Records(), want) {
		t.Errorf("ApplyBatch: got %v want %v", got.Records(), want)
	}

	// Errors from the transform become sticky frame errors.
	failing := df.ApplyBatch(func(cols []series.Series) ([]series.Series, error) {
		return nil, fmt.Errorf("boom")
	})
	if failing.Err == nil {
		t.Errorf("expected sticky error from failing transform")
	}

	// Sticky input errors short-circuit.
	withErr := DataFrame{Err: fmt.Errorf("upstream")}
	if got := withErr.ApplyBatch(double); got.Err == nil {
		t.Errorf("expected upstream error to propagate")
	}
}

func BenchmarkDataFrame_Filter(b *testing.B) {
	df := New(
		series.New(generateSeriesInts(100000), series.Int, "A"),
		series.New(generateSeriesInts(100000), series.Int, "B"),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.FilterAggregation(And,
			F{Colname: "A", Comparator: series.Greater, Comparando: 50},
			F{Colname: "B", Comparator: series.Less, Comparando: 99999999},
		)
	}
}

func BenchmarkDataFrame_InnerJoin(b *testing.B) {
	keys := generateSeriesInts(20000)
	a := New(
		series.New(keys, series.Int, "K"),
		series.New(generateSeriesInts(20000), series.Int, "VA"),
	)
	bData := New(
		series.New(keys, series.Int, "K"),
		series.New(generateSeriesInts(20000), series.Int, "VB"),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.InnerJoin(bData, "K")
	}
}

func generateSeriesInts(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = (i * 7919) % 100000
	}
	return out
}
