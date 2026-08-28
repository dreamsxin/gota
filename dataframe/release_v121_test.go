package dataframe

import (
	"reflect"
	"testing"

	"github.com/dreamsxin/gota/series"
)

func TestInnerJoinCompositeKeyEncoding(t *testing.T) {
	left := New(
		series.New([]string{"a", "a|string:b"}, series.String, "key1"),
		series.New([]string{"b|string:c", "c"}, series.String, "key2"),
		series.New([]int{1, 2}, series.Int, "left_value"),
	)
	right := New(
		series.New([]string{"a|string:b"}, series.String, "key1"),
		series.New([]string{"c"}, series.String, "key2"),
		series.New([]int{20}, series.Int, "right_value"),
	)

	joined := left.InnerJoin(right, "key1", "key2")
	if joined.Err != nil {
		t.Fatal(joined.Err)
	}
	if joined.Nrow() != 1 {
		t.Fatalf("collision-safe join rows: got %d want 1", joined.Nrow())
	}
	if got := joined.Col("left_value").Record(0); got != "2" {
		t.Fatalf("collision-safe join matched left value %s, want 2", got)
	}
}

func TestMultiIndexKeyEncodingHonorsLevelBoundaries(t *testing.T) {
	mi, err := NewMultiIndex(
		[]string{"U", "US", "U\x00S"},
		[]string{"x", "x", "x"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if got := mi.Get("U"); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("partial key U: got %v want [0]", got)
	}
	if got := mi.Get("U\x00S", "x"); !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("delimiter-containing full key: got %v want [2]", got)
	}
	if got := mi.Get("U", "x", "extra"); got != nil {
		t.Fatalf("too many levels: got %v want nil", got)
	}
}

func TestQueryLogicalPrecedenceAndParentheses(t *testing.T) {
	df := New(
		series.New([]int{5, 25, 30}, series.Int, "score"),
		series.New([]bool{false, false, true}, series.Bool, "active"),
		series.New([]string{"first", "second", "third"}, series.String, "name"),
	)

	precedence := df.Query("score < 10 OR score > 20 AND active == true")
	if precedence.Err != nil {
		t.Fatal(precedence.Err)
	}
	if got := precedence.Col("name").Records(); !reflect.DeepEqual(got, []string{"first", "third"}) {
		t.Fatalf("AND precedence: got %v want [first third]", got)
	}

	parenthesized := df.Query("(score < 10 OR score > 20) AND active == false")
	if parenthesized.Err != nil {
		t.Fatal(parenthesized.Err)
	}
	if got := parenthesized.Col("name").Records(); !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("parenthesized query: got %v want [first second]", got)
	}
}

func TestQueryQuotedValuesProtectLogicalWordsAndCommas(t *testing.T) {
	df := New(series.New([]string{"A AND B", "x,y", "plain"}, series.String, "label"))

	result := df.Query(`label in "A AND B","x,y"`)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if got := result.Col("label").Records(); !reflect.DeepEqual(got, []string{"A AND B", "x,y"}) {
		t.Fatalf("quoted query values: got %v", got)
	}

	invalid := df.Query("(label == plain")
	if invalid.Err == nil {
		t.Fatal("missing closing parenthesis should return an error")
	}
}
