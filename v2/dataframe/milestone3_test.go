package dataframe

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// Milestone 3 golden tests: byte-budget ScanCSV batching, GroupBy with a
// chain-local execution context, and Schema DType/FromSchema behavior.

func TestMilestone3_ScanCSV_ByteBudget(t *testing.T) {
	// Swap in a tiny budget so the budget, not the row count, decides the
	// chunk boundaries (RFC §9.3).
	prev := batchByteBudget
	batchByteBudget = 30
	defer func() { batchByteBudget = prev }()

	var sb strings.Builder
	sb.WriteString("A,B\n")
	for i := 0; i < 20; i++ {
		sb.WriteString("xx,yy\n") // ~6 bytes per row
	}

	var batches, rows int
	err := ScanCSV(strings.NewReader(sb.String()), 0, func(batch DataFrame) error {
		if batch.Err != nil {
			return batch.Err
		}
		batches++
		rows += batch.Nrow()
		return nil
	})
	if err != nil {
		t.Fatalf("ScanCSV: %v", err)
	}
	if rows != 20 {
		t.Fatalf("rows = %d, want 20", rows)
	}
	if batches < 3 {
		t.Fatalf("batches = %d, want >= 3 (byte budget must split the stream)", batches)
	}
}

func TestMilestone3_ScanCSV_RowLimitStillWins(t *testing.T) {
	prev := batchByteBudget
	batchByteBudget = BatchByteBudget
	defer func() { batchByteBudget = prev }()

	var sb strings.Builder
	sb.WriteString("A\n")
	for i := 0; i < 10; i++ {
		sb.WriteString("1\n")
	}
	var batches int
	err := ScanCSV(strings.NewReader(sb.String()), 4, func(batch DataFrame) error {
		batches++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanCSV: %v", err)
	}
	if batches != 3 {
		t.Fatalf("batches = %d, want 3 (4+4+2 rows)", batches)
	}
}

func TestMilestone3_GroupBy_ExecutionContext(t *testing.T) {
	df := New(
		series.New([]string{"a", "b", "a", "b", "a"}, series.String, "k1"),
		series.New([]string{"x", "x", "y", "y", "x"}, series.String, "k2"),
		series.New([]string{"m", "n", "m", "n", "m"}, series.String, "k3"),
		series.New([]int{1, 2, 3, 4, 5}, series.Int, "v"),
	)

	// Three string keys take the composite (string-key) path, which interns
	// keys through the context.
	ctx := series.NewExecutionContext()
	withCtx := df.WithExecutionContext(ctx).GroupBy("k1", "k2", "k3")
	got := withCtx.Aggregation([]AggregationType{Aggregation_SUM}, []string{"v"})
	if got.Err != nil {
		t.Fatalf("Aggregation: %v", got.Err)
	}
	if ctx.Len() == 0 {
		t.Fatal("composite GroupBy must intern keys through the context")
	}
	ctx.Release()
	if ctx.Len() != 0 {
		t.Fatal("released context must drop the pool in O(1)")
	}

	// Golden parity: interning must not change results.
	want := df.GroupBy("k1", "k2", "k3").Aggregation(
		[]AggregationType{Aggregation_SUM}, []string{"v"})
	if !reflect.DeepEqual(got.Records(), want.Records()) {
		t.Fatalf("interned GroupBy differs:\ngot  %v\nwant %v", got.Records(), want.Records())
	}
}

func TestMilestone3_ContextPropagates(t *testing.T) {
	df := New(series.New([]int{1, 2, 3}, series.Int, "v"))
	ctx := series.NewExecutionContext()
	chained := df.WithExecutionContext(ctx).Copy().Subset([]int{0, 2}).Select([]string{"v"})
	_ = chained
	// Shape-preserving ops keep the context attached; verify via GroupBy
	// interning on the derived frame.
	df2 := New(
		series.New([]string{"p", "q", "p"}, series.String, "a"),
		series.New([]string{"r", "r", "r"}, series.String, "b"),
		series.New([]string{"s", "s", "s"}, series.String, "c"),
		series.New([]int{1, 2, 3}, series.Int, "v"),
	).WithExecutionContext(ctx).Subset([]int{0, 1, 2})
	df2.GroupBy("a", "b", "c").Aggregation([]AggregationType{Aggregation_SUM}, []string{"v"})
	if ctx.Len() == 0 {
		t.Fatal("context must propagate through Subset into GroupBy")
	}
}

func TestMilestone3_Schema_DTypeAndFromSchema(t *testing.T) {
	dict := series.NewCategorical([]string{"US", "UK", "US", "DE"}, "country").ToDictionarySeries()
	df := New(
		dict,
		series.New([]int{1, 2, 3, 4}, series.Int, "n"),
	)
	s := df.Schema()

	f, ok := s.Field("country")
	if !ok || f.DType.Physical() != series.PhysDictionary {
		t.Fatalf("country field = %+v", f)
	}
	cats, ok := series.DictionaryCategories(f.DType)
	if !ok || len(cats) != 3 {
		t.Fatalf("categories = %v, %v", cats, ok)
	}
	if nf, _ := s.Field("n"); nf.DType != series.DTInt64 || nf.Nullable {
		t.Fatalf("n field = %+v", nf)
	}

	// FromSchema rebuilds a conforming empty frame, dictionary included.
	empty := FromSchema(s)
	if empty.Err != nil {
		t.Fatalf("FromSchema: %v", empty.Err)
	}
	if empty.Nrow() != 0 {
		t.Fatalf("FromSchema rows = %d", empty.Nrow())
	}
	es := empty.Schema()
	ef, ok := es.Field("country")
	if !ok || ef.DType.Physical() != series.PhysDictionary {
		t.Fatalf("empty country field = %+v", ef)
	}
	ecats, _ := series.DictionaryCategories(ef.DType)
	if !reflect.DeepEqual(ecats, cats) {
		t.Fatalf("empty categories = %v want %v", ecats, cats)
	}
	// Schemas compare equal including dictionary categories.
	if !s.Equal(es) {
		t.Fatal("original and rebuilt schemas must be Equal")
	}
}
