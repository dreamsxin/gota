package series

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAggregateByGroup_Float(t *testing.T) {
	s := New([]float64{1, 2, math.NaN(), 4, 5}, Float, "x")
	groupCodes := []int{0, 0, 0, 1, 1}

	agg, ok := s.AggregateByGroup(groupCodes, 2, true, true, true, true, true)
	if !ok {
		t.Fatal("AggregateByGroup(float) not supported")
	}
	// When mean is requested, a NaN in the group propagates into Sum/Mean
	// (documented NaN propagation semantics).
	if !floatsEqual(agg.Sum, []float64{math.NaN(), 9}) {
		t.Fatalf("Sum = %v, want [NaN 9]", agg.Sum)
	}
	if !floatsEqual(agg.Mean, []float64{math.NaN(), 4.5}) {
		t.Fatalf("Mean = %v, want [NaN 4.5]", agg.Mean)
	}
	if got, want := agg.Count, []float64{3, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Count = %v, want %v", got, want)
	}
	if !floatsEqual(agg.Max, []float64{2, 5}) {
		t.Fatalf("Max = %v, want [2 5]", agg.Max)
	}
	if !floatsEqual(agg.Min, []float64{1, 4}) {
		t.Fatalf("Min = %v, want [1 4]", agg.Min)
	}
}

func TestAggregateByGroup_Int(t *testing.T) {
	s := New([]int{10, 20, 30, 40}, Int, "n")
	groupCodes := []int{1, 0, 1, 0}

	agg, ok := s.AggregateByGroup(groupCodes, 2, true, true, true, true, true)
	if !ok {
		t.Fatal("AggregateByGroup(int) not supported")
	}
	if got, want := agg.Sum, []float64{60, 40}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Sum = %v, want %v", got, want)
	}
	if got, want := agg.Mean, []float64{30, 20}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Mean = %v, want %v", got, want)
	}
	if got, want := agg.Max, []float64{40, 30}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Max = %v, want %v", got, want)
	}
	if got, want := agg.Min, []float64{20, 10}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Min = %v, want %v", got, want)
	}
}

func TestAggregateByGroup_Unsupported(t *testing.T) {
	s := New([]string{"a", "b"}, String, "s")
	if _, ok := s.AggregateByGroup([]int{0, 1}, 2, true, true, true, true, true); ok {
		t.Fatal("AggregateByGroup(string) should report unsupported")
	}
}

func TestSumRowsAndByGroup(t *testing.T) {
	f := New([]float64{1.5, math.NaN(), 3.5}, Float, "f")
	if got := f.SumRows([]int{0, 1, 2}); got != 5 {
		t.Fatalf("float SumRows = %v, want 5", got)
	}
	if got := f.SumByGroup([]int{0, 0, 1}, 2); !reflect.DeepEqual(got, []float64{1.5, 3.5}) {
		t.Fatalf("float SumByGroup = %v, want [1.5 3.5]", got)
	}

	i := New([]int{1, 2, 3}, Int, "i")
	if got := i.SumRows([]int{0, 2}); got != 4 {
		t.Fatalf("int SumRows = %v, want 4", got)
	}
	if got := i.SumByGroup([]int{0, 0, 1}, 2); !reflect.DeepEqual(got, []float64{3, 3}) {
		t.Fatalf("int SumByGroup = %v, want [3 3]", got)
	}

	// Bool and String take the Element fallback path.
	b := New([]bool{true, false}, Bool, "b")
	if got := b.SumRows([]int{0, 1}); got != 1 {
		t.Fatalf("bool SumRows = %v, want 1", got)
	}
	st := New([]string{"2", "3"}, String, "s")
	if got := st.SumRows([]int{0, 1}); got != 5 {
		t.Fatalf("string SumRows = %v, want 5", got)
	}
	if got := st.SumByGroup([]int{0, 1}, 2); !reflect.DeepEqual(got, []float64{2, 3}) {
		t.Fatalf("string SumByGroup = %v, want [2 3]", got)
	}
}

func TestMeanRowsAndByGroup(t *testing.T) {
	i := New([]int{1, 2, 3, 4}, Int, "i")
	if got := i.MeanRows([]int{0, 1, 2}); got != 2 {
		t.Fatalf("MeanRows = %v, want 2", got)
	}
	if got := i.MeanRows(nil); got != 0 {
		t.Fatalf("MeanRows(empty) = %v, want 0", got)
	}
	if got := i.MeanByGroup([]int{0, 0, 1, 1}, 2); !reflect.DeepEqual(got, []float64{1.5, 3.5}) {
		t.Fatalf("MeanByGroup = %v, want [1.5 3.5]", got)
	}

	f := New([]float64{math.NaN(), 2, 4}, Float, "f")
	if got := f.MeanRows([]int{0, 1}); !math.IsNaN(got) {
		t.Fatalf("MeanRows with NaN = %v, want NaN", got)
	}
	if got := f.MeanByGroup([]int{0, 0, 1}, 2); !floatsEqual(got, []float64{math.NaN(), 4}) {
		t.Fatalf("MeanByGroup = %v, want [NaN 4]", got)
	}

	st := New([]string{"1", "2", "3"}, String, "s")
	if got := st.MeanRows([]int{0, 1, 2}); got != 2 {
		t.Fatalf("string MeanRows = %v, want 2", got)
	}
}

func TestMaxMinRowsAndByGroup(t *testing.T) {
	f := New([]float64{3, math.NaN(), 1, 5}, Float, "f")
	if got := f.MaxRows([]int{0, 2, 3}); got != 5 {
		t.Fatalf("MaxRows = %v, want 5", got)
	}
	if got := f.MinRows([]int{0, 2, 3}); got != 1 {
		t.Fatalf("MinRows = %v, want 1", got)
	}
	if got := f.MaxRows([]int{1, 2}); !math.IsNaN(got) {
		t.Fatalf("MaxRows starting on NaN = %v, want NaN", got)
	}

	maxs, ok := f.MaxByGroup([]int{0, 0, 1, 1}, 2)
	if !ok {
		t.Fatal("MaxByGroup(float) not supported")
	}
	// A NaN that is not the group's first value is skipped.
	if !floatsEqual(maxs, []float64{3, 5}) {
		t.Fatalf("MaxByGroup = %v, want [3 5]", maxs)
	}
	mins, ok := f.MinByGroup([]int{0, 0, 1, 1}, 2)
	if !ok {
		t.Fatal("MinByGroup(float) not supported")
	}
	if !floatsEqual(mins, []float64{3, 1}) {
		t.Fatalf("MinByGroup = %v, want [3 1]", mins)
	}

	i := New([]int{5, 2, 8}, Int, "i")
	if got := i.MaxRows([]int{0, 1, 2}); got != 8 {
		t.Fatalf("int MaxRows = %v, want 8", got)
	}
	if got := i.MinRows([]int{0, 1, 2}); got != 2 {
		t.Fatalf("int MinRows = %v, want 2", got)
	}
	if got, _ := i.MaxByGroup([]int{0, 1, 1}, 2); !reflect.DeepEqual(got, []float64{5, 8}) {
		t.Fatalf("int MaxByGroup = %v, want [5 8]", got)
	}
	if got, _ := i.MinByGroup([]int{0, 1, 1}, 2); !reflect.DeepEqual(got, []float64{5, 2}) {
		t.Fatalf("int MinByGroup = %v, want [5 2]", got)
	}

	if _, ok := New([]string{"a"}, String, "s").MaxByGroup([]int{0}, 1); ok {
		t.Fatal("MaxByGroup(string) should report unsupported")
	}
	if _, ok := New([]string{"a"}, String, "s").MinByGroup([]int{0}, 1); ok {
		t.Fatal("MinByGroup(string) should report unsupported")
	}

	st := New([]string{"a", "c", "b"}, String, "s")
	// String max ordering works, but Float() of a string is NaN.
	if got := st.MaxRows([]int{0, 1, 2}); !math.IsNaN(got) {
		t.Fatalf("string MaxRows = %v, want NaN", got)
	}
	if got := st.MinRows([]int{0, 1, 2}); !math.IsNaN(got) {
		t.Fatalf("string MinRows = %v, want NaN", got)
	}
}

func TestFactorize_Typed(t *testing.T) {
	table := []struct {
		name   string
		s      Series
		labels []string
		codes  []int
		counts []int
	}{
		{"string", New([]string{"b", "a", "b", ""}, String, "s"),
			[]string{"b", "a", ""}, []int{0, 1, 0, 2}, []int{2, 1, 1}},
		{"stringNA", New([]interface{}{"x", nil}, String, "s"),
			[]string{"x", "<nil>"}, []int{0, 1}, []int{1, 1}},
		{"int", New([]int{1, 2, 1}, Int, "i"),
			[]string{"1", "2"}, []int{0, 1, 0}, []int{2, 1}},
		{"bool", New([]bool{true, false, true}, Bool, "b"),
			[]string{"true", "false"}, []int{0, 1, 0}, []int{2, 1}},
		{"float", New([]float64{1.5, 2.5, 1.5}, Float, "f"),
			[]string{"1.5", "2.5"}, []int{0, 1, 0}, []int{2, 1}},
	}
	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			labels, codes, counts, ok := tc.s.Factorize()
			if !ok {
				t.Fatalf("Factorize(%s) not supported", tc.name)
			}
			if !reflect.DeepEqual(labels, tc.labels) {
				t.Fatalf("labels = %v, want %v", labels, tc.labels)
			}
			if !reflect.DeepEqual(codes, tc.codes) {
				t.Fatalf("codes = %v, want %v", codes, tc.codes)
			}
			if !reflect.DeepEqual(counts, tc.counts) {
				t.Fatalf("counts = %v, want %v", counts, tc.counts)
			}
		})
	}

	ts := New([]time.Time{time.Unix(1, 0)}, Time, "t")
	if _, _, _, ok := ts.Factorize(); ok {
		t.Fatal("Factorize(time) should report unsupported")
	}
}

func TestFactorizePair_Typed(t *testing.T) {
	strs := New([]string{"a", "b", "a"}, String, "l")
	ints := New([]int{1, 2, 1}, Int, "r")

	labels, codes, counts, ok := FactorizePair(strs, ints)
	if !ok {
		t.Fatal("FactorizePair(string,int) not supported")
	}
	if !reflect.DeepEqual(labels, []string{"a_1", "b_2"}) {
		t.Fatalf("labels = %v", labels)
	}
	if !reflect.DeepEqual(codes, []int{0, 1, 0}) {
		t.Fatalf("codes = %v", codes)
	}
	if !reflect.DeepEqual(counts, []int{2, 1}) {
		t.Fatalf("counts = %v", counts)
	}

	if _, _, _, ok := FactorizePair(ints, strs); !ok {
		t.Fatal("FactorizePair(int,string) not supported")
	}
	if _, _, _, ok := FactorizePair(strs, New([]string{"x", "y", "x"}, String, "x2")); !ok {
		t.Fatal("FactorizePair(string,string) not supported")
	}
	if _, _, _, ok := FactorizePair(ints, New([]int{9, 9, 9}, Int, "i2")); !ok {
		t.Fatal("FactorizePair(int,int) not supported")
	}

	short := New([]int{1}, Int, "short")
	if _, _, _, ok := FactorizePair(strs, short); ok {
		t.Fatal("FactorizePair with mismatched lengths should report unsupported")
	}
	floats := New([]float64{1.5}, Float, "f")
	if _, _, _, ok := FactorizePair(strs, floats); ok {
		t.Fatal("FactorizePair(string,float) should report unsupported")
	}
}

func TestSeriesShift_AllTypes(t *testing.T) {
	base := time.Unix(1000, 0)
	cases := []struct {
		name string
		s    Series
	}{
		{"string", New([]string{"a", "b", "c"}, String, "s")},
		{"int", New([]int{1, 2, 3}, Int, "i")},
		{"float", New([]float64{1, 2, 3}, Float, "f")},
		{"bool", New([]bool{true, false, true}, Bool, "b")},
		{"time", New([]time.Time{base, base.Add(time.Hour), base.Add(2 * time.Hour)}, Time, "t")},
	}
	// Compare shifted elements against the original series itself so the
	// assertion does not depend on Records formatting or the local timezone.
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			orig := c.s.Records()

			down := c.s.Shift(1)
			if down.Err != nil {
				t.Fatalf("Shift(1): %v", down.Err)
			}
			if !down.Elem(0).IsNA() {
				t.Fatalf("Shift(1)[0] = %v, want NA", down.Elem(0))
			}
			got := down.Records()
			for i := 1; i < len(got); i++ {
				if got[i] != orig[i-1] {
					t.Fatalf("Shift(1)[%d] = %q, want %q", i, got[i], orig[i-1])
				}
			}

			up := c.s.Shift(-1)
			got = up.Records()
			if !up.Elem(len(got) - 1).IsNA() {
				t.Fatalf("Shift(-1)[last] = %v, want NA", up.Elem(len(got)-1))
			}
			for i := 0; i < len(got)-1; i++ {
				if got[i] != orig[i+1] {
					t.Fatalf("Shift(-1)[%d] = %q, want %q", i, got[i], orig[i+1])
				}
			}
		})
	}

	if got := cases[1].s.Shift(0).Records(); !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
		t.Fatalf("Shift(0) should copy: %v", got)
	}
	// |periods| beyond length yields an all-NaN series.
	for _, want := range []string{"NaN"} {
		for _, rec := range cases[1].s.Shift(10).Records() {
			if rec != want {
				t.Fatalf("Shift(10) records = %v, want all NaN", cases[1].s.Shift(10).Records())
			}
		}
		for _, rec := range cases[1].s.Shift(-10).Records() {
			if rec != want {
				t.Fatalf("Shift(-10) records = %v, want all NaN", cases[1].s.Shift(-10).Records())
			}
		}
	}
	empty := New([]string{}, String, "e").Empty()
	if got := empty.Shift(1); got.Len() != 0 {
		t.Fatalf("Shift on empty series: len = %d, want 0", got.Len())
	}
}

func TestSeriesModeSkewKurt(t *testing.T) {
	m := New([]string{"b", "a", "b", "c", "b"}, String, "s")
	mode := m.Mode()
	if got := mode.Records(); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("Mode = %v, want [b]", got)
	}
	// Tie resolves to the lexicographically smallest mode.
	tie := New([]string{"z", "a", "z", "a"}, String, "s")
	if got := tie.Mode().Records(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("Mode tie = %v, want [a]", got)
	}
	allNA := New([]interface{}{nil, nil}, String, "s")
	if got := allNA.Mode(); got.Len() != 0 {
		t.Fatalf("Mode(all NaN) len = %d, want 0", got.Len())
	}

	sk := New([]float64{1, 2, 3, 4, 5}, Float, "f").Skew()
	if !almostEqual(sk, 0, 1e-9) {
		t.Fatalf("Skew symmetric = %v, want 0", sk)
	}
	if got := New([]float64{1, 2}, Float, "f").Skew(); !math.IsNaN(got) {
		t.Fatalf("Skew(n=2) = %v, want NaN", got)
	}
	if got := New([]float64{2, 2, 2}, Float, "f").Skew(); !math.IsNaN(got) {
		t.Fatalf("Skew(zero variance) = %v, want NaN", got)
	}

	// Normal sample kurtosis near 0; n=4 minimum.
	ku := New([]float64{1, 2, 3, 4}, Float, "f").Kurt()
	if math.IsNaN(ku) {
		t.Fatalf("Kurt(n=4) should be defined, got NaN")
	}
	if got := New([]float64{1, 2, 3}, Float, "f").Kurt(); !math.IsNaN(got) {
		t.Fatalf("Kurt(n=3) = %v, want NaN", got)
	}
	if got := New([]float64{3, 3, 3, 3}, Float, "f").Kurt(); !math.IsNaN(got) {
		t.Fatalf("Kurt(zero variance) = %v, want NaN", got)
	}
}

func TestSeriesAccessorsAndFill(t *testing.T) {
	s := New([]int{1, 2, 3}, Int, "x")
	if got := s.Error(); got != nil {
		t.Fatalf("Error on clean series = %v", got)
	}
	if got := s.Int64(); !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatalf("Int64 = %v", got)
	}
	if got := s.Str(); !strings.Contains(got, "Name: x") || !strings.Contains(got, "Type: int") {
		t.Fatalf("Str = %q, want series dump containing name and type", got)
	}
	if v := s.Val(1); v != 2 {
		t.Fatalf("Val(1) = %v, want 2", v)
	}

	// Fill grows the series to num rows by repeating values.
	fs := s.Copy()
	fs.Fill(5, 9)
	if got := fs.Records(); !reflect.DeepEqual(got, []string{"1", "2", "3", "9", "9"}) {
		t.Fatalf("Fill records = %v", got)
	}

	idx, err := ParseIndexes(3, []int{2, 0})
	if err != nil || !reflect.DeepEqual(idx, []int{2, 0}) {
		t.Fatalf("ParseIndexes = %v, %v", idx, err)
	}
}

func TestTimesDirectAndBatchConvertHelpers(t *testing.T) {
	ts := time.Unix(5, 0)
	s := TimesDirect([]time.Time{ts})
	if s.Len() != 1 || s.t != Time {
		t.Fatalf("TimesDirect: len=%d type=%v", s.Len(), s.t)
	}

	f := BatchConvertFloats([]float64{1.5, 2.5}, Int, "f")
	if got := f.Records(); !reflect.DeepEqual(got, []string{"1", "2"}) {
		t.Fatalf("BatchConvertFloats = %v", got)
	}
	b := BatchConvertBools([]bool{true, false}, String, "b")
	if got := b.Records(); !reflect.DeepEqual(got, []string{"true", "false"}) {
		t.Fatalf("BatchConvertBools = %v", got)
	}
}

func TestCategoricalRename(t *testing.T) {
	cat := NewCategorical([]string{"US", "UK"}, "country")
	renamed := cat.Rename("iso")
	if renamed.Name != "iso" {
		t.Fatalf("Rename: name = %q, want iso", renamed.Name)
	}
	if cat.Name != "country" {
		t.Fatalf("Rename should not mutate the receiver: name = %q", cat.Name)
	}
}

func TestEWMIgnoreNA(t *testing.T) {
	s := New([]float64{1, math.NaN(), 3}, Float, "x")
	r := s.EWM(2).IgnoreNA(true).Mean()
	if r.Len() != 3 {
		t.Fatalf("IgnoreNA Mean len = %d, want 3", r.Len())
	}
}

func almostEqual(a, b, tol float64) bool {
	return math.Abs(a-b) <= tol
}

// floatsEqual compares float slices treating NaN as equal to NaN.
func floatsEqual(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if math.IsNaN(a[i]) && math.IsNaN(b[i]) {
			continue
		}
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
