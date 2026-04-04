package dataframe

import (
	"bytes"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// RenameAll
// -----------------------------------------------------------------------

func TestDataFrame_RenameAll(t *testing.T) {
	df := New(
		series.New([]int{1, 2}, series.Int, "a"),
		series.New([]int{3, 4}, series.Int, "b"),
		series.New([]int{5, 6}, series.Int, "c"),
	)
	out, err := df.RenameAll(map[string]string{"a": "x", "c": "z"})
	if err != nil {
		t.Fatal(err)
	}
	names := out.Names()
	if names[0] != "x" || names[1] != "b" || names[2] != "z" {
		t.Errorf("RenameAll: got %v want [x b z]", names)
	}
}

func TestDataFrame_RenameAll_NotFound(t *testing.T) {
	df := New(series.New([]int{1}, series.Int, "a"))
	_, err := df.RenameAll(map[string]string{"no_such": "x"})
	if err == nil {
		t.Error("RenameAll not found: expected error")
	}
}

// -----------------------------------------------------------------------
// AsCategorical
// -----------------------------------------------------------------------

func TestDataFrame_AsCategorical(t *testing.T) {
	df := New(
		series.New([]string{"US", "UK", "US", "DE"}, series.String, "country"),
		series.New([]int{1, 2, 3, 4}, series.Int, "id"),
	)
	cat, err := df.AsCategorical("country")
	if err != nil {
		t.Fatal(err)
	}
	if cat.NCategories() != 3 {
		t.Errorf("AsCategorical NCategories: got %d want 3", cat.NCategories())
	}
	if cat.Len() != 4 {
		t.Errorf("AsCategorical Len: got %d want 4", cat.Len())
	}
}

func TestDataFrame_AsCategorical_NonString(t *testing.T) {
	df := New(series.New([]int{1, 2, 3}, series.Int, "x"))
	_, err := df.AsCategorical("x")
	if err == nil {
		t.Error("AsCategorical non-string: expected error")
	}
}

// -----------------------------------------------------------------------
// WriteXLSXMultiSheet
// -----------------------------------------------------------------------

func TestWriteXLSXMultiSheet(t *testing.T) {
	df1 := New(series.New([]string{"a", "b"}, series.String, "name"))
	df2 := New(series.New([]int{1, 2, 3}, series.Int, "value"))

	var buf bytes.Buffer
	err := WriteXLSXMultiSheet(&buf,
		SheetData{"Names", df1},
		SheetData{"Values", df2},
	)
	if err != nil {
		t.Fatalf("WriteXLSXMultiSheet: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("WriteXLSXMultiSheet: empty output")
	}

	// Read back the first sheet.
	got1 := ReadXLSX(bytes.NewReader(buf.Bytes()), WithSheet("Names"))
	if got1.Err != nil {
		t.Fatalf("ReadXLSX Names: %v", got1.Err)
	}
	if got1.Nrow() != 2 {
		t.Errorf("Names sheet rows: got %d want 2", got1.Nrow())
	}

	// Read back the second sheet.
	got2 := ReadXLSX(bytes.NewReader(buf.Bytes()), WithSheet("Values"))
	if got2.Err != nil {
		t.Fatalf("ReadXLSX Values: %v", got2.Err)
	}
	if got2.Nrow() != 3 {
		t.Errorf("Values sheet rows: got %d want 3", got2.Nrow())
	}
}

func TestWriteXLSXMultiSheet_Empty(t *testing.T) {
	var buf bytes.Buffer
	err := WriteXLSXMultiSheet(&buf)
	if err == nil {
		t.Error("WriteXLSXMultiSheet empty: expected error")
	}
}

// -----------------------------------------------------------------------
// Benchmarks for new features
// -----------------------------------------------------------------------

func BenchmarkDataFrame_Query(b *testing.B) {
	df := New(
		series.New(makeFloats(10000), series.Float, "score"),
		series.New(makeStrings(10000, 5), series.String, "category"),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.Query("score > 50")
	}
}

func BenchmarkDataFrame_Shift(b *testing.B) {
	df := New(series.New(makeFloats(100000), series.Float, "x"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.Shift(1)
	}
}

func BenchmarkDataFrame_CapplyParallel(b *testing.B) {
	df := New(
		series.New(makeFloats(100000), series.Float, "A"),
		series.New(makeFloats(100000), series.Float, "B"),
		series.New(makeFloats(100000), series.Float, "C"),
	)
	double := func(s series.Series) series.Series {
		vals := s.Float()
		out := make([]float64, len(vals))
		for i, v := range vals {
			out[i] = v * 2
		}
		r := series.Floats(out)
		r.Name = s.Name
		return r
	}
	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			df.Capply(double)
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			df.CapplyParallel(double)
		}
	})
}

func BenchmarkCategorical_vs_StringSeries(b *testing.B) {
	// 100k rows, 5 distinct values.
	vals := makeStrings(100000, 5)

	b.Run("StringSeries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := series.Strings(vals)
			_ = s.ValueCounts()
		}
	})
	b.Run("Categorical", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cat := series.NewCategorical(vals, "v")
			_ = cat.ValueCounts()
		}
	})
}

// helpers
func makeFloats(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = float64(i % 100)
	}
	return out
}

func makeStrings(n, distinct int) []string {
	cats := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	out := make([]string, n)
	for i := range out {
		out[i] = cats[i%distinct]
	}
	return out
}

// -----------------------------------------------------------------------
// v2.3 Performance benchmarks
// -----------------------------------------------------------------------

func BenchmarkDataFrame_Query_StringCol(b *testing.B) {
	df := New(
		series.New(makeStrings(100000, 5), series.String, "country"),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.Query("country == A")
	}
}

func BenchmarkDataFrame_ValueCounts(b *testing.B) {
	df := New(
		series.New(makeStrings(100000, 5), series.String, "cat"),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.ValueCounts("cat", false, false)
	}
}

func BenchmarkDataFrame_Arrange(b *testing.B) {
	b.Run("10k", func(b *testing.B) {
		df := New(series.New(makeFloats(10000), series.Float, "v"))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.Arrange(Sort("v"))
		}
	})
	b.Run("200k", func(b *testing.B) {
		df := New(series.New(makeFloats(200000), series.Float, "v"))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.Arrange(Sort("v"))
		}
	})
}

func BenchmarkDataFrame_RapplyParallel(b *testing.B) {
	df := New(
		series.New(makeFloats(10000), series.Float, "A"),
		series.New(makeFloats(10000), series.Float, "B"),
	)
	f := func(s series.Series) series.Series { return s }
	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			df.Rapply(f)
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			df.RapplyParallel(f)
		}
	})
}
