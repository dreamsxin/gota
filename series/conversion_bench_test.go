package series_test

import (
	"math/rand"
	"testing"

	"github.com/dreamsxin/gota/series"
)

// Benchmark for optimized BatchConvert vs regular New
func BenchmarkBatchConvert_IntToString(b *testing.B) {
	rand.Seed(100)
	ints := generateInts(100000)

	b.Run("Regular_New", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			series.New(ints, series.String, "test")
		}
	})

	b.Run("BatchConvert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			series.BatchConvert(ints, series.String, "test")
		}
	})
}

func BenchmarkBatchConvert_FloatToString(b *testing.B) {
	rand.Seed(100)
	floats := generateFloats(100000)

	b.Run("Regular_New", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			series.New(floats, series.String, "test")
		}
	})

	b.Run("BatchConvert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			series.BatchConvert(floats, series.String, "test")
		}
	})
}

func BenchmarkBatchConvert_IntToFloat(b *testing.B) {
	rand.Seed(100)
	ints := generateInts(100000)

	b.Run("Regular_New", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			series.New(ints, series.Float, "test")
		}
	})

	b.Run("BatchConvert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			series.BatchConvert(ints, series.Float, "test")
		}
	})
}

// Benchmark for pool usage
func BenchmarkSeriesPool_Usage(b *testing.B) {
	b.Run("WithoutPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := series.Ints(generateInts(1000))
			_ = s.Mean()
		}
	})

	b.Run("WithPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := series.GetSeries()
			// Simulate using the series
			ints := generateInts(1000)
			*s = series.Ints(ints)
			_ = s.Mean()
			series.PutSeries(s)
		}
	})
}

// Benchmark for NewNoCopy vs New
func BenchmarkDataFrame_NewVsNewNoCopy(b *testing.B) {
	b.Skip("Requires dataframe import - add in implementation")
}
