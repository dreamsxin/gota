package dataframe

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/dreamsxin/gota/series"
)

// ============================================================================
// Helper functions for string operations
// ============================================================================

// findInStringSlice finds the index of a string in a slice, returns -1 if not found
func findInStringSlice(str string, s []string) int {
	for i, e := range s {
		if e == str {
			return i
		}
	}
	return -1
}

// inIntSlice checks if an integer is in a slice
func inIntSlice(i int, is []int) bool {
	for _, v := range is {
		if v == i {
			return true
		}
	}
	return false
}

// isStrInStrSlice checks if a string is in a slice (boolean version)
func isStrInStrSlice(strSlice []string, str string) bool {
	return findInStringSlice(str, strSlice) != -1
}

// strIndexInStrSlice finds string index (alias for findInStringSlice for consistency)
func strIndexInStrSlice(strSlice []string, str string) int {
	return findInStringSlice(str, strSlice)
}

// ============================================================================
// Helper functions for column name management
// ============================================================================

// buildAggregatedColname creates a column name for aggregated columns
func buildAggregatedColname(c string, typ AggregationType) string {
	return fmt.Sprintf("%s_%s", c, typ)
}

// ============================================================================
// Helper functions for type detection
// ============================================================================

// findType auto-detects the Series type from string data
func findType(arr []string) (series.Type, error) {
	var hasFloats, hasInts, hasBools, hasTimes, hasStrings bool
	for _, str := range arr {
		if str == "" || str == "NaN" {
			continue
		}
		if _, err := strconv.Atoi(str); err == nil {
			hasInts = true
			continue
		}
		if _, err := strconv.ParseFloat(str, 64); err == nil {
			hasFloats = true
			continue
		}
		if _, err := time.ParseInLocation(time.RFC3339, str, time.Local); err == nil {
			hasTimes = true
			continue
		}
		if str == "true" || str == "false" {
			hasBools = true
			continue
		}
		hasStrings = true
	}

	switch {
	case hasStrings:
		return series.String, nil
	case hasBools:
		return series.Bool, nil
	case hasFloats:
		return series.Float, nil
	case hasInts:
		return series.Int, nil
	case hasTimes:
		return series.Time, nil
	default:
		return series.String, fmt.Errorf("couldn't detect type")
	}
}

// ============================================================================
// Helper functions for data transformation
// ============================================================================

// transposeRecords transposes a 2D string slice
func transposeRecords(x [][]string) [][]string {
	n := len(x)
	if n == 0 {
		return x
	}
	m := len(x[0])
	y := make([][]string, m)
	for i := 0; i < m; i++ {
		z := make([]string, n)
		for j := 0; j < n; j++ {
			z[j] = x[j][i]
		}
		y[i] = z
	}
	return y
}

// getDefaultElem returns a default element value for a given type
func getDefaultElem(tpe series.Type) series.Element {
	switch tpe {
	case series.String:
		return defaultStringElem
	case series.Int:
		return defaultIntElem
	case series.Float:
		return defaultFloatElem
	case series.Bool:
		return defaultBoolElem
	}
	return nil
}

// Default element values for pivot tables and other operations
var (
	defaultIntElem    = series.New([]int{0}, series.Int, "").Elem(0)
	defaultStringElem = series.New([]string{""}, series.String, "").Elem(0)
	defaultFloatElem  = series.New([]float64{0}, series.Float, "").Elem(0)
	defaultBoolElem   = series.New([]bool{false}, series.Bool, "").Elem(0)
)

// numWorkers returns the number of parallel workers to use for concurrent
// operations. It is at least 1 and at most GOMAXPROCS.
func numWorkers() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		return 1
	}
	return n
}
