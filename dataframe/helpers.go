package dataframe

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
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

// parallelOrder returns the sort permutation for s using a parallel merge-sort
// across numWorkers() goroutines. NaN elements are pushed to the end.
// This is a drop-in replacement for series.Series.Order for large slices.
func parallelOrder(s series.Series, reverse bool) []int {
	n := s.Len()
	workers := numWorkers()
	if workers < 2 || n < 2 {
		return s.Order(reverse)
	}

	// Split into chunks, sort each chunk in parallel, then merge.
	chunkSize := (n + workers - 1) / workers
	chunks := make([]sortChunk, 0, workers)

	var wg sync.WaitGroup
	mu := sync.Mutex{}

	for start := 0; start < n; start += chunkSize {
		end := start + chunkSize
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			sub := s.Subset(makeRange(lo, hi))
			sorted := sub.Order(reverse)
			// Translate back to original indexes.
			for i, v := range sorted {
				sorted[i] = lo + v
			}
			mu.Lock()
			chunks = append(chunks, sortChunk{sorted})
			mu.Unlock()
		}(start, end)
	}
	wg.Wait()

	// k-way merge of sorted chunks.
	return kMerge(s, chunks, reverse)
}

// makeRange returns [lo, lo+1, ..., hi-1].
func makeRange(lo, hi int) []int {
	out := make([]int, hi-lo)
	for i := range out {
		out[i] = lo + i
	}
	return out
}

// sortChunk is a sorted index chunk used by parallelOrder / kMerge.
type sortChunk struct{ idx []int }

// heapEntry is a position pointer into a sortChunk, used by the k-way merge heap.
type heapEntry struct {
	chunkIdx int
	pos      int
}

// kMerge merges pre-sorted index chunks into a single sorted permutation.
func kMerge(s series.Series, chunks []sortChunk, reverse bool) []int {
	heads := make([]heapEntry, 0, len(chunks))
	for i, c := range chunks {
		if len(c.idx) > 0 {
			heads = append(heads, heapEntry{i, 0})
		}
	}

	less := func(a, b int) bool {
		ai := chunks[heads[a].chunkIdx].idx[heads[a].pos]
		bi := chunks[heads[b].chunkIdx].idx[heads[b].pos]
		ea := s.Elem(ai)
		eb := s.Elem(bi)
		if ea.IsNA() {
			return false
		}
		if eb.IsNA() {
			return true
		}
		if reverse {
			return ea.Greater(eb)
		}
		return ea.Less(eb)
	}

	heapLen := len(heads)
	for i := heapLen/2 - 1; i >= 0; i-- {
		heapSiftDown(heads, i, heapLen, less)
	}

	result := make([]int, 0, s.Len())
	for heapLen > 0 {
		top := heads[0]
		result = append(result, chunks[top.chunkIdx].idx[top.pos])
		top.pos++
		if top.pos >= len(chunks[top.chunkIdx].idx) {
			heads[0] = heads[heapLen-1]
			heapLen--
		} else {
			heads[0] = top
		}
		if heapLen > 0 {
			heapSiftDown(heads, 0, heapLen, less)
		}
	}
	return result
}

func heapSiftDown(h []heapEntry, i, n int, less func(a, b int) bool) {
	for {
		smallest := i
		l, r := 2*i+1, 2*i+2
		if l < n && less(l, smallest) {
			smallest = l
		}
		if r < n && less(r, smallest) {
			smallest = r
		}
		if smallest == i {
			break
		}
		h[i], h[smallest] = h[smallest], h[i]
		i = smallest
	}
}
