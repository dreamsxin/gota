// Package dataframe/index provides a lightweight explicit index system
// analogous to pandas Index / MultiIndex.
//
// An Index wraps an ordered set of labels and supports label-based row lookup,
// slicing, and alignment — without changing the core DataFrame API.
package dataframe

import (
	"fmt"
	"strings"

	"github.com/dreamsxin/gota/series"
)

// -----------------------------------------------------------------------
// Index – single-level label index
// -----------------------------------------------------------------------

// Index is an ordered list of string labels, similar to pandas.Index.
// It is attached to a DataFrame via DataFrame.SetIndex / DataFrame.WithIndex.
type Index struct {
	labels []string
	// reverse map: label -> list of positions (to handle non-unique labels).
	lookup map[string][]int
}

// NewIndex creates an Index from a string slice.
func NewIndex(labels []string) Index {
	idx := Index{
		labels: make([]string, len(labels)),
		lookup: make(map[string][]int, len(labels)),
	}
	for i, l := range labels {
		idx.labels[i] = l
		idx.lookup[l] = append(idx.lookup[l], i)
	}
	return idx
}

// Len returns the number of labels.
func (idx Index) Len() int { return len(idx.labels) }

// Label returns the label at position i.
func (idx Index) Label(i int) string {
	if i < 0 || i >= len(idx.labels) {
		return ""
	}
	return idx.labels[i]
}

// Labels returns a copy of all labels.
func (idx Index) Labels() []string {
	out := make([]string, len(idx.labels))
	copy(out, idx.labels)
	return out
}

// Get returns all row positions for the given label.
// Returns nil if the label is not found.
func (idx Index) Get(label string) []int {
	return idx.lookup[label]
}

// GetFirst returns the first row position for the given label, or -1 if missing.
func (idx Index) GetFirst(label string) int {
	positions := idx.lookup[label]
	if len(positions) == 0 {
		return -1
	}
	return positions[0]
}

// Slice returns the row positions for labels[start:end] (exclusive end label).
// Both start and end must exist in the index.
func (idx Index) Slice(start, end string) ([]int, error) {
	si := idx.GetFirst(start)
	if si < 0 {
		return nil, fmt.Errorf("index slice: start label %q not found", start)
	}
	ei := idx.GetFirst(end)
	if ei < 0 {
		return nil, fmt.Errorf("index slice: end label %q not found", end)
	}
	if si > ei {
		return nil, fmt.Errorf("index slice: start %q is after end %q", start, end)
	}
	out := make([]int, 0, ei-si+1)
	for i := si; i <= ei; i++ {
		out = append(out, i)
	}
	return out, nil
}

// Contains reports whether label exists in the index.
func (idx Index) Contains(label string) bool {
	_, ok := idx.lookup[label]
	return ok
}

// IsUnique reports whether all labels are distinct.
func (idx Index) IsUnique() bool {
	for _, v := range idx.lookup {
		if len(v) > 1 {
			return false
		}
	}
	return true
}

// String implements the Stringer interface.
func (idx Index) String() string {
	return "Index([" + strings.Join(idx.labels, ", ") + "])"
}

// -----------------------------------------------------------------------
// MultiIndex – multi-level label index
// -----------------------------------------------------------------------

// MultiIndex represents a hierarchical index with multiple levels, similar
// to pandas MultiIndex.  Each level is an Index; a row is identified by a
// tuple of one label per level.
type MultiIndex struct {
	levels []Index
	// codes[level][row] is the position within that level's label set.
	// We store the full tuple as a string key for fast lookup.
	keys []string // composite key per row
}

// NewMultiIndex builds a MultiIndex from a slice of label slices, one per level.
// All slices must have the same length (number of rows).
func NewMultiIndex(levels ...[]string) (MultiIndex, error) {
	if len(levels) == 0 {
		return MultiIndex{}, fmt.Errorf("NewMultiIndex: no levels provided")
	}
	nrows := len(levels[0])
	for i, lvl := range levels {
		if len(lvl) != nrows {
			return MultiIndex{}, fmt.Errorf("NewMultiIndex: level %d has %d labels, expected %d", i, len(lvl), nrows)
		}
	}

	mi := MultiIndex{
		levels: make([]Index, len(levels)),
		keys:   make([]string, nrows),
	}
	for i, lvl := range levels {
		mi.levels[i] = NewIndex(lvl)
	}
	// Build composite key for each row.
	parts := make([]string, len(levels))
	for row := 0; row < nrows; row++ {
		for l, lvl := range levels {
			parts[l] = lvl[row]
		}
		mi.keys[row] = strings.Join(parts, "\x00")
	}
	return mi, nil
}

// NLevels returns the number of levels.
func (mi MultiIndex) NLevels() int { return len(mi.levels) }

// Len returns the number of rows covered by the MultiIndex.
func (mi MultiIndex) Len() int { return len(mi.keys) }

// Level returns the Index for a given level number.
func (mi MultiIndex) Level(i int) Index {
	if i < 0 || i >= len(mi.levels) {
		return Index{}
	}
	return mi.levels[i]
}

// Get returns all row positions matching the given tuple of labels.
// The number of labels provided can be less than NLevels() for partial key lookup.
func (mi MultiIndex) Get(labels ...string) []int {
	var out []int
	prefix := strings.Join(labels, "\x00")
	for i, k := range mi.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, i)
		}
	}
	return out
}

// String implements the Stringer interface.
func (mi MultiIndex) String() string {
	nlevels := len(mi.levels)
	names := make([]string, nlevels)
	for i, lvl := range mi.levels {
		names[i] = fmt.Sprintf("Level%d%s", i, lvl.String())
	}
	return "MultiIndex(\n  " + strings.Join(names, ",\n  ") + "\n)"
}

// -----------------------------------------------------------------------
// IndexedDataFrame – DataFrame with an explicit Index
// -----------------------------------------------------------------------

// IndexedDataFrame wraps a DataFrame with an explicit row index.
// It provides label-based access methods analogous to pandas df.loc[].
type IndexedDataFrame struct {
	df    DataFrame
	index Index
}

// WithIndex attaches an Index to a DataFrame, returning an IndexedDataFrame.
// The index must have the same length as df.Nrow().
func (df DataFrame) WithIndex(idx Index) (IndexedDataFrame, error) {
	if idx.Len() != df.Nrow() {
		return IndexedDataFrame{}, fmt.Errorf("WithIndex: index length %d != nrows %d", idx.Len(), df.Nrow())
	}
	return IndexedDataFrame{df: df, index: idx}, nil
}

// WithColumnIndex uses the values of a named column as the row index label,
// then drops that column from the DataFrame.
func (df DataFrame) WithColumnIndex(colname string) (IndexedDataFrame, error) {
	col := df.Col(colname)
	if col.Err != nil {
		return IndexedDataFrame{}, fmt.Errorf("WithColumnIndex: %v", col.Err)
	}
	idx := NewIndex(col.Records())
	return IndexedDataFrame{df: df.Drop(colname), index: idx}, nil
}

// DataFrame returns the underlying DataFrame (without index column).
func (idf IndexedDataFrame) DataFrame() DataFrame { return idf.df }

// Index returns the attached Index.
func (idf IndexedDataFrame) Index() Index { return idf.index }

// Loc returns a DataFrame containing the rows matching the given label.
// Analogous to pandas df.loc[label].
func (idf IndexedDataFrame) Loc(label string) DataFrame {
	positions := idf.index.Get(label)
	if len(positions) == 0 {
		return DataFrame{Err: fmt.Errorf("loc: label %q not found", label)}
	}
	return idf.df.Subset(positions)
}

// LocSlice returns a DataFrame for all rows with labels in [start, end] (inclusive).
func (idf IndexedDataFrame) LocSlice(start, end string) DataFrame {
	positions, err := idf.index.Slice(start, end)
	if err != nil {
		return DataFrame{Err: err}
	}
	return idf.df.Subset(positions)
}

// ResetIndex returns a regular DataFrame with the index values inserted as the
// first column under the given name.
func (idf IndexedDataFrame) ResetIndex(colname string) DataFrame {
	if colname == "" {
		colname = "index"
	}
	idxCol := series.Strings(idf.index.Labels())
	idxCol.Name = colname
	cols := append([]series.Series{idxCol}, idf.df.columns...)
	return New(cols...)
}

// -----------------------------------------------------------------------
// MultiIndexedDataFrame
// -----------------------------------------------------------------------

// MultiIndexedDataFrame wraps a DataFrame with a MultiIndex.
type MultiIndexedDataFrame struct {
	df    DataFrame
	index MultiIndex
}

// WithMultiIndex attaches a MultiIndex to a DataFrame.
func (df DataFrame) WithMultiIndex(mi MultiIndex) (MultiIndexedDataFrame, error) {
	if mi.Len() != df.Nrow() {
		return MultiIndexedDataFrame{}, fmt.Errorf("WithMultiIndex: index length %d != nrows %d", mi.Len(), df.Nrow())
	}
	return MultiIndexedDataFrame{df: df, index: mi}, nil
}

// DataFrame returns the underlying DataFrame.
func (midf MultiIndexedDataFrame) DataFrame() DataFrame { return midf.df }

// MultiIndex returns the attached MultiIndex.
func (midf MultiIndexedDataFrame) MultiIndex() MultiIndex { return midf.index }

// Loc returns a DataFrame for all rows matching the given partial or full key.
func (midf MultiIndexedDataFrame) Loc(labels ...string) DataFrame {
	positions := midf.index.Get(labels...)
	if len(positions) == 0 {
		return DataFrame{Err: fmt.Errorf("loc: key %v not found", labels)}
	}
	return midf.df.Subset(positions)
}
