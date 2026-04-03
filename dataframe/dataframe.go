// Package dataframe provides an implementation of data frames and methods to
// subset, join, mutate, set, arrange, summarize, etc.
package dataframe

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/dreamsxin/gota/series"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"gonum.org/v1/gonum/mat"
)

// DataFrame is a data structure designed for operating on table like data (Such
// as Excel, CSV files, SQL table results...) where every column have to keep type
// integrity. As a general rule of thumb, variables are stored on columns where
// every row of a DataFrame represents an observation for each variable.
//
// On the real world, data is very messy and sometimes there are non measurements
// or missing data. For this reason, DataFrame has support for NaN elements and
// allows the most common data cleaning and mungling operations such as
// subsetting, filtering, type transformations, etc. In addition to this, this
// library provides the necessary functions to concatenate DataFrames (By rows or
// columns), different Join operations (Inner, Outer, Left, Right, Cross) and the
// ability to read and write from different formats (CSV/JSON).
type DataFrame struct {
	columns []series.Series
	ncols   int
	nrows   int

	// deprecated: Use Error() instead
	Err error
}

// New is the generic DataFrame constructor
//
// Performance note: This function copies all input Series.
// For better performance with large datasets, consider using
// NewNoCopy() if you can guarantee the input Series won't be modified.
func New(se ...series.Series) DataFrame {
	if len(se) == 0 {
		return DataFrame{Err: fmt.Errorf("empty DataFrame")}
	}

	// Pre-allocate columns slice with exact capacity
	columns := make([]series.Series, len(se))
	for i, s := range se {
		columns[i] = s.Copy()
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}

	// Fill DataFrame base structure
	df := DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// NewNoCopy creates a DataFrame without copying input Series.
// This is more memory-efficient but requires that the input Series
// are not modified after DataFrame creation.
//
// Use with caution: Only use when you control the input Series lifecycle.
//
// Example:
//
//	s1 := series.New([]int{1,2,3}, series.Int, "A")
//	s2 := series.New([]float64{1.0,2.0,3.0}, series.Float, "B")
//	df := dataframe.NewNoCopy(s1, s2) // No copy, more efficient
func NewNoCopy(se ...series.Series) DataFrame {
	if len(se) == 0 {
		return DataFrame{Err: fmt.Errorf("empty DataFrame")}
	}

	// Use input slices directly without copying
	columns := make([]series.Series, len(se))
	copy(columns, se)

	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}

	df := DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

func checkColumnsDimensions(se ...series.Series) (nrows, ncols int, err error) {
	ncols = len(se)
	nrows = -1
	if se == nil || ncols == 0 {
		err = fmt.Errorf("no Series given")
		return
	}
	for i, s := range se {
		if s.Err != nil {
			err = fmt.Errorf("error on series %d: %v", i, s.Err)
			return
		}
		if nrows == -1 {
			nrows = s.Len()
		}
		if nrows != s.Len() {
			err = fmt.Errorf("arguments have different dimensions")
			return
		}
	}
	return
}

// Copy returns a copy of the DataFrame
func (df DataFrame) Copy() DataFrame {
	copy := New(df.columns...)
	if df.Err != nil {
		copy.Err = df.Err
	}
	return copy
}

// String implements the Stringer interface for DataFrame
func (df DataFrame) String() (str string) {
	return df.print(true, true, true, true, 10, 70, "DataFrame")
}

// Returns error or nil if no error occured
func (df *DataFrame) Error() error {
	return df.Err
}

func (df DataFrame) Show() error {
	table := tablewriter.NewWriter(os.Stdout)

	records := df.Records()

	if len(records) <= 0 {
		return fmt.Errorf("no records to show")
	}
	table.SetHeader(records[0])

	table.AppendBulk(records[1:])

	table.Render()
	return nil
}

func (df DataFrame) Print(shortRows, shortCols, showDims, showTypes bool) (str string) {
	return df.print(shortRows, shortCols, showDims, showTypes, 10, 70, "DataFrame")
}

func (df DataFrame) print(
	shortRows, shortCols, showDims, showTypes bool,
	maxRows int,
	maxCharsTotal int,
	class string) (str string) {

	addRightPadding := func(s string, nchar int) string {
		if utf8.RuneCountInString(s) < nchar {
			return s + strings.Repeat(" ", nchar-utf8.RuneCountInString(s))
		}
		return s
	}

	addLeftPadding := func(s string, nchar int) string {
		if utf8.RuneCountInString(s) < nchar {
			return strings.Repeat(" ", nchar-utf8.RuneCountInString(s)) + s
		}
		return s
	}

	if df.Err != nil {
		str = fmt.Sprintf("%s error: %v", class, df.Err)
		return
	}
	nrows, ncols := df.Dims()
	if nrows == 0 || ncols == 0 {
		str = fmt.Sprintf("Empty %s", class)
		return
	}
	idx := make([]int, maxRows)
	for i := 0; i < len(idx); i++ {
		idx[i] = i
	}
	var records [][]string
	shortening := false
	if shortRows && nrows > maxRows {
		shortening = true
		df = df.Subset(idx)
		records = df.Records()
	} else {
		records = df.Records()
	}

	if showDims {
		str += fmt.Sprintf("[%dx%d] %s\n\n", nrows, ncols, class)
	}

	// Add the row numbers
	for i := 0; i < df.nrows+1; i++ {
		add := ""
		if i != 0 {
			add = strconv.Itoa(i-1) + ":"
		}
		records[i] = append([]string{add}, records[i]...)
	}
	if shortening {
		dots := make([]string, ncols+1)
		for i := 1; i < ncols+1; i++ {
			dots[i] = "..."
		}
		records = append(records, dots)
	}
	types := df.Types()
	typesrow := make([]string, ncols)
	for i := 0; i < ncols; i++ {
		typesrow[i] = fmt.Sprintf("<%v>", types[i])
	}
	typesrow = append([]string{""}, typesrow...)

	if showTypes {
		records = append(records, typesrow)
	}

	maxChars := make([]int, df.ncols+1)
	for i := 0; i < len(records); i++ {
		for j := 0; j < df.ncols+1; j++ {
			// Escape special characters
			records[i][j] = strconv.Quote(records[i][j])
			records[i][j] = records[i][j][1 : len(records[i][j])-1]

			// Detect maximum number of characters per column
			if len(records[i][j]) > maxChars[j] {
				maxChars[j] = utf8.RuneCountInString(records[i][j])
			}
		}
	}
	maxCols := len(records[0])
	var notShowing []string
	if shortCols {
		maxCharsCum := 0
		for colnum, m := range maxChars {
			maxCharsCum += m
			if maxCharsCum > maxCharsTotal {
				maxCols = colnum
				break
			}
		}
		notShowingNames := records[0][maxCols:]
		notShowingTypes := typesrow[maxCols:]
		notShowing = make([]string, len(notShowingNames))
		for i := 0; i < len(notShowingNames); i++ {
			notShowing[i] = fmt.Sprintf("%s %s", notShowingNames[i], notShowingTypes[i])
		}
	}
	for i := 0; i < len(records); i++ {
		// Add right padding to all elements
		records[i][0] = addLeftPadding(records[i][0], maxChars[0]+1)
		for j := 1; j < df.ncols; j++ {
			records[i][j] = addRightPadding(records[i][j], maxChars[j])
		}
		records[i] = records[i][0:maxCols]
		if shortCols && len(notShowing) != 0 {
			records[i] = append(records[i], "...")
		}
		// Create the final string
		str += strings.Join(records[i], " ")
		str += "\n"
	}
	if shortCols && len(notShowing) != 0 {
		var notShown string
		var notShownArr [][]string
		cum := 0
		i := 0
		for n, ns := range notShowing {
			cum += len(ns)
			if cum > maxCharsTotal {
				notShownArr = append(notShownArr, notShowing[i:n])
				cum = 0
				i = n
			}
		}
		if i < len(notShowing) {
			notShownArr = append(notShownArr, notShowing[i:])
		}
		for k, ns := range notShownArr {
			notShown += strings.Join(ns, ", ")
			if k != len(notShownArr)-1 {
				notShown += ","
			}
			notShown += "\n"
		}
		str += fmt.Sprintf("\nNot Showing: %s", notShown)
	}
	return str
}

// Subsetting, mutating and transforming DataFrame methods
// =======================================================

// Set will update the values of a DataFrame for the rows selected via indexes.
func (df DataFrame) Set(indexes series.Indexes, newvalues DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if newvalues.Err != nil {
		return DataFrame{Err: fmt.Errorf("argument has errors: %v", newvalues.Err)}
	}
	if df.ncols != newvalues.ncols {
		return DataFrame{Err: fmt.Errorf("different number of columns")}
	}
	columns := make([]series.Series, df.ncols)
	for i, s := range df.columns {
		columns[i] = s.Set(indexes, newvalues.columns[i])
		if columns[i].Err != nil {
			df = DataFrame{Err: fmt.Errorf("setting error on column %d: %v", i, columns[i].Err)}
			return df
		}
	}
	return df
}

// Subset returns a subset of the rows of the original DataFrame based on the
// Series subsetting indexes.
func (df DataFrame) Subset(indexes series.Indexes) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series, df.ncols)
	for i, column := range df.columns {
		s := column.Subset(indexes)
		columns[i] = s
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	return DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
}

func (df DataFrame) SliceRow(start, end int) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series, df.ncols)
	for i, column := range df.columns {
		s := column.Slice(start, end)
		columns[i] = s
		columns[i].Name = column.Name
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	return DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
}

// SelectIndexes are the supported indexes used for the DataFrame.Select method. Currently supported are:
//
//	int              // Matches the given index number
//	[]int            // Matches all given index numbers
//	[]bool           // Matches all columns marked as true
//	string           // Matches the column with the matching column name
//	[]string         // Matches all columns with the matching column names
//	Series [Int]     // Same as []int
//	Series [Bool]    // Same as []bool
//	Series [String]  // Same as []string
type SelectIndexes interface{}

// Select the given DataFrame columns
func (df DataFrame) Select(indexes SelectIndexes) DataFrame {
	if df.Err != nil {
		return df
	}
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return DataFrame{Err: fmt.Errorf("can't select columns: %v", err)}
	}
	columns := make([]series.Series, len(idx))
	for k, i := range idx {
		if i < 0 || i >= df.ncols {
			return DataFrame{Err: fmt.Errorf("can't select columns: index out of range")}
		}
		columns[k] = df.columns[i].Copy()
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// Drop the given DataFrame columns
func (df DataFrame) Drop(indexes SelectIndexes) DataFrame {
	if df.Err != nil {
		return df
	}
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return DataFrame{Err: fmt.Errorf("can't select columns: %v", err)}
	}
	var columns []series.Series
	for k, col := range df.columns {
		if !inIntSlice(k, idx) {
			columns = append(columns, col.Copy())
		}
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

const KEY_ERROR = "KEY_ERROR"

// GroupBy Group dataframe by columns
func (df DataFrame) GroupBy(colnames ...string) *Groups {
	if len(colnames) <= 0 {
		return nil
	}
	// Check that all colnames exist.
	for _, c := range colnames {
		if idx := findInStringSlice(c, df.Names()); idx == -1 {
			return &Groups{Err: fmt.Errorf("GroupBy: can't find column name: %s", c)}
		}
	}

	// Attach a hidden row-index column so Transform can restore original order.
	const idxCol = "__groupby_row_idx__"
	rowIdxs := make([]int, df.nrows)
	for i := range rowIdxs {
		rowIdxs[i] = i
	}
	dfWithIdx := df.Mutate(series.New(rowIdxs, series.Int, idxCol))

	groupDataFrame := make(map[string]DataFrame)
	groupSeries := make(map[string][]map[string]interface{})

	colTypes := map[string]series.Type{}
	for _, c := range dfWithIdx.columns {
		colTypes[c.Name] = c.Type()
	}

	for _, s := range dfWithIdx.Maps() {
		// Build a type-safe composite key compatible with the original format.
		key := ""
		for i, c := range colnames {
			sep := ""
			if i > 0 {
				sep = "_"
			}
			key = fmt.Sprintf("%s%s%v", key, sep, s[c])
		}
		groupSeries[key] = append(groupSeries[key], s)
	}

	for k, cMaps := range groupSeries {
		groupDataFrame[k] = LoadMaps(cMaps, WithTypes(colTypes))
	}
	return &Groups{groups: groupDataFrame, colnames: colnames, idxCol: idxCol, nrows: df.nrows}
}

// AggregationType Aggregation method type
type AggregationType int

//go:generate stringer -type=AggregationType -linecomment
const (
	Aggregation_MAX    AggregationType = iota + 1 // MAX
	Aggregation_MIN                               // MIN
	Aggregation_MEAN                              // MEAN
	Aggregation_MEDIAN                            // MEDIAN
	Aggregation_STD                               // STD
	Aggregation_SUM                               // SUM
	Aggregation_COUNT                             // COUNT
)

// Groups : structure generated by groupby
type Groups struct {
	groups      map[string]DataFrame
	colnames    []string
	idxCol      string // hidden row-index column name (set by GroupBy)
	nrows       int    // original DataFrame row count (for Transform)
	aggregation DataFrame
	Err         error
}

// Agg :Aggregate dataframe by aggregation type and aggregation column name
func (gps Groups) Agg(typ AggregationType, colnames []string) DataFrame {
	if gps.groups == nil {
		return DataFrame{Err: fmt.Errorf("Aggregation: input is nil")}
	}
	typs := []AggregationType{}
	for range colnames {
		typs = append(typs, typ)
	}
	return gps.Aggregation(typs, colnames)
}

// Aggregation :Aggregate dataframe by aggregation type and aggregation column name
func (gps Groups) Aggregation(typs []AggregationType, colnames []string) DataFrame {
	if gps.groups == nil {
		return DataFrame{Err: fmt.Errorf("Aggregation: input is nil")}
	}
	if len(typs) != len(colnames) {
		return DataFrame{Err: fmt.Errorf("Aggregation: len(typs) != len(colnames)")}
	}
	if len(gps.groups) == 0 {
		return DataFrame{Err: fmt.Errorf("Aggregation: no groups")}
	}
	dfMaps := make([]map[string]interface{}, 0, len(gps.groups))
	for _, df := range gps.groups {
		rows := df.Maps()
		if len(rows) == 0 {
			continue
		}
		targetMap := rows[0]
		curMap := make(map[string]interface{})
		// add columns of group by
		for _, c := range gps.colnames {
			if value, ok := targetMap[c]; ok {
				curMap[c] = value
			} else {
				return DataFrame{Err: fmt.Errorf("Aggregation: can't find column name: %s", c)}
			}
		}
		// Aggregation
		for i, c := range colnames {
			curSeries := df.Col(c)
			if curSeries.Err != nil {
				curMap[buildAggregatedColname(c, typs[i])] = nil
				continue
			}
			var value float64
			switch typs[i] {
			case Aggregation_MAX:
				value = curSeries.Max()
			case Aggregation_MEAN:
				value = curSeries.Mean()
			case Aggregation_MEDIAN:
				value = curSeries.Median()
			case Aggregation_MIN:
				value = curSeries.Min()
			case Aggregation_STD:
				value = curSeries.StdDev()
			case Aggregation_SUM:
				value = curSeries.Sum()
			case Aggregation_COUNT:
				value = float64(curSeries.Len())
			default:
				return DataFrame{Err: fmt.Errorf("Aggregation: method %s not found", typs[i])}
			}
			curMap[buildAggregatedColname(c, typs[i])] = value
		}
		dfMaps = append(dfMaps, curMap)
	}

	if len(dfMaps) == 0 {
		return DataFrame{Err: fmt.Errorf("Aggregation: no data after aggregation")}
	}

	// Infer column types from the first result row.
	colTypes := map[string]series.Type{}
	for k, v := range dfMaps[0] {
		switch v.(type) {
		case string:
			colTypes[k] = series.String
		case int, int16, int32, int64:
			colTypes[k] = series.Int
		case float32, float64:
			colTypes[k] = series.Float
		}
	}

	gps.aggregation = LoadMaps(dfMaps, WithTypes(colTypes))
	return gps.aggregation
}

// AggregationParallel is like Aggregation but processes groups concurrently.
// It is safe to use when the aggregation functions are pure (no shared state).
func (gps Groups) AggregationParallel(typs []AggregationType, colnames []string) DataFrame {
	if gps.groups == nil {
		return DataFrame{Err: fmt.Errorf("AggregationParallel: input is nil")}
	}
	if len(typs) != len(colnames) {
		return DataFrame{Err: fmt.Errorf("AggregationParallel: len(typs) != len(colnames)")}
	}
	if len(gps.groups) == 0 {
		return DataFrame{Err: fmt.Errorf("AggregationParallel: no groups")}
	}

	type result struct {
		m   map[string]interface{}
		err error
	}

	ch := make(chan result, len(gps.groups))
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup

	for _, df := range gps.groups {
		wg.Add(1)
		go func(df DataFrame) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			rows := df.Maps()
			if len(rows) == 0 {
				ch <- result{}
				return
			}
			targetMap := rows[0]
			curMap := make(map[string]interface{})
			for _, c := range gps.colnames {
				if value, ok := targetMap[c]; ok {
					curMap[c] = value
				} else {
					ch <- result{err: fmt.Errorf("AggregationParallel: can't find column %s", c)}
					return
				}
			}
			for i, c := range colnames {
				curSeries := df.Col(c)
				if curSeries.Err != nil {
					curMap[buildAggregatedColname(c, typs[i])] = nil
					continue
				}
				var value float64
				switch typs[i] {
				case Aggregation_MAX:
					value = curSeries.Max()
				case Aggregation_MEAN:
					value = curSeries.Mean()
				case Aggregation_MEDIAN:
					value = curSeries.Median()
				case Aggregation_MIN:
					value = curSeries.Min()
				case Aggregation_STD:
					value = curSeries.StdDev()
				case Aggregation_SUM:
					value = curSeries.Sum()
				case Aggregation_COUNT:
					value = float64(curSeries.Len())
				default:
					ch <- result{err: fmt.Errorf("AggregationParallel: method %s not found", typs[i])}
					return
				}
				curMap[buildAggregatedColname(c, typs[i])] = value
			}
			ch <- result{m: curMap}
		}(df)
	}
	go func() { wg.Wait(); close(ch) }()

	var dfMaps []map[string]interface{}
	for r := range ch {
		if r.err != nil {
			return DataFrame{Err: r.err}
		}
		if r.m != nil {
			dfMaps = append(dfMaps, r.m)
		}
	}
	if len(dfMaps) == 0 {
		return DataFrame{Err: fmt.Errorf("AggregationParallel: no data")}
	}
	colTypes := map[string]series.Type{}
	for k, v := range dfMaps[0] {
		switch v.(type) {
		case string:
			colTypes[k] = series.String
		case int, int16, int32, int64:
			colTypes[k] = series.Int
		case float32, float64:
			colTypes[k] = series.Float
		}
	}
	return LoadMaps(dfMaps, WithTypes(colTypes))
}

// GetGroups returns the grouped DataFrames created by GroupBy.
// The hidden row-index column is stripped from each group before returning.
func (g Groups) GetGroups() map[string]DataFrame {
	if g.idxCol == "" {
		return g.groups
	}
	// Strip the hidden index column from each group.
	clean := make(map[string]DataFrame, len(g.groups))
	for k, df := range g.groups {
		if df.ColIndex(g.idxCol) >= 0 {
			clean[k] = df.Drop(g.idxCol)
		} else {
			clean[k] = df
		}
	}
	return clean
}

// Apply applies a user-defined function to each group's DataFrame and returns
// the concatenated result. This is analogous to pandas groupby().apply().
// The hidden row-index column is stripped before passing each group to f.
func (gps Groups) Apply(f func(DataFrame) DataFrame) DataFrame {
	if gps.Err != nil {
		return DataFrame{Err: gps.Err}
	}
	var results []DataFrame
	for _, df := range gps.groups {
		// Strip hidden index column before passing to user function.
		if gps.idxCol != "" && df.ColIndex(gps.idxCol) >= 0 {
			df = df.Drop(gps.idxCol)
		}
		res := f(df)
		if res.Err != nil {
			return DataFrame{Err: res.Err}
		}
		results = append(results, res)
	}
	if len(results) == 0 {
		return DataFrame{Err: fmt.Errorf("GroupBy.Apply: no groups")}
	}
	out := results[0]
	for _, r := range results[1:] {
		out = out.Concat(r)
		if out.Err != nil {
			return out
		}
	}
	return out
}

// Transform applies a user-defined function to each group's column Series and
// returns a new Series aligned to the original DataFrame row order.
// This is analogous to pandas groupby().transform().
func (gps Groups) Transform(colname string, f func(series.Series) series.Series) (series.Series, error) {
	if gps.Err != nil {
		return series.Series{Err: gps.Err}, gps.Err
	}

	// If we have the hidden index column, restore original row order.
	if gps.idxCol != "" && gps.nrows > 0 {
		type indexedVal struct {
			idx int
			val series.Element
		}
		all := make([]indexedVal, 0, gps.nrows)

		for _, df := range gps.groups {
			col := df.Col(colname)
			if col.Err != nil {
				return series.Series{Err: col.Err}, col.Err
			}
			transformed := f(col)
			if transformed.Err != nil {
				return series.Series{Err: transformed.Err}, transformed.Err
			}
			idxSeries := df.Col(gps.idxCol)
			for i := 0; i < transformed.Len(); i++ {
				origIdx, err := idxSeries.Elem(i).Int()
				if err != nil {
					return series.Series{}, fmt.Errorf("Transform: row index error: %v", err)
				}
				all = append(all, indexedVal{idx: origIdx, val: transformed.Elem(i)})
			}
		}

		// Sort by original row index.
		sort.Slice(all, func(i, j int) bool { return all[i].idx < all[j].idx })

		// Determine output type from first non-nil element.
		outType := series.Float
		if len(all) > 0 {
			outType = all[0].val.Type()
		}
		out := series.New([]float64{}, outType, colname)
		for _, iv := range all {
			out.Append(iv.val)
		}
		out.Name = colname
		return out, nil
	}

	// Fallback (no index info): concat in map iteration order.
	var segs []series.Series
	for _, df := range gps.groups {
		col := df.Col(colname)
		if col.Err != nil {
			return series.Series{Err: col.Err}, col.Err
		}
		transformed := f(col)
		if transformed.Err != nil {
			return series.Series{Err: transformed.Err}, transformed.Err
		}
		segs = append(segs, transformed)
	}
	if len(segs) == 0 {
		return series.New([]float64{}, series.Float, colname), nil
	}
	out := segs[0]
	for _, seg := range segs[1:] {
		out = out.Concat(seg)
	}
	out.Name = colname
	return out, nil
}

// Rename changes the name of one of the columns of a DataFrame
func (df DataFrame) Rename(newname, oldname string) DataFrame {
	if df.Err != nil {
		return df
	}
	// Check that colname exist on dataframe
	colnames := df.Names()
	idx := findInStringSlice(oldname, colnames)
	if idx == -1 {
		return DataFrame{Err: fmt.Errorf("rename: can't find column name")}
	}

	copy := df.Copy()
	copy.columns[idx].Name = newname
	return copy
}

// CBind combines the columns of this DataFrame and dfb DataFrame.
func (df DataFrame) CBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}
	cols := append(df.columns, dfb.columns...)
	return New(cols...)
}

// RBind matches the column names of two DataFrames and returns combined
// rows from both of them.
func (df DataFrame) RBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}
	expandedSeries := make([]series.Series, df.ncols)
	for k, v := range df.Names() {
		idx := findInStringSlice(v, dfb.Names())
		if idx == -1 {
			return DataFrame{Err: fmt.Errorf("rbind: column names are not compatible")}
		}

		originalSeries := df.columns[k]
		addedSeries := dfb.columns[idx]
		newSeries := originalSeries.Concat(addedSeries)
		if err := newSeries.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("rbind: %v", err)}
		}
		expandedSeries[k] = newSeries
	}
	return New(expandedSeries...)
}

// Concat concatenates rows of two DataFrames like RBind, but also including
// unmatched columns.
func (df DataFrame) Concat(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}

	uniques := make(map[string]struct{})
	cols := []string{}
	for _, t := range []DataFrame{df, dfb} {
		for _, u := range t.Names() {
			if _, ok := uniques[u]; !ok {
				uniques[u] = struct{}{}
				cols = append(cols, u)
			}
		}
	}

	expandedSeries := make([]series.Series, len(cols))
	for k, v := range cols {
		aidx := findInStringSlice(v, df.Names())
		bidx := findInStringSlice(v, dfb.Names())

		// aidx and bidx must not be -1 at the same time.
		var a, b series.Series
		if aidx != -1 {
			a = df.columns[aidx]
		} else {
			bb := dfb.columns[bidx]
			a = series.New(make([]struct{}, df.nrows), bb.Type(), bb.Name)
		}
		if bidx != -1 {
			b = dfb.columns[bidx]
		} else {
			b = series.New(make([]struct{}, dfb.nrows), a.Type(), a.Name)
		}
		newSeries := a.Concat(b)
		if err := newSeries.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("concat: %v", err)}
		}
		expandedSeries[k] = newSeries
	}
	return New(expandedSeries...)
}

// Mutate changes a column of the DataFrame with the given Series or adds it as
// a new column if the column name does not exist.
func (df DataFrame) Mutate(ss ...series.Series) DataFrame {
	if df.Err != nil || len(ss) == 0 {
		return df
	}

	for i := 0; i < len(ss); i++ {
		if df.nrows != ss[i].Len() {
			return DataFrame{Err: fmt.Errorf("mutate: %s wrong dimensions", ss[i].Name)}
		}
	}

	df = df.Copy()
	// Check that colname exist on dataframe
	columns := df.columns
	for i := 0; i < len(ss); i++ {
		if idx := findInStringSlice(ss[i].Name, df.Names()); idx != -1 {
			columns[idx] = ss[i]
		} else {
			columns = append(columns, ss[i])
		}
	}

	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// F is the filtering structure
type F struct {
	Colidx     int
	Colname    string
	Comparator series.Comparator
	Comparando interface{}
}

// Filter will filter the rows of a DataFrame based on the given filters. All
// filters on the argument of a Filter call are aggregated as an OR operation
// whereas if we chain Filter calls, every filter will act as an AND operation
// with regards to the rest.
func (df DataFrame) Filter(filters ...F) DataFrame {
	return df.FilterAggregation(Or, filters...)
}

// Aggregation defines the filter aggregation
type Aggregation int

func (a Aggregation) String() string {
	switch a {
	case Or:
		return "or"
	case And:
		return "and"
	}
	return fmt.Sprintf("unknown aggragation %d", a)
}

const (
	// Or aggregates filters with logical or
	Or Aggregation = iota
	// And aggregates filters with logical and
	And
)

// FilterAggregation will filter the rows of a DataFrame based on the given filters. All
// filters on the argument of a Filter call are aggregated depending on the supplied
// aggregation.
func (df DataFrame) FilterAggregation(agg Aggregation, filters ...F) DataFrame {
	if df.Err != nil {
		return df
	}

	compResults := make([]series.Series, len(filters))
	for i, f := range filters {
		var idx int
		if f.Colname == "" {
			idx = f.Colidx
		} else {
			idx = findInStringSlice(f.Colname, df.Names())
			if idx < 0 {
				return DataFrame{Err: fmt.Errorf("filter: can't find column name")}
			}
		}
		res := df.columns[idx].Compare(f.Comparator, f.Comparando)
		if err := res.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		compResults[i] = res
	}

	if len(compResults) == 0 {
		return df.Copy()
	}

	res, err := compResults[0].Bool()
	if err != nil {
		return DataFrame{Err: fmt.Errorf("filter: %v", err)}
	}
	for i := 1; i < len(compResults); i++ {
		nextRes, err := compResults[i].Bool()
		if err != nil {
			return DataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		for j := 0; j < len(res); j++ {
			switch agg {
			case Or:
				res[j] = res[j] || nextRes[j]
			case And:
				res[j] = res[j] && nextRes[j]
			default:
				panic(agg)
			}
		}
	}
	return df.Subset(res)
}

// Order is the ordering structure
type Order struct {
	Colname string
	Reverse bool
}

// Sort return an ordering structure for regular column sorting sort.
func Sort(colname string) Order {
	return Order{colname, false}
}

// RevSort return an ordering structure for reverse column sorting.
func RevSort(colname string) Order {
	return Order{colname, true}
}

// Arrange sort the rows of a DataFrame according to the given Order
func (df DataFrame) Arrange(order ...Order) DataFrame {
	if df.Err != nil {
		return df
	}
	if len(order) == 0 {
		return DataFrame{Err: fmt.Errorf("rename: no arguments")}
	}

	// Check that all colnames exist before starting to sort
	for i := 0; i < len(order); i++ {
		colname := order[i].Colname
		if df.ColIndex(colname) == -1 {
			return DataFrame{Err: fmt.Errorf("colname %s doesn't exist", colname)}
		}
	}

	// Initialize the index that will be used to store temporary and final order
	// results.
	origIdx := make([]int, df.nrows)
	for i := 0; i < df.nrows; i++ {
		origIdx[i] = i
	}

	swapOrigIdx := func(newidx []int) {
		newOrigIdx := make([]int, len(newidx))
		for k, i := range newidx {
			newOrigIdx[k] = origIdx[i]
		}
		origIdx = newOrigIdx
	}

	suborder := origIdx
	for i := len(order) - 1; i >= 0; i-- {
		colname := order[i].Colname
		idx := df.ColIndex(colname)
		nextSeries := df.columns[idx].Subset(suborder)
		suborder = nextSeries.Order(order[i].Reverse)
		swapOrigIdx(suborder)
	}
	return df.Subset(origIdx)
}

// Capply applies the given function to the columns of a DataFrame
func (df DataFrame) Capply(f func(series.Series) series.Series) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series, df.ncols)
	for i, s := range df.columns {
		applied := f(s)
		applied.Name = s.Name
		columns[i] = applied
	}
	return New(columns...)
}

// CapplyParallel applies f to each column concurrently using up to GOMAXPROCS
// goroutines. Column order is preserved. The function f must be safe to call
// from multiple goroutines simultaneously.
func (df DataFrame) CapplyParallel(f func(series.Series) series.Series) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series, df.ncols)
	type result struct {
		idx int
		s   series.Series
	}
	ch := make(chan result, df.ncols)
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for i, s := range df.columns {
		wg.Add(1)
		go func(idx int, col series.Series) {
			defer wg.Done()
			sem <- struct{}{}
			applied := f(col)
			applied.Name = col.Name
			ch <- result{idx, applied}
			<-sem
		}(i, s)
	}
	go func() { wg.Wait(); close(ch) }()
	for r := range ch {
		columns[r.idx] = r.s
	}
	return New(columns...)
}

// Rapply applies the given function to the rows of a DataFrame. Prior to applying
// the function the elements of each row are cast to a Series of a specific
// type. In order of priority: String -> Float -> Int -> Bool. This casting also
// takes place after the function application to equalize the type of the columns.
func (df DataFrame) Rapply(f func(series.Series) series.Series) DataFrame {
	if df.Err != nil {
		return df
	}

	detectType := func(types []series.Type) series.Type {
		var hasStrings, hasFloats, hasInts, hasBools bool
		for _, t := range types {
			switch t {
			case series.String:
				hasStrings = true
			case series.Float:
				hasFloats = true
			case series.Int:
				hasInts = true
			case series.Bool:
				hasBools = true
			}
		}
		switch {
		case hasStrings:
			return series.String
		case hasBools:
			return series.Bool
		case hasFloats:
			return series.Float
		case hasInts:
			return series.Int
		default:
			panic("type not supported")
		}
	}

	// Detect row type prior to function application
	types := df.Types()
	rowType := detectType(types)

	// Create Element matrix
	elements := make([][]series.Element, df.nrows)
	rowlen := -1
	for i := 0; i < df.nrows; i++ {
		row := series.New(nil, rowType, "").Empty()
		for _, col := range df.columns {
			row.Append(col.Elem(i))
		}
		row = f(row)
		if row.Err != nil {
			return DataFrame{Err: fmt.Errorf("error applying function on row %d: %v", i, row.Err)}
		}

		if rowlen != -1 && rowlen != row.Len() {
			return DataFrame{Err: fmt.Errorf("error applying function: rows have different lengths")}
		}
		rowlen = row.Len()

		rowElems := make([]series.Element, rowlen)
		for j := 0; j < rowlen; j++ {
			rowElems[j] = row.Elem(j)
		}
		elements[i] = rowElems
	}

	// Cast columns if necessary
	columns := make([]series.Series, rowlen)
	for j := 0; j < rowlen; j++ {
		types := make([]series.Type, df.nrows)
		for i := 0; i < df.nrows; i++ {
			types[i] = elements[i][j].Type()
		}
		colType := detectType(types)
		s := series.New(nil, colType, "").Empty()
		for i := 0; i < df.nrows; i++ {
			s.Append(elements[i][j])
		}
		columns[j] = s
	}

	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// RapplyParallel applies f to each row concurrently using up to GOMAXPROCS
// goroutines. Row order is preserved. f must be safe to call concurrently.
func (df DataFrame) RapplyParallel(f func(series.Series) series.Series) DataFrame {
	if df.Err != nil {
		return df
	}

	detectType := func(types []series.Type) series.Type {
		var hasStrings, hasFloats, hasInts, hasBools bool
		for _, t := range types {
			switch t {
			case series.String:
				hasStrings = true
			case series.Float:
				hasFloats = true
			case series.Int:
				hasInts = true
			case series.Bool:
				hasBools = true
			}
		}
		switch {
		case hasStrings:
			return series.String
		case hasBools:
			return series.Bool
		case hasFloats:
			return series.Float
		case hasInts:
			return series.Int
		default:
			return series.String
		}
	}

	types := df.Types()
	rowType := detectType(types)

	type rowResult struct {
		idx   int
		elems []series.Element
		err   error
	}

	ch := make(chan rowResult, df.nrows)
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup

	for i := 0; i < df.nrows; i++ {
		wg.Add(1)
		go func(rowIdx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			row := series.New(nil, rowType, "").Empty()
			for _, col := range df.columns {
				row.Append(col.Elem(rowIdx))
			}
			row = f(row)
			if row.Err != nil {
				ch <- rowResult{idx: rowIdx, err: row.Err}
				return
			}
			elems := make([]series.Element, row.Len())
			for j := 0; j < row.Len(); j++ {
				elems[j] = row.Elem(j)
			}
			ch <- rowResult{idx: rowIdx, elems: elems}
		}(i)
	}
	go func() { wg.Wait(); close(ch) }()

	elements := make([][]series.Element, df.nrows)
	rowlen := -1
	for r := range ch {
		if r.err != nil {
			return DataFrame{Err: fmt.Errorf("RapplyParallel row %d: %v", r.idx, r.err)}
		}
		if rowlen == -1 {
			rowlen = len(r.elems)
		} else if rowlen != len(r.elems) {
			return DataFrame{Err: fmt.Errorf("RapplyParallel: rows have different lengths")}
		}
		elements[r.idx] = r.elems
	}
	if rowlen <= 0 {
		return df.Copy()
	}

	columns := make([]series.Series, rowlen)
	for j := 0; j < rowlen; j++ {
		colTypes := make([]series.Type, df.nrows)
		for i := 0; i < df.nrows; i++ {
			colTypes[i] = elements[i][j].Type()
		}
		colType := detectType(colTypes)
		s := series.New(nil, colType, "").Empty()
		for i := 0; i < df.nrows; i++ {
			s.Append(elements[i][j])
		}
		columns[j] = s
	}

	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	result := DataFrame{columns: columns, ncols: ncols, nrows: nrows}
	colnames := result.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		result.columns[i].Name = colname
	}
	return result
}

// LoadOption is the type used to configure the load of elements
type LoadOption func(*loadOptions)

type loadOptions struct {
	// Specifies which is the default type in case detectTypes is disabled.
	defaultType series.Type

	// If set, the type of each column will be automatically detected unless
	// otherwise specified.
	detectTypes bool

	// If set, the first row of the tabular structure will be used as column
	// names.
	hasHeader bool

	// The names to set as columns names.
	names []string

	// Defines which values are going to be considered as NaN when parsing from string.
	nanValues []string

	// Defines the csv delimiter
	delimiter rune

	// EnablesLazyQuotes
	lazyQuotes bool

	// Defines the comment delimiter
	comment rune

	// The types of specific columns can be specified via column name.
	types map[string]series.Type

	// Defines which col names are going to be skipped when load from stucts.
	skipColNames map[string]string

	// Defines which col idx are going to be skipped when load from slice.
	skipColIdxs map[int]int

	// sheet specifies the XLSX sheet name to read (used by ReadXLSX).
	sheet string
}

// DefaultType sets the defaultType option for loadOptions.
func DefaultType(t series.Type) LoadOption {
	return func(c *loadOptions) {
		c.defaultType = t
	}
}

// DetectTypes sets the detectTypes option for loadOptions.
func DetectTypes(b bool) LoadOption {
	return func(c *loadOptions) {
		c.detectTypes = b
	}
}

// HasHeader sets the hasHeader option for loadOptions.
func HasHeader(b bool) LoadOption {
	return func(c *loadOptions) {
		c.hasHeader = b
	}
}

// Names sets the names option for loadOptions.
func Names(names ...string) LoadOption {
	return func(c *loadOptions) {
		c.names = names
	}
}

// NaNValues sets the nanValues option for loadOptions.
func NaNValues(nanValues []string) LoadOption {
	return func(c *loadOptions) {
		c.nanValues = nanValues
	}
}

// WithTypes sets the types option for loadOptions.
func WithTypes(coltypes map[string]series.Type) LoadOption {
	return func(c *loadOptions) {
		c.types = coltypes
	}
}

// WithDelimiter sets the csv delimiter other than ',', for example '\t'
func WithDelimiter(b rune) LoadOption {
	return func(c *loadOptions) {
		c.delimiter = b
	}
}

// WithLazyQuotes sets csv parsing option to LazyQuotes
func WithLazyQuotes(b bool) LoadOption {
	return func(c *loadOptions) {
		c.lazyQuotes = b
	}
}

// WithComments sets the csv comment line detect to remove lines
func WithComments(b rune) LoadOption {
	return func(c *loadOptions) {
		c.comment = b
	}
}

// WithSkipCol
func WithSkipColNames(m map[string]string) LoadOption {
	return func(c *loadOptions) {
		c.skipColNames = m
	}
}

func WithSkipColIdxs(m map[int]int) LoadOption {
	return func(c *loadOptions) {
		c.skipColIdxs = m
	}
}

// LoadStructs creates a new DataFrame from arbitrary struct slices.
//
// LoadStructs will ignore unexported fields inside an struct. Note also that
// unless otherwise specified the column names will correspond with the name of
// the field.
//
// You can configure each field with the `dataframe:"name[,type]"` struct
// tag. If the name on the tag is the empty string `""` the field name will be
// used instead. If the name is `"-"` the field will be ignored.
//
// Examples:
//
//	// field will be ignored
//	field int
//
//	// Field will be ignored
//	Field int `dataframe:"-"`
//
//	// Field will be parsed with column name Field and type int
//	Field int
//
//	// Field will be parsed with column name `field_column` and type int.
//	Field int `dataframe:"field_column"`
//
//	// Field will be parsed with column name `field` and type string.
//	Field int `dataframe:"field,string"`
//
//	// Field will be parsed with column name `Field` and type string.
//	Field int `dataframe:",string"`
//
// If the struct tags and the given LoadOptions contradict each other, the later
// will have preference over the former.
func LoadStructs(i interface{}, options ...LoadOption) DataFrame {
	if i == nil {
		return DataFrame{Err: fmt.Errorf("load: can't create DataFrame from <nil> value")}
	}

	// Set the default load options
	cfg := loadOptions{
		defaultType: series.String,
		detectTypes: true,
		hasHeader:   true,
		nanValues:   []string{"NA", "NaN", "<nil>"},
	}

	// Set any custom load options
	for _, option := range options {
		option(&cfg)
	}

	tpy, val := reflect.TypeOf(i), reflect.ValueOf(i)
	switch tpy.Kind() {
	case reflect.Slice:
		if tpy.Elem().Kind() != reflect.Struct {
			return DataFrame{Err: fmt.Errorf(
				"load: type %s (%s %s) is not supported, must be []struct", tpy.Name(), tpy.Elem().Kind(), tpy.Kind())}
		}
		if val.Len() == 0 {
			return DataFrame{Err: fmt.Errorf("load: can't create DataFrame from empty slice")}
		}

		numFields := val.Index(0).Type().NumField()
		var columns []series.Series
		for j := 0; j < numFields; j++ {
			// Extract field metadata
			if !val.Index(0).Field(j).CanInterface() {
				continue
			}
			field := val.Index(0).Type().Field(j)
			fieldName := field.Name
			fieldType := field.Type.String()

			if cfg.skipColNames != nil {
				if _, ok := cfg.skipColNames[fieldName]; ok {
					continue
				}
			}

			// Process struct tags
			fieldTags := field.Tag.Get("dataframe")
			if fieldTags == "-" {
				continue
			}
			tagOpts := strings.Split(fieldTags, ",")
			if len(tagOpts) > 2 {
				return DataFrame{Err: fmt.Errorf("malformed struct tag on field %s: %s", fieldName, fieldTags)}
			}
			if len(tagOpts) > 0 {
				if name := strings.TrimSpace(tagOpts[0]); name != "" {
					fieldName = name
				}
				if len(tagOpts) == 2 {
					if tagType := strings.TrimSpace(tagOpts[1]); tagType != "" {
						fieldType = tagType
					}
				}
			}

			// Handle `types` option
			var t series.Type
			if cfgtype, ok := cfg.types[fieldName]; ok {
				t = cfgtype
			} else {
				// Handle `detectTypes` option
				if cfg.detectTypes {
					// Parse field type
					parsedType, err := parseType(fieldType)
					if err != nil {
						return DataFrame{Err: err}
					}
					t = parsedType
				} else {
					t = cfg.defaultType
				}
			}

			// Create Series for this field
			elements := make([]interface{}, val.Len())
			for i := 0; i < val.Len(); i++ {
				fieldValue := val.Index(i).Field(j)
				elements[i] = fieldValue.Interface()

				// Handle `nanValues` option
				if findInStringSlice(fmt.Sprint(elements[i]), cfg.nanValues) != -1 {
					elements[i] = nil
				}
			}

			// Handle `hasHeader` option
			if !cfg.hasHeader {
				tmp := make([]interface{}, 1)
				tmp[0] = fieldName
				elements = append(tmp, elements...)
				fieldName = ""
			}
			columns = append(columns, series.New(elements, t, fieldName))
		}
		return New(columns...)
	}
	return DataFrame{Err: fmt.Errorf(
		"load: type %s (%s) is not supported, must be []struct", tpy.Name(), tpy.Kind())}
}

func parseType(s string) (series.Type, error) {
	switch s {
	case "float", "float64", "float32":
		return series.Float, nil
	case "int", "int64", "int32", "int16", "int8":
		return series.Int, nil
	case "string":
		return series.String, nil
	case "bool":
		return series.Bool, nil
	case "time", "time.Time":
		return series.Time, nil
	}
	return "", fmt.Errorf("type (%s) is not supported", s)
}

// LoadRecords creates a new DataFrame based on the given records.
//
// Performance optimization: Pre-allocates column slices to reduce memory allocations.
func LoadRecords(records [][]string, options ...LoadOption) DataFrame {
	// Set the default load options
	cfg := loadOptions{
		defaultType: series.String,
		detectTypes: true,
		hasHeader:   true,
		nanValues:   []string{"NA", "NaN", "<nil>"},
	}

	// Set any custom load options
	for _, option := range options {
		option(&cfg)
	}

	if len(records) == 0 {
		return DataFrame{Err: fmt.Errorf("load records: empty DataFrame")}
	}
	if cfg.hasHeader && len(records) <= 1 {
		return DataFrame{Err: fmt.Errorf("load records: empty DataFrame")}
	}
	if cfg.names != nil && len(cfg.names) != len(records[0]) {
		if len(cfg.names) > len(records[0]) {
			return DataFrame{Err: fmt.Errorf("load records: too many column names")}
		}
		return DataFrame{Err: fmt.Errorf("load records: not enough column names")}
	}

	// Extract headers
	numCols := len(records[0])
	headers := make([]string, numCols)
	if cfg.hasHeader {
		headers = records[0]
		records = records[1:]
	}
	if cfg.names != nil {
		headers = cfg.names
	}

	numRows := len(records)
	types := make([]series.Type, numCols)
	rawcols := make([][]string, numCols)

	// Pre-allocate column data with exact capacity
	for i, colname := range headers {
		rawcol := make([]string, numRows)
		for j := 0; j < numRows; j++ {
			val := records[j][i]
			if findInStringSlice(val, cfg.nanValues) != -1 {
				val = "NaN"
			}
			rawcol[j] = val
		}
		rawcols[i] = rawcol

		t, ok := cfg.types[colname]
		if !ok {
			t = cfg.defaultType
			if cfg.detectTypes {
				if l, err := findType(rawcol); err == nil {
					t = l
				}
			}
		}
		types[i] = t
	}

	// Pre-allocate columns slice
	columns := make([]series.Series, numCols)
	for i, colname := range headers {
		col := series.New(rawcols[i], types[i], colname)
		if col.Err != nil {
			return DataFrame{Err: col.Err}
		}
		columns[i] = col
	}

	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}

	df := DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}

	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// LoadMaps creates a new DataFrame based on the given maps. This function assumes
// that every map on the array represents a row of observations.
func LoadMaps(maps []map[string]interface{}, options ...LoadOption) DataFrame {
	if len(maps) == 0 {
		return DataFrame{Err: fmt.Errorf("load maps: empty array")}
	}
	inStrSlice := func(i string, s []string) bool {
		for _, v := range s {
			if v == i {
				return true
			}
		}
		return false
	}
	// Detect all colnames
	var colnames []string
	for _, v := range maps {
		for k := range v {
			if exists := inStrSlice(k, colnames); !exists {
				colnames = append(colnames, k)
			}
		}
	}
	sort.Strings(colnames)
	records := make([][]string, len(maps)+1)
	records[0] = colnames
	for k, m := range maps {
		row := make([]string, len(colnames))
		for i, colname := range colnames {
			element := ""
			val, ok := m[colname]
			if ok {
				element = fmt.Sprint(val)
			}
			row[i] = element
		}
		records[k+1] = row
	}
	return LoadRecords(records, options...)
}

// LoadMatrix loads the given Matrix as a DataFrame
// TODO: Add Loadoptions
func LoadMatrix(mat Matrix) DataFrame {
	nrows, ncols := mat.Dims()
	columns := make([]series.Series, ncols)
	for i := 0; i < ncols; i++ {
		floats := make([]float64, nrows)
		for j := 0; j < nrows; j++ {
			floats[j] = mat.At(j, i)
		}
		columns[i] = series.Floats(floats)
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df := DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// ReadCSV reads a CSV file from a io.Reader and builds a DataFrame with the
// resulting records.
func ReadCSV(r io.Reader, options ...LoadOption) DataFrame {
	csvReader := csv.NewReader(r)
	cfg := loadOptions{
		delimiter:  ',',
		lazyQuotes: false,
		comment:    0,
	}
	for _, option := range options {
		option(&cfg)
	}

	csvReader.Comma = cfg.delimiter
	csvReader.LazyQuotes = cfg.lazyQuotes
	csvReader.Comment = cfg.comment

	records, err := csvReader.ReadAll()
	if err != nil {
		return DataFrame{Err: err}
	}
	return LoadRecords(records, options...)
}

// ReadJSON reads a JSON array from a io.Reader and builds a DataFrame with the
// resulting records.
func ReadJSON(r io.Reader, options ...LoadOption) DataFrame {
	var m []map[string]interface{}
	d := json.NewDecoder(r)
	d.UseNumber()
	err := d.Decode(&m)
	if err != nil {
		return DataFrame{Err: err}
	}
	return LoadMaps(m, options...)
}

// WriteOption is the type used to configure the writing of elements
type WriteOption func(*writeOptions)

type writeOptions struct {
	// Specifies whether the header is also written
	writeHeader bool
}

// WriteHeader sets the writeHeader option for writeOptions.
func WriteHeader(b bool) WriteOption {
	return func(c *writeOptions) {
		c.writeHeader = b
	}
}

// WriteCSV writes the DataFrame to the given io.Writer as a CSV file.
func (df DataFrame) WriteCSV(w io.Writer, options ...WriteOption) error {
	if df.Err != nil {
		return df.Err
	}

	// Set the default write options
	cfg := writeOptions{
		writeHeader: true,
	}

	// Set any custom write options
	for _, option := range options {
		option(&cfg)
	}

	records := df.Records()
	if !cfg.writeHeader {
		records = records[1:]
	}

	return csv.NewWriter(w).WriteAll(records)
}

// WriteJSON writes the DataFrame to the given io.Writer as a JSON array.
func (df DataFrame) WriteJSON(w io.Writer) error {
	if df.Err != nil {
		return df.Err
	}
	return json.NewEncoder(w).Encode(df.Maps())
}

// Internal state for implementing ReadHTML
type remainder struct {
	index int
	text  string
	nrows int
}

func readRows(trs []*html.Node) [][]string {
	rems := []remainder{}
	rows := [][]string{}
	for _, tr := range trs {
		xrems := []remainder{}
		row := []string{}
		index := 0
		text := ""
		for j, td := 0, tr.FirstChild; td != nil; j, td = j+1, td.NextSibling {
			if td.Type == html.ElementNode && td.DataAtom == atom.Td {

				for len(rems) > 0 {
					v := rems[0]
					if v.index > index {
						break
					}
					v, rems = rems[0], rems[1:]
					row = append(row, v.text)
					if v.nrows > 1 {
						xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
					}
					index++
				}

				rowspan, colspan := 1, 1
				for _, attr := range td.Attr {
					switch attr.Key {
					case "rowspan":
						if k, err := strconv.Atoi(attr.Val); err == nil {
							rowspan = k
						}
					case "colspan":
						if k, err := strconv.Atoi(attr.Val); err == nil {
							colspan = k
						}
					}
				}
				for c := td.FirstChild; c != nil; c = c.NextSibling {
					if c.Type == html.TextNode {
						text = strings.TrimSpace(c.Data)
					}
				}

				for k := 0; k < colspan; k++ {
					row = append(row, text)
					if rowspan > 1 {
						xrems = append(xrems, remainder{index, text, rowspan - 1})
					}
					index++
				}
			}
		}
		for j := 0; j < len(rems); j++ {
			v := rems[j]
			row = append(row, v.text)
			if v.nrows > 1 {
				xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
			}
		}
		rows = append(rows, row)
		rems = xrems
	}
	for len(rems) > 0 {
		xrems := []remainder{}
		row := []string{}
		for i := 0; i < len(rems); i++ {
			v := rems[i]
			row = append(row, v.text)
			if v.nrows > 1 {
				xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
			}
		}
		rows = append(rows, row)
		rems = xrems
	}
	return rows
}

func ReadHTML(r io.Reader, options ...LoadOption) []DataFrame {
	var err error
	var dfs []DataFrame
	var doc *html.Node
	var f func(*html.Node)

	doc, err = html.Parse(r)
	if err != nil {
		return []DataFrame{DataFrame{Err: err}}
	}

	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.DataAtom == atom.Table {
			trs := []*html.Node{}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && c.DataAtom == atom.Tbody {
					for cc := c.FirstChild; cc != nil; cc = cc.NextSibling {
						if cc.Type == html.ElementNode && (cc.DataAtom == atom.Th || cc.DataAtom == atom.Tr) {
							trs = append(trs, cc)
						}
					}
				}
			}

			df := LoadRecords(readRows(trs), options...)
			if df.Err == nil {
				dfs = append(dfs, df)
			}
			return
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}

	f(doc)
	return dfs
}

// Getters/Setters for DataFrame fields
// ====================================

// Names returns the name of the columns on a DataFrame.
func (df DataFrame) Names() []string {
	colnames := make([]string, df.ncols)
	for i, s := range df.columns {
		colnames[i] = s.Name
	}
	return colnames
}

// Types returns the types of the columns on a DataFrame.
func (df DataFrame) Types() []series.Type {
	coltypes := make([]series.Type, df.ncols)
	for i, s := range df.columns {
		coltypes[i] = s.Type()
	}
	return coltypes
}

// SetNames changes the column names of a DataFrame to the ones passed as an
// argument.
func (df DataFrame) SetNames(colnames ...string) error {
	if df.Err != nil {
		return df.Err
	}
	if len(colnames) != df.ncols {
		return fmt.Errorf("setting names: wrong dimensions")
	}
	for k, s := range colnames {
		df.columns[k].Name = s
	}
	return nil
}

// Dims retrieves the dimensions of a DataFrame.
func (df DataFrame) Dims() (int, int) {
	return df.Nrow(), df.Ncol()
}

// Nrow returns the number of rows on a DataFrame.
func (df DataFrame) Nrow() int {
	return df.nrows
}

// Ncol returns the number of columns on a DataFrame.
func (df DataFrame) Ncol() int {
	return df.ncols
}

func (df DataFrame) At(i, j int) float64 {
	return df.Elem(i, j).Float()
}

func (df DataFrame) T() mat.Matrix {
	return mat.Transpose{
		Matrix: df,
	}
}

// Col returns a copy of the Series with the given column name contained in the DataFrame.
func (df DataFrame) Col(colname string) series.Series {
	if df.Err != nil {
		return series.Series{Err: df.Err}
	}
	// Check that colname exist on dataframe
	idx := findInStringSlice(colname, df.Names())
	if idx < 0 {
		return series.Series{Err: fmt.Errorf("unknown column name")}
	}
	return df.columns[idx].Copy()
}

func (df DataFrame) FillNaN(colname string, value series.Series) DataFrame {
	if df.Err != nil {
		return df
	}
	// Check that colname exist on dataframe
	idx := findInStringSlice(colname, df.Names())
	if idx < 0 {
		s := series.New(value, value.Type(), colname)
		for i := 1; i < df.nrows; i++ {
			s.Append(value)
		}

		return df.Mutate(s)
	}
	df.columns[idx].FillNaN(value)
	return df
}

// rowKey builds a composite string key from all columns of a single row.
// Used by Duplicated and DropDuplicates.
func (df DataFrame) rowKey(i int) string {
	var sb strings.Builder
	for c, col := range df.columns {
		if c > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(col.Elem(i).String())
	}
	return sb.String()
}

// Duplicated returns a bool slice where true indicates the row is a duplicate
// of an earlier row.  Only the first occurrence is considered non-duplicate.
// If subset is non-empty, only the specified columns are used for comparison.
func (df DataFrame) Duplicated(subset ...string) []bool {
	result := make([]bool, df.nrows)
	seen := make(map[string]struct{}, df.nrows)

	// Determine which column indices to use.
	var colIdxs []int
	if len(subset) == 0 {
		for i := 0; i < df.ncols; i++ {
			colIdxs = append(colIdxs, i)
		}
	} else {
		for _, name := range subset {
			idx := df.ColIndex(name)
			if idx >= 0 {
				colIdxs = append(colIdxs, idx)
			}
		}
	}

	for i := 0; i < df.nrows; i++ {
		var sb strings.Builder
		for c, ci := range colIdxs {
			if c > 0 {
				sb.WriteByte('|')
			}
			sb.WriteString(df.columns[ci].Elem(i).String())
		}
		key := sb.String()
		if _, ok := seen[key]; ok {
			result[i] = true
		} else {
			seen[key] = struct{}{}
		}
	}
	return result
}

// DropDuplicates returns a new DataFrame with duplicate rows removed.
// Only the first occurrence of each unique row is kept.
// If subset is non-empty, only the specified columns are used to detect duplicates.
func (df DataFrame) DropDuplicates(subset ...string) DataFrame {
	dups := df.Duplicated(subset...)
	var keep []int
	for i, isDup := range dups {
		if !isDup {
			keep = append(keep, i)
		}
	}
	if len(keep) == 0 {
		return df.Subset([]int{})
	}
	return df.Subset(keep)
}

// NAHow controls the row-drop behaviour of DropNA.
type NAHow string

const (
	// NAHowAny drops a row if ANY of the examined columns is NaN.
	NAHowAny NAHow = "any"
	// NAHowAll drops a row only if ALL of the examined columns are NaN.
	NAHowAll NAHow = "all"
)

// DropNA returns a new DataFrame with rows removed according to NaN content.
//
//   - how = "any" (default): drop a row if any examined column is NaN.
//   - how = "all": drop a row only if all examined columns are NaN.
//   - subset: optional list of column names to examine; defaults to all columns.
func (df DataFrame) DropNA(how NAHow, subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}
	if how == "" {
		how = NAHowAny
	}

	// Resolve column indices to check.
	var colIdxs []int
	if len(subset) == 0 {
		for i := 0; i < df.ncols; i++ {
			colIdxs = append(colIdxs, i)
		}
	} else {
		for _, name := range subset {
			idx := df.ColIndex(name)
			if idx >= 0 {
				colIdxs = append(colIdxs, idx)
			}
		}
	}

	var keep []int
	for i := 0; i < df.nrows; i++ {
		nanCount := 0
		for _, ci := range colIdxs {
			if df.columns[ci].Elem(i).IsNA() {
				nanCount++
			}
		}
		switch how {
		case NAHowAny:
			if nanCount == 0 {
				keep = append(keep, i)
			}
		case NAHowAll:
			if nanCount < len(colIdxs) {
				keep = append(keep, i)
			}
		}
	}
	if len(keep) == 0 {
		return df.Subset([]int{})
	}
	return df.Subset(keep)
}

// NAFillStrategy selects the filling method for FillNAStrategy.
type NAFillStrategy string

const (
	// NAFillForward fills each NaN with the nearest preceding non-NaN value.
	NAFillForward NAFillStrategy = "ffill"
	// NAFillBackward fills each NaN with the nearest following non-NaN value.
	NAFillBackward NAFillStrategy = "bfill"
)

// FillNAStrategy fills NaN values in every column (or the specified subset)
// using the given strategy ("ffill" or "bfill").
func (df DataFrame) FillNAStrategy(strategy NAFillStrategy, subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}

	// Clone to avoid mutating the original.
	result := df.Copy()

	applies := func(name string) bool {
		if len(subset) == 0 {
			return true
		}
		for _, s := range subset {
			if s == name {
				return true
			}
		}
		return false
	}

	for i, col := range result.columns {
		if !applies(col.Name) {
			continue
		}
		switch strategy {
		case NAFillForward:
			result.columns[i] = col.FillNaNForward()
		case NAFillBackward:
			result.columns[i] = col.FillNaNBackward()
		}
	}
	return result
}

// FillNAStrategyLimit fills NaN values using ffill or bfill but limits the
// maximum number of consecutive NaN values that will be filled.
// limit <= 0 means no limit (same as FillNAStrategy).
func (df DataFrame) FillNAStrategyLimit(strategy NAFillStrategy, limit int, subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}
	result := df.Copy()
	applies := func(name string) bool {
		if len(subset) == 0 {
			return true
		}
		for _, s := range subset {
			if s == name {
				return true
			}
		}
		return false
	}
	for i, col := range result.columns {
		if !applies(col.Name) {
			continue
		}
		switch strategy {
		case NAFillForward:
			result.columns[i] = col.FillNaNForwardLimit(limit)
		case NAFillBackward:
			result.columns[i] = col.FillNaNBackwardLimit(limit)
		}
	}
	return result
}

// CumSum returns a new DataFrame where each numeric column is replaced by its
// cumulative sum. Non-numeric columns (String, Bool) are left unchanged.
func (df DataFrame) CumSum(subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}
	result := df.Copy()
	applies := func(name string) bool {
		if len(subset) == 0 {
			return true
		}
		for _, s := range subset {
			if s == name {
				return true
			}
		}
		return false
	}
	for i, col := range result.columns {
		if !applies(col.Name) {
			continue
		}
		switch col.Type() {
		case series.Int, series.Float:
			cs := col.CumSum()
			cs.Name = col.Name
			result.columns[i] = cs
		}
	}
	return result
}

// CumProd returns a new DataFrame where each numeric column is replaced by its
// cumulative product.
func (df DataFrame) CumProd(subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}
	result := df.Copy()
	applies := func(name string) bool {
		if len(subset) == 0 {
			return true
		}
		for _, s := range subset {
			if s == name {
				return true
			}
		}
		return false
	}
	for i, col := range result.columns {
		if !applies(col.Name) {
			continue
		}
		switch col.Type() {
		case series.Int, series.Float:
			cs := col.CumProd()
			cs.Name = col.Name
			result.columns[i] = cs
		}
	}
	return result
}

// Diff returns a new DataFrame where each numeric column is replaced by its
// first-order difference (or periods-order if periods != 1).
func (df DataFrame) Diff(periods int, subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}
	result := df.Copy()
	applies := func(name string) bool {
		if len(subset) == 0 {
			return true
		}
		for _, s := range subset {
			if s == name {
				return true
			}
		}
		return false
	}
	for i, col := range result.columns {
		if !applies(col.Name) {
			continue
		}
		switch col.Type() {
		case series.Int, series.Float:
			cs := col.Diff(periods)
			cs.Name = col.Name
			result.columns[i] = cs
		}
	}
	return result
}

// Shift shifts all column values by periods rows. Positive periods shifts
// down (inserts NaN at the top); negative shifts up (inserts NaN at the bottom).
// Non-numeric columns are also shifted (NaN becomes the zero/empty value for
// that type). subset limits which columns are shifted; others pass through.
//
// Example:
//
//	df.Shift(1)          // shift all columns down by 1
//	df.Shift(-2, "price") // shift "price" up by 2
func (df DataFrame) Shift(periods int, subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}
	if periods == 0 {
		return df.Copy()
	}
	applies := func(name string) bool {
		if len(subset) == 0 {
			return true
		}
		for _, s := range subset {
			if s == name {
				return true
			}
		}
		return false
	}
	result := df.Copy()
	n := df.nrows
	abs := periods
	if abs < 0 {
		abs = -abs
	}
	for ci, col := range result.columns {
		if !applies(col.Name) {
			continue
		}
		shifted := col.Empty()
		if periods > 0 {
			// Shift down: prepend `periods` NaNs, drop last `periods` elements.
			for i := 0; i < periods && i < n; i++ {
				shifted.Append(nil)
			}
			for i := 0; i < n-periods; i++ {
				shifted.Append(col.Elem(i))
			}
		} else {
			// Shift up: drop first `abs` elements, append `abs` NaNs.
			for i := abs; i < n; i++ {
				shifted.Append(col.Elem(i))
			}
			for i := 0; i < abs && i < n; i++ {
				shifted.Append(nil)
			}
		}
		shifted.Name = col.Name
		result.columns[ci] = shifted
	}
	return result
}

// PctChange returns a new DataFrame where each numeric column is replaced by
// its percentage change relative to the element periods positions prior.
func (df DataFrame) PctChange(periods int, subset ...string) DataFrame {
	if df.Err != nil {
		return df
	}
	result := df.Copy()
	applies := func(name string) bool {
		if len(subset) == 0 {
			return true
		}
		for _, s := range subset {
			if s == name {
				return true
			}
		}
		return false
	}
	for i, col := range result.columns {
		if !applies(col.Name) {
			continue
		}
		switch col.Type() {
		case series.Int, series.Float:
			cs := col.PctChange(periods)
			cs.Name = col.Name
			result.columns[i] = cs
		}
	}
	return result
}

// Corr returns the pairwise Pearson correlation matrix of all numeric columns
// as a new DataFrame. Rows and columns are labeled by column names.
func (df DataFrame) Corr() DataFrame {
	if df.Err != nil {
		return df
	}
	var numCols []series.Series
	for _, col := range df.columns {
		switch col.Type() {
		case series.Int, series.Float:
			numCols = append(numCols, col)
		}
	}
	n := len(numCols)
	if n == 0 {
		return DataFrame{Err: fmt.Errorf("Corr: no numeric columns")}
	}
	// Build result: first column is labels, then n columns of floats.
	names := make([]string, n)
	for i, c := range numCols {
		names[i] = c.Name
	}
	labelCol := series.Strings(names)
	labelCol.Name = ""
	cols := []series.Series{labelCol}
	for _, a := range numCols {
		vals := make([]float64, n)
		for j, b := range numCols {
			vals[j] = a.Corr(b)
		}
		s := series.Floats(vals)
		s.Name = a.Name
		cols = append(cols, s)
	}
	return New(cols...)
}

// Cov returns the pairwise sample covariance matrix of all numeric columns
// as a new DataFrame.
func (df DataFrame) Cov() DataFrame {
	if df.Err != nil {
		return df
	}
	var numCols []series.Series
	for _, col := range df.columns {
		switch col.Type() {
		case series.Int, series.Float:
			numCols = append(numCols, col)
		}
	}
	n := len(numCols)
	if n == 0 {
		return DataFrame{Err: fmt.Errorf("Cov: no numeric columns")}
	}
	names := make([]string, n)
	for i, c := range numCols {
		names[i] = c.Name
	}
	labelCol := series.Strings(names)
	labelCol.Name = ""
	cols := []series.Series{labelCol}
	for _, a := range numCols {
		vals := make([]float64, n)
		for j, b := range numCols {
			vals[j] = a.Cov(b)
		}
		s := series.Floats(vals)
		s.Name = a.Name
		cols = append(cols, s)
	}
	return New(cols...)
}

// Melt unpivots a DataFrame from wide format to long format, analogous to
// pandas DataFrame.melt().
//
// idVars: columns to keep as identifier variables (rows are repeated).
// valueVars: columns to unpivot; if empty, all non-id columns are used.
// varName: name of the new column that holds the original column names (default "variable").
// valueName: name of the new column that holds the cell values (default "value").
func (df DataFrame) Melt(idVars []string, valueVars []string, varName, valueName string) DataFrame {
	if df.Err != nil {
		return df
	}
	if varName == "" {
		varName = "variable"
	}
	if valueName == "" {
		valueName = "value"
	}
	// Resolve valueVars.
	if len(valueVars) == 0 {
		idSet := make(map[string]struct{}, len(idVars))
		for _, n := range idVars {
			idSet[n] = struct{}{}
		}
		for _, name := range df.Names() {
			if _, ok := idSet[name]; !ok {
				valueVars = append(valueVars, name)
			}
		}
	}
	// Validate columns.
	for _, n := range idVars {
		if df.ColIndex(n) < 0 {
			return DataFrame{Err: fmt.Errorf("melt: id column %q not found", n)}
		}
	}
	for _, n := range valueVars {
		if df.ColIndex(n) < 0 {
			return DataFrame{Err: fmt.Errorf("melt: value column %q not found", n)}
		}
	}

	nrows := df.nrows
	nval := len(valueVars)
	totalRows := nrows * nval

	// Build output id columns.
	outCols := make([]series.Series, len(idVars)+2)
	for k, idName := range idVars {
		src := df.Col(idName)
		// Repeat each element nval times, alternating rows.
		elems := make([]interface{}, totalRows)
		for i := 0; i < nrows; i++ {
			for j := 0; j < nval; j++ {
				elems[i*nval+j] = src.Elem(i).Val()
			}
		}
		outCols[k] = series.New(elems, src.Type(), idName)
	}

	// Build variable column.
	varElems := make([]string, totalRows)
	for i := 0; i < nrows; i++ {
		for j, vn := range valueVars {
			varElems[i*nval+j] = vn
		}
	}
	outCols[len(idVars)] = series.Strings(varElems)
	outCols[len(idVars)].Name = varName

	// Build value column — use Float for numeric, String otherwise.
	valElems := make([]interface{}, totalRows)
	for i := 0; i < nrows; i++ {
		for j, vn := range valueVars {
			valElems[i*nval+j] = df.Col(vn).Elem(i).Val()
		}
	}
	// Detect type: if all value columns are numeric, use Float; otherwise String.
	valType := series.Float
	for _, vn := range valueVars {
		switch df.Col(vn).Type() {
		case series.String, series.Bool, series.Time:
			valType = series.String
		}
	}
	outCols[len(idVars)+1] = series.New(valElems, valType, valueName)

	return New(outCols...)
}

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
// It uses a hash-join algorithm (O(n+m)) instead of a nested-loop (O(n*m)).
func (df DataFrame) InnerJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	jk, err := resolveJoinKeys(df, b, keys)
	if err != nil {
		return DataFrame{Err: err}
	}
	aCols := df.columns
	bCols := b.columns
	newCols := jk.newCols

	// Build hash table on b.
	ht := buildHashTable(b, jk.iKeysB)

	for i := 0; i < df.nrows; i++ {
		aKey := buildJoinKey(aCols, jk.iKeysA, i)
		for _, j := range ht[aKey] {
			appendMatchedRow(newCols, aCols, bCols, jk, i, j)
		}
	}
	return New(newCols...)
}

// LeftJoin returns a DataFrame containing the left join of two DataFrames.
// It uses a hash-join algorithm (O(n+m)) instead of a nested-loop (O(n*m)).
func (df DataFrame) LeftJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	jk, err := resolveJoinKeys(df, b, keys)
	if err != nil {
		return DataFrame{Err: err}
	}
	aCols := df.columns
	bCols := b.columns
	newCols := jk.newCols

	ht := buildHashTable(b, jk.iKeysB)

	for i := 0; i < df.nrows; i++ {
		aKey := buildJoinKey(aCols, jk.iKeysA, i)
		matches := ht[aKey]
		if len(matches) == 0 {
			appendLeftOnlyRow(newCols, aCols, jk, i)
		} else {
			for _, j := range matches {
				appendMatchedRow(newCols, aCols, bCols, jk, i, j)
			}
		}
	}
	return New(newCols...)
}

// RightJoin returns a DataFrame containing the right join of two DataFrames.
// It uses a hash-join algorithm (O(n+m)) instead of a nested-loop (O(n*m)).
func (df DataFrame) RightJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	jk, err := resolveJoinKeys(df, b, keys)
	if err != nil {
		return DataFrame{Err: err}
	}
	aCols := df.columns
	bCols := b.columns
	newCols := jk.newCols

	// Build hash table on a (left side).
	htA := make(map[string][]int, df.nrows)
	for i := 0; i < df.nrows; i++ {
		k := buildJoinKey(aCols, jk.iKeysA, i)
		htA[k] = append(htA[k], i)
	}

	// First pass: emit matched rows, preserving b's row order.
	for j := 0; j < b.nrows; j++ {
		bKey := buildJoinKey(bCols, jk.iKeysB, j)
		for _, i := range htA[bKey] {
			appendMatchedRow(newCols, aCols, bCols, jk, i, j)
		}
	}
	// Second pass: emit right-only rows (b rows with no match in a).
	for j := 0; j < b.nrows; j++ {
		bKey := buildJoinKey(bCols, jk.iKeysB, j)
		if len(htA[bKey]) == 0 {
			appendRightOnlyRow(newCols, bCols, jk, j)
		}
	}
	return New(newCols...)
}

// OuterJoin returns a DataFrame containing the outer join of two DataFrames.
// It uses a hash-join algorithm (O(n+m)) instead of a nested-loop (O(n*m)).
func (df DataFrame) OuterJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	jk, err := resolveJoinKeys(df, b, keys)
	if err != nil {
		return DataFrame{Err: err}
	}
	aCols := df.columns
	bCols := b.columns
	newCols := jk.newCols

	// Build hash table on b; track which b rows were matched.
	htB := buildHashTable(b, jk.iKeysB)
	bMatched := make([]bool, b.nrows)

	// Iterate a: emit matched rows; emit left-only rows for unmatched a rows.
	for i := 0; i < df.nrows; i++ {
		aKey := buildJoinKey(aCols, jk.iKeysA, i)
		matches := htB[aKey]
		if len(matches) == 0 {
			appendLeftOnlyRow(newCols, aCols, jk, i)
		} else {
			for _, j := range matches {
				appendMatchedRow(newCols, aCols, bCols, jk, i, j)
				bMatched[j] = true
			}
		}
	}
	// Emit right-only rows for b rows that had no match in a.
	for j := 0; j < b.nrows; j++ {
		if !bMatched[j] {
			appendRightOnlyRow(newCols, bCols, jk, j)
		}
	}
	return New(newCols...)
}

// CrossJoin returns a DataFrame containing the cross join of two DataFrames.
func (df DataFrame) CrossJoin(b DataFrame) DataFrame {
	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for i := 0; i < df.ncols; i++ {
		newCols = append(newCols, aCols[i].Empty())
	}
	for i := 0; i < b.ncols; i++ {
		newCols = append(newCols, bCols[i].Empty())
	}
	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			for ii := 0; ii < df.ncols; ii++ {
				elem := aCols[ii].Elem(i)
				newCols[ii].Append(elem)
			}
			for ii := 0; ii < b.ncols; ii++ {
				jj := ii + df.ncols
				elem := bCols[ii].Elem(j)
				newCols[jj].Append(elem)
			}
		}
	}
	return New(newCols...)
}

// ColIndex returns the index of the column with name `s`. If it fails to find the
// column it returns -1 instead.
func (df DataFrame) ColIndex(s string) int {
	for k, v := range df.Names() {
		if v == s {
			return k
		}
	}
	return -1
}

// Records return the string record representation of a DataFrame.
func (df DataFrame) Records() [][]string {
	var records [][]string
	records = append(records, df.Names())
	if df.ncols == 0 || df.nrows == 0 {
		return records
	}
	var tRecords [][]string
	for _, col := range df.columns {
		tRecords = append(tRecords, col.Records())
	}
	records = append(records, transposeRecords(tRecords)...)
	return records
}

// Maps return the array of maps representation of a DataFrame.
func (df DataFrame) Maps(funcs ...func(series.Type, interface{}) interface{}) []map[string]interface{} {
	maps := make([]map[string]interface{}, df.nrows)
	colnames := df.Names()
	for i := 0; i < df.nrows; i++ {
		m := make(map[string]interface{})
		for k, v := range colnames {
			s := df.columns[k]
			val := df.columns[k].Val(i)
			for _, f := range funcs {
				val = f(s.Type(), val)
			}
			m[v] = val
		}
		maps[i] = m
	}
	return maps
}

func (df DataFrame) GetRow(r int, funcs ...func(series.Type, interface{}) interface{}) (m map[string]interface{}) {
	if r >= df.nrows {
		return
	}
	m = make(map[string]interface{})
	colnames := df.Names()
	for k, v := range colnames {
		s := df.columns[k]
		val := df.columns[k].Val(r)
		for _, f := range funcs {
			val = f(s.Type(), val)
		}
		m[v] = val
	}
	return
}

// Elem returns the element on row `r` and column `c`. Will panic if the index is
// out of bounds.
func (df DataFrame) Elem(r, c int) series.Element {
	return df.columns[c].Elem(r)
}

// fixColnames assigns a name to the missing column names and makes it so that the
// column names are unique.
func fixColnames(colnames []string) {
	// Find duplicated and missing colnames
	dupnamesidx := make(map[string][]int)
	var missingnames []int
	for i := 0; i < len(colnames); i++ {
		a := colnames[i]
		if a == "" {
			missingnames = append(missingnames, i)
			continue
		}
		// for now, dupnamesidx contains the indices of *all* the columns
		// the columns with unique locations will be removed after this loop
		dupnamesidx[a] = append(dupnamesidx[a], i)
	}
	// NOTE: deleting a map key in a range is legal and correct in Go.
	for k, places := range dupnamesidx {
		if len(places) < 2 {
			delete(dupnamesidx, k)
		}
	}
	// Now: dupnameidx contains only keys that appeared more than once

	// Autofill missing column names
	counter := 0
	for _, i := range missingnames {
		proposedName := fmt.Sprintf("X%d", counter)
		for findInStringSlice(proposedName, colnames) != -1 {
			counter++
			proposedName = fmt.Sprintf("X%d", counter)
		}
		colnames[i] = proposedName
		counter++
	}

	// Sort map keys to make sure it always follows the same order
	var keys []string
	for k := range dupnamesidx {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Add a suffix to the duplicated colnames
	for _, name := range keys {
		idx := dupnamesidx[name]
		if name == "" {
			name = "X"
		}
		counter := 0
		for _, i := range idx {
			proposedName := fmt.Sprintf("%s_%d", name, counter)
			for findInStringSlice(proposedName, colnames) != -1 {
				counter++
				proposedName = fmt.Sprintf("%s_%d", name, counter)
			}
			colnames[i] = proposedName
			counter++
		}
	}
}

func parseSelectIndexes(l int, indexes SelectIndexes, colnames []string) ([]int, error) {
	var idx []int
	switch indexes.(type) {
	case []int:
		idx = indexes.([]int)
	case int:
		idx = []int{indexes.(int)}
	case []bool:
		bools := indexes.([]bool)
		if len(bools) != l {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case string:
		s := indexes.(string)
		i := findInStringSlice(s, colnames)
		if i < 0 {
			return nil, fmt.Errorf("can't select columns: column name %q not found", s)
		}
		idx = append(idx, i)
	case []string:
		xs := indexes.([]string)
		for _, s := range xs {
			i := findInStringSlice(s, colnames)
			if i < 0 {
				return nil, fmt.Errorf("can't select columns: column name %q not found", s)
			}
			idx = append(idx, i)
		}
	case series.Series:
		s := indexes.(series.Series)
		if err := s.Err; err != nil {
			return nil, fmt.Errorf("indexing error: new values has errors: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("indexing error: indexes contain NaN")
		}
		switch s.Type() {
		case series.Int:
			return s.Int()
		case series.Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("indexing error: %v", err)
			}
			return parseSelectIndexes(l, bools, colnames)
		case series.String:
			xs := indexes.(series.Series).Records()
			return parseSelectIndexes(l, xs, colnames)
		default:
			return nil, fmt.Errorf("indexing error: unknown indexing mode")
		}
	default:
		return nil, fmt.Errorf("indexing error: unknown indexing mode")
	}
	return idx, nil
}

// Matrix is an interface which is compatible with gonum's mat.Matrix interface
type Matrix interface {
	Dims() (r, c int)
	At(i, j int) float64
}

// Describe prints the summary statistics for each column of the dataframe
func (df DataFrame) Describe() DataFrame {
	labels := series.Strings([]string{
		"mean",
		"median",
		"stddev",
		"min",
		"25%",
		"50%",
		"75%",
		"max",
	})
	labels.Name = "column"

	ss := []series.Series{labels}

	for _, col := range df.columns {
		var newCol series.Series
		switch col.Type() {
		case series.String:
			newCol = series.New([]string{
				"-",
				"-",
				"-",
				col.MinStr(),
				"-",
				"-",
				"-",
				col.MaxStr(),
			},
				col.Type(),
				col.Name,
			)
		case series.Bool:
			fallthrough
		case series.Float:
			fallthrough
		case series.Int:
			newCol = series.New([]float64{
				col.Mean(),
				col.Median(),
				col.StdDev(),
				col.Min(),
				col.Quantile(0.25),
				col.Quantile(0.50),
				col.Quantile(0.75),
				col.Max(),
			},
				series.Float,
				col.Name,
			)
		case series.Time:
			// Show min and max timestamps; other stats are not applicable.
			minStr, maxStr := "-", "-"
			for i := 0; i < col.Len(); i++ {
				e := col.Elem(i)
				if e.IsNA() {
					continue
				}
				if minStr == "-" || e.String() < minStr {
					minStr = e.String()
				}
				if maxStr == "-" || e.String() > maxStr {
					maxStr = e.String()
				}
			}
			newCol = series.New(
				[]string{"-", "-", "-", minStr, "-", "-", "-", maxStr},
				series.String,
				col.Name,
			)
		}
		ss = append(ss, newCol)
	}

	ddf := New(ss...)
	return ddf
}

type PivotValue struct {
	Colname         string
	AggregationType AggregationType
}

// Pivot Create a dataframe like spreadsheet-style pivot table
func (df DataFrame) Pivot(rows []string, columns []string, values []PivotValue) DataFrame {
	err := df.checkPivotParams(rows, columns, values)
	if err != nil {
		return DataFrame{Err: err}
	}

	aggregatedDF := df.aggregateByRowsAndColumns(rows, columns, values)
	if aggregatedDF.Err != nil {
		return aggregatedDF
	}

	generatedColnames, generatedColtyps := df.buildGeneratedCols(aggregatedDF, columns, values)

	var rowGroups map[string]DataFrame
	if len(rows) == 0 {
		rowGroups = map[string]DataFrame{"": aggregatedDF}
	} else {
		rowGroups = aggregatedDF.GroupBy(rows...).groups
	}
	rowGroupsKeys := make([]string, 0, len(rowGroups))
	for key := range rowGroups {
		rowGroupsKeys = append(rowGroupsKeys, key)
	}
	sort.Strings(rowGroupsKeys)
	newColnames, newColElements := df.buildNewCols(rows, generatedColnames, len(rowGroupsKeys))

	rowIdx := 0
	for key, rowGroupDF := range rowGroups {
		rowIdx = strIndexInStrSlice(rowGroupsKeys, key)

		// fill row
		for colIdx, colname := range rows {
			newColElements[colIdx][rowIdx] = rowGroupDF.Col(colname).Elem(0)
		}
		// set default value for columns
		for colIdx := range generatedColnames {
			newColElements[colIdx+len(rows)][rowIdx] = getDefaultElem(generatedColtyps[colIdx])
		}

		// update value of columns
		for i := 0; i < rowGroupDF.Nrow(); i++ {
			colNames := make([]string, 0, len(columns))
			for _, col := range columns {
				colNames = append(colNames, rowGroupDF.Col(col).Elem(i).String())
			}

			for _, valueColumn := range values {
				aggregatedColname := buildAggregatedColname(valueColumn.Colname, valueColumn.AggregationType)
				newColNames := append(colNames, aggregatedColname)
				newColname := strings.Join(newColNames, "_")
				colIdx := strIndexInStrSlice(generatedColnames, newColname)
				newColElements[len(rows)+colIdx][rowIdx] = rowGroupDF.Col(aggregatedColname).Elem(i)
			}
		}
		rowIdx++
	}

	newColumnSlice := make([]series.Series, 0, len(newColnames))
	for i, colname := range newColnames {
		var typ series.Type
		if i < len(rows) {
			typ = df.Col(colname).Type()
		} else {
			typ = generatedColtyps[i-len(rows)]
		}
		newColumnSlice = append(newColumnSlice, series.New(newColElements[i], typ, colname))
	}

	return New(newColumnSlice...)
}

func (df *DataFrame) checkPivotParams(rows []string, columns []string, values []PivotValue) error {
	if len(values) == 0 {
		return fmt.Errorf("values cannot be empty")
	}

	usedColumnNames := make(map[string]bool, len(rows)+len(columns)+len(values))
	dfNames := df.Names()
	for _, colName := range rows {
		err := df.isValidColumnParam(usedColumnNames, colName, dfNames)
		if err != nil {
			return err
		}
	}
	for _, colName := range columns {
		err := df.isValidColumnParam(usedColumnNames, colName, dfNames)
		if err != nil {
			return err
		}
	}
	for _, col := range values {
		err := df.isValidColumnParam(usedColumnNames, col.Colname, dfNames)
		if err != nil {
			return err
		}
	}

	for _, value := range values {
		switch df.Col(value.Colname).Type() {
		case series.Int, series.Float:
			// only support numbers
			continue
		default:
			return fmt.Errorf("series cannot aggregate")
		}
	}
	return nil
}

func (df *DataFrame) isValidColumnParam(usedColumnNames map[string]bool, colName string, dfNames []string) error {
	if _, ok := usedColumnNames[colName]; ok {
		return fmt.Errorf("column %s cannot be used more than once", colName)
	}
	usedColumnNames[colName] = true
	if !isStrInStrSlice(dfNames, colName) {
		return fmt.Errorf("column %s not exist", colName)
	}
	return nil
}

func (df DataFrame) aggregateByRowsAndColumns(rows []string, columns []string, values []PivotValue) DataFrame {
	valueColnames := make([]string, 0, len(values))
	aggregationTypes := make([]AggregationType, 0, len(values))
	for _, value := range values {
		valueColnames = append(valueColnames, value.Colname)
		if value.AggregationType == 0 {
			// default AggregationType is Aggregation_SUM
			aggregationTypes = append(aggregationTypes, Aggregation_SUM)
		} else {
			aggregationTypes = append(aggregationTypes, value.AggregationType)
		}
	}

	var selectedColnames []string
	if len(rows) > 0 {
		selectedColnames = append(selectedColnames, rows...)
	}
	if len(columns) > 0 {
		selectedColnames = append(selectedColnames, columns...)
	}
	if len(selectedColnames) == 0 {
		t := Groups{groups: map[string]DataFrame{"": df}, colnames: valueColnames}
		return t.Aggregation(aggregationTypes, valueColnames)
	}

	groups := df.GroupBy(selectedColnames...)
	if groups.Err != nil {
		return DataFrame{Err: groups.Err}
	}
	return groups.Aggregation(aggregationTypes, valueColnames)
}

func (df DataFrame) buildGeneratedCols(aggregatedDF DataFrame, columns []string, values []PivotValue) ([]string, []series.Type) {
	if len(columns) == 0 {
		generatedColnames := make([]string, 0, len(values))
		generatedColtyps := make([]series.Type, 0, len(values))
		for _, value := range values {
			aggregatedValueColname := buildAggregatedColname(value.Colname, value.AggregationType)
			generatedColnames = append(generatedColnames, aggregatedValueColname)
			generatedColtyps = append(generatedColtyps, df.Col(value.Colname).Type())
		}
		return generatedColnames, generatedColtyps
	}

	columnGroups := aggregatedDF.GroupBy(columns...).groups
	generatedColElemsList := make([][]series.Element, 0, len(columnGroups))
	for _, columnGroupDf := range columnGroups {
		columnStrValues := make([]string, 0, len(columns))
		columnElems := make([]series.Element, 0, len(columns))
		for _, column := range columns {
			columnStrValues = append(columnStrValues, columnGroupDf.Col(column).Elem(0).String())
			columnElems = append(columnElems, columnGroupDf.Col(column).Elem(0))
		}
		generatedColElemsList = append(generatedColElemsList, columnElems)
	}

	// sort generatedColElemsList by elements
	sort.Slice(generatedColElemsList, func(i, j int) bool {
		generatedColElemsI := generatedColElemsList[i]
		generatedColElemsJ := generatedColElemsList[j]

		for idx := range generatedColElemsI {
			if generatedColElemsI[idx].Less(generatedColElemsJ[idx]) {
				return true
			} else if generatedColElemsI[idx].Greater(generatedColElemsJ[idx]) {
				return false
			} else {
				continue
			}
		}
		// all elements are equal
		return false
	})

	generatedColnames := make([]string, 0, len(generatedColElemsList)*len(values))
	generatedColtyps := make([]series.Type, 0, len(generatedColElemsList)*len(values))
	for _, generatedColElems := range generatedColElemsList {
		tmpColnames := make([]string, 0, len(generatedColElems))
		for _, elem := range generatedColElems {
			tmpColnames = append(tmpColnames, elem.String())
		}
		for _, value := range values {
			aggregatedValueColname := buildAggregatedColname(value.Colname, value.AggregationType)
			tmpColName := strings.Join(append(tmpColnames, aggregatedValueColname), "_")
			generatedColnames = append(generatedColnames, tmpColName)
			generatedColtyps = append(generatedColtyps, df.Col(value.Colname).Type())
		}
	}
	return generatedColnames, generatedColtyps
}

func (df DataFrame) buildNewCols(rows []string, generatedColnames []string, rowCnt int) ([]string, [][]series.Element) {
	newColnames := make([]string, 0, len(rows)+len(generatedColnames))
	if len(rows) > 0 {
		newColnames = append(newColnames, rows...)
	}
	if len(generatedColnames) > 0 {
		newColnames = append(newColnames, generatedColnames...)
	}

	newColElements := make([][]series.Element, len(newColnames))
	for i := range newColElements {
		newColElements[i] = make([]series.Element, rowCnt)
	}
	return newColnames, newColElements
}
