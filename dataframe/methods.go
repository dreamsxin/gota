package dataframe

import (
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"

	"github.com/dreamsxin/gota/series"
)

// ============================================================================
// Display & Inspection Methods (Pandas-compatible)
// ============================================================================

// Head returns the first n rows of the DataFrame.
// Similar to pandas head().
//
// Example:
//   df.Head(5)  // First 5 rows
func (df DataFrame) Head(n int) DataFrame {
	if df.Err != nil {
		return df
	}
	if n <= 0 {
		return df.Subset([]int{})
	}
	if n >= df.nrows {
		return df.Copy()
	}
	indexes := make([]int, n)
	for i := 0; i < n; i++ {
		indexes[i] = i
	}
	return df.Subset(indexes)
}

// Tail returns the last n rows of the DataFrame.
// Similar to pandas tail().
//
// Example:
//   df.Tail(5)  // Last 5 rows
func (df DataFrame) Tail(n int) DataFrame {
	if df.Err != nil {
		return df
	}
	if n <= 0 {
		return df.Subset([]int{})
	}
	if n >= df.nrows {
		return df.Copy()
	}
	indexes := make([]int, n)
	start := df.nrows - n
	for i := 0; i < n; i++ {
		indexes[i] = start + i
	}
	return df.Subset(indexes)
}

// Info prints a concise summary of the DataFrame.
// Similar to pandas info().
//
// Output includes:
// - Number of rows and columns
// - Column names and non-null counts
// - Data types
// - Memory usage
func (df DataFrame) Info(w io.Writer) {
	if w == nil {
		w = io.Discard
	}
	
	if df.Err != nil {
		fmt.Fprintf(w, "DataFrame error: %v\n", df.Err)
		return
	}
	
	types := df.Types()
	colnames := df.Names()
	
	fmt.Fprintf(w, "<class 'dataframe.DataFrame'>\n")
	fmt.Fprintf(w, "Index: %d entries, 0 to %d\n", df.nrows, df.nrows-1)
	fmt.Fprintf(w, "Data columns (total %d columns):\n", df.ncols)
	
	// Calculate max column name length for formatting
	maxNameLen := 0
	for _, name := range colnames {
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}
	
	// Count non-null values per column
	for i, col := range df.columns {
		nonNull := 0
		for j := 0; j < df.nrows; j++ {
			if !col.Elem(j).IsNA() {
				nonNull++
			}
		}
		fmt.Fprintf(w, "   %-*s  %d non-null   %s\n", maxNameLen, colnames[i], nonNull, types[i])
	}
	
	// Estimate memory usage
	totalBytes := 0
	for _, col := range df.columns {
		switch col.Type() {
		case series.Int:
			totalBytes += df.nrows * 8 // int64
		case series.Float:
			totalBytes += df.nrows * 8 // float64
		case series.Bool:
			totalBytes += df.nrows * 1 // bool
		case series.String:
			totalBytes += df.nrows * 16 // string header + avg data
		case series.Time:
			totalBytes += df.nrows * 8 // time.Time
		}
	}
	
	if totalBytes > 1024*1024 {
		fmt.Fprintf(w, "memory usage: %.1f+ MB\n", float64(totalBytes)/(1024*1024))
	} else if totalBytes > 1024 {
		fmt.Fprintf(w, "memory usage: %.1f+ KB\n", float64(totalBytes)/1024)
	} else {
		fmt.Fprintf(w, "memory usage: %d+ bytes\n", totalBytes)
	}
}

// ============================================================================
// Missing Data Detection (Pandas-compatible)
// ============================================================================

// IsNull returns a DataFrame of boolean values indicating missing values.
// Similar to pandas isnull().
//
// Example:
//   mask := df.IsNull()
//   // Returns DataFrame with true where values are NaN/missing
func (df DataFrame) IsNull() DataFrame {
	if df.Err != nil {
		return df
	}
	
	columns := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		bools := make([]bool, df.nrows)
		for j := 0; j < df.nrows; j++ {
			bools[j] = col.Elem(j).IsNA()
		}
		columns[i] = series.Bools(bools)
		columns[i].Name = col.Name
	}
	
	return New(columns...)
}

// IsNA is an alias for IsNull.
// Similar to pandas isna().
func (df DataFrame) IsNA() DataFrame {
	return df.IsNull()
}

// NotNull returns a DataFrame of boolean values indicating non-missing values.
// Similar to pandas notnull().
//
// Example:
//   mask := df.NotNull()
//   // Returns DataFrame with true where values are NOT NaN/missing
func (df DataFrame) NotNull() DataFrame {
	if df.Err != nil {
		return df
	}
	
	columns := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		bools := make([]bool, df.nrows)
		for j := 0; j < df.nrows; j++ {
			bools[j] = !col.Elem(j).IsNA()
		}
		columns[i] = series.Bools(bools)
		columns[i].Name = col.Name
	}
	
	return New(columns...)
}

// NotNA is an alias for NotNull.
// Similar to pandas notna().
func (df DataFrame) NotNA() DataFrame {
	return df.NotNull()
}

// ============================================================================
// Value Counts & Statistics
// ============================================================================

// ValueCounts returns the frequency count of unique values for a column.
// Similar to pandas value_counts().
//
// Parameters:
// - colname: column name to count values for
// - normalize: if true, returns relative frequencies instead of counts
// - ascending: if true, sorts in ascending order (default: false)
//
// Returns:
// - DataFrame with two columns: the unique values and their counts
func (df DataFrame) ValueCounts(colname string, normalize bool, ascending bool) DataFrame {
	if df.Err != nil {
		return df
	}
	
	col := df.Col(colname)
	if col.Err != nil {
		return DataFrame{Err: col.Err}
	}
	
	// Count occurrences
	counts := make(map[string]int)
	for i := 0; i < col.Len(); i++ {
		key := col.Elem(i).String()
		counts[key]++
	}
	
	// Convert to slices
	values := make([]string, 0, len(counts))
	countVals := make([]int, 0, len(counts))
	for v, c := range counts {
		values = append(values, v)
		countVals = append(countVals, c)
	}
	
	// Sort by count
	sortIdx := make([]int, len(countVals))
	for i := range sortIdx {
		sortIdx[i] = i
	}
	sort.Slice(sortIdx, func(i, j int) bool {
		if ascending {
			return countVals[sortIdx[i]] < countVals[sortIdx[j]]
		}
		return countVals[sortIdx[i]] > countVals[sortIdx[j]]
	})
	
	// Build result
	sortedValues := make([]string, len(values))
	sortedCounts := make([]float64, len(countVals))
	for i, idx := range sortIdx {
		sortedValues[i] = values[idx]
		if normalize {
			sortedCounts[i] = float64(countVals[idx]) / float64(col.Len())
		} else {
			sortedCounts[i] = float64(countVals[idx])
		}
	}
	
	resultCols := []series.Series{
		series.Strings(sortedValues),
		series.Floats(sortedCounts),
	}
	
	if normalize {
		resultCols[0].Name = colname
		resultCols[1].Name = "proportion"
	} else {
		resultCols[0].Name = colname
		resultCols[1].Name = "count"
	}
	
	return New(resultCols...)
}

// ============================================================================
// Top N Selection (Pandas-compatible)
// ============================================================================

// NLargest returns the top n rows sorted by the specified column in descending order.
// Similar to pandas nlargest().
//
// Example:
//   df.NLargest(5, "price")  // Top 5 rows by price
func (df DataFrame) NLargest(n int, colname string) DataFrame {
	if df.Err != nil {
		return df
	}
	return df.Arrange(RevSort(colname)).Head(n)
}

// NSmallest returns the bottom n rows sorted by the specified column in ascending order.
// Similar to pandas nsmallest().
//
// Example:
//   df.NSmallest(5, "price")  // Bottom 5 rows by price
func (df DataFrame) NSmallest(n int, colname string) DataFrame {
	if df.Err != nil {
		return df
	}
	return df.Arrange(Sort(colname)).Head(n)
}

// ============================================================================
// Random Sampling (Pandas-compatible)
// ============================================================================

// Sample returns a random sample of rows from the DataFrame.
// Similar to pandas sample().
//
// Parameters:
// - n: number of rows to sample (if > 0, frac is ignored)
// - frac: fraction of rows to sample (0.0 to 1.0)
// - replace: whether to sample with replacement
// - seed: random seed for reproducibility
//
// Example:
//   df.Sample(10, -1, false, 42)   // 10 random rows
//   df.Sample(-1, 0.1, false, 42)  // 10% of rows
func (df DataFrame) Sample(n int, frac float64, replace bool, seed int64) DataFrame {
	if df.Err != nil {
		return df
	}
	
	if n < 0 && frac <= 0 {
		return DataFrame{Err: fmt.Errorf("sample: must specify n or frac")}
	}
	
	// Determine sample size
	sampleSize := n
	if n < 0 {
		sampleSize = int(float64(df.nrows) * frac)
	}
	
	if sampleSize <= 0 {
		return df.Subset([]int{})
	}
	
	// Initialize random source
	rng := rand.New(rand.NewSource(seed))
	
	// Generate random indexes
	var indexes []int
	if replace {
		// Sampling with replacement
		indexes = make([]int, sampleSize)
		for i := 0; i < sampleSize; i++ {
			indexes[i] = rng.Intn(df.nrows)
		}
	} else {
		// Sampling without replacement
		if sampleSize > df.nrows {
			return DataFrame{Err: fmt.Errorf("sample: n larger than population")}
		}
		
		// Fisher-Yates shuffle
		perm := rng.Perm(df.nrows)
		indexes = perm[:sampleSize]
		sort.Ints(indexes) // Sort for consistent output
	}
	
	return df.Subset(indexes)
}

// ============================================================================
// Pipe & Function Application (Pandas-compatible)
// ============================================================================

// Pipe applies a function to the DataFrame and returns the result.
// Similar to pandas pipe().
// Enables method chaining for custom operations.
//
// Example:
//   result := df.
//       Filter(dataframe.F{"age", ">", 18}).
//       Pipe(customTransform).
//       Arrange(dataframe.Sort("name"))
func (df DataFrame) Pipe(f func(DataFrame) DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	return f(df)
}

// PipeWithArgs applies a function with additional arguments to the DataFrame.
// Similar to pandas pipe() with args.
//
// Example:
//   result := df.PipeWithArgs(customFunc, arg1, arg2)
func (df DataFrame) PipeWithArgs(f func(DataFrame, ...interface{}) DataFrame, args ...interface{}) DataFrame {
	if df.Err != nil {
		return df
	}
	return f(df, args...)
}

// ApplyMap applies a function element-wise to the entire DataFrame.
// Similar to pandas applymap().
//
// Example:
//   df2 := df.ApplyMap(func(val interface{}) interface{} {
//       if s, ok := val.(string); ok {
//           return strings.ToUpper(s)
//       }
//       return val
//   })
func (df DataFrame) ApplyMap(f func(interface{}) interface{}) DataFrame {
	if df.Err != nil {
		return df
	}
	
	columns := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		elements := make([]interface{}, df.nrows)
		for j := 0; j < df.nrows; j++ {
			elements[j] = f(col.Elem(j).Val())
		}
		columns[i] = series.New(elements, col.Type(), col.Name)
	}
	
	return New(columns...)
}

// ============================================================================
// Value Clipping & Range Operations (Pandas-compatible)
// ============================================================================

// Clip clips values to the specified range.
// Similar to pandas clip().
//
// Parameters:
// - lower: minimum value (use nil for no lower bound)
// - upper: maximum value (use nil for no upper bound)
//
// Example:
//   df2 := df.Clip(0, 100)  // Clip all values to [0, 100]
func (df DataFrame) Clip(lower, upper *float64) DataFrame {
	if df.Err != nil {
		return df
	}
	
	columns := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		if col.Type() != series.Float && col.Type() != series.Int {
			// Pass through non-numeric columns unchanged
			columns[i] = col.Copy()
			continue
		}
		
		floats := col.Float()
		clipped := make([]float64, len(floats))
		for j, v := range floats {
			clipped[j] = v
			if lower != nil && v < *lower {
				clipped[j] = *lower
			}
			if upper != nil && v > *upper {
				clipped[j] = *upper
			}
		}
		columns[i] = series.Floats(clipped)
		columns[i].Name = col.Name
	}
	
	return New(columns...)
}

// ClipColumn clips values in a specific column to the specified range.
// Similar to pandas Series clip().
//
// Example:
//   df2 := df.ClipColumn("price", 0, 1000)
func (df DataFrame) ClipColumn(colname string, lower, upper *float64) DataFrame {
	if df.Err != nil {
		return df
	}
	
	idx := df.ColIndex(colname)
	if idx < 0 {
		return DataFrame{Err: fmt.Errorf("clip: column %q not found", colname)}
	}
	
	col := df.columns[idx]
	if col.Type() != series.Float && col.Type() != series.Int {
		return DataFrame{Err: fmt.Errorf("clip: column %q is not numeric", colname)}
	}
	
	floats := col.Float()
	clipped := make([]float64, len(floats))
	for j, v := range floats {
		clipped[j] = v
		if lower != nil && v < *lower {
			clipped[j] = *lower
		}
		if upper != nil && v > *upper {
			clipped[j] = *upper
		}
	}
	
	result := df.Copy()
	result.columns[idx] = series.Floats(clipped)
	result.columns[idx].Name = col.Name
	
	return result
}

// ============================================================================
// Value Replacement (Pandas-compatible)
// ============================================================================

// Replace replaces values in the DataFrame.
// Similar to pandas replace().
//
// Parameters:
// - toReplace: value to replace
// - with: replacement value
//
// Example:
//   df2 := df.Replace("NA", nil)  // Replace "NA" strings with NaN
func (df DataFrame) Replace(toReplace, with interface{}) DataFrame {
	if df.Err != nil {
		return df
	}
	
	columns := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		elements := make([]interface{}, df.nrows)
		for j := 0; j < df.nrows; j++ {
			val := col.Elem(j).Val()
			if val == toReplace {
				elements[j] = with
			} else {
				elements[j] = val
			}
		}
		columns[i] = series.New(elements, col.Type(), col.Name)
	}
	
	return New(columns...)
}

// ReplaceInColumn replaces values in a specific column.
// Similar to pandas Series replace().
//
// Example:
//   df2 := df.ReplaceInColumn("status", "unknown", nil)
func (df DataFrame) ReplaceInColumn(colname string, toReplace, with interface{}) DataFrame {
	if df.Err != nil {
		return df
	}
	
	idx := df.ColIndex(colname)
	if idx < 0 {
		return DataFrame{Err: fmt.Errorf("replace: column %q not found", colname)}
	}
	
	col := df.columns[idx]
	elements := make([]interface{}, df.nrows)
	for j := 0; j < df.nrows; j++ {
		val := col.Elem(j).Val()
		if val == toReplace {
			elements[j] = with
		} else {
			elements[j] = val
		}
	}
	
	result := df.Copy()
	result.columns[idx] = series.New(elements, col.Type(), col.Name)
	
	return result
}

// ============================================================================
// Type Conversion (Pandas-compatible)
// ============================================================================

// Astype converts column types.
// Similar to pandas astype().
//
// Parameters:
// - coltypes: map of column name to target type
//
// Example:
//   df2 := df.Astype(map[string]series.Type{
//       "price": series.Float,
//       "qty": series.Int,
//   })
func (df DataFrame) Astype(coltypes map[string]series.Type) DataFrame {
	if df.Err != nil {
		return df
	}
	
	columns := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		targetType, ok := coltypes[col.Name]
		if !ok || targetType == col.Type() {
			columns[i] = col.Copy()
			continue
		}
		
		// Convert using BatchConvert for efficiency
		switch col.Type() {
		case series.Int:
			ints, _ := col.Int()
			columns[i] = series.BatchConvert(ints, targetType, col.Name)
		case series.Float:
			floats := col.Float()
			columns[i] = series.BatchConvert(floats, targetType, col.Name)
		case series.String:
			strings := col.Records()
			columns[i] = series.BatchConvert(strings, targetType, col.Name)
		case series.Bool:
			bools, _ := col.Bool()
			columns[i] = series.BatchConvert(bools, targetType, col.Name)
		default:
			columns[i] = col.Copy()
		}
	}
	
	return New(columns...)
}

// ============================================================================
// Between Operation (Pandas-compatible)
// ============================================================================

// Between returns a boolean Series indicating if values are between two bounds.
// Similar to pandas Series between().
//
// Parameters:
// - colname: column to check
// - left: left bound
// - right: right bound
// - inclusive: whether bounds are inclusive ("both", "neither", "left", "right")
//
// Example:
//   mask := df.Between("age", 18, 65, "both")
func (df DataFrame) Between(colname string, left, right float64, inclusive string) series.Series {
	if df.Err != nil {
		return series.Series{Err: df.Err}
	}
	
	col := df.Col(colname)
	if col.Err != nil {
		return col
	}
	
	floats := col.Float()
	bools := make([]bool, len(floats))
	
	for i, v := range floats {
		switch inclusive {
		case "both", "":
			bools[i] = v >= left && v <= right
		case "neither":
			bools[i] = v > left && v < right
		case "left":
			bools[i] = v >= left && v < right
		case "right":
			bools[i] = v > left && v <= right
		default:
			bools[i] = v >= left && v <= right
		}
	}
	
	result := series.Bools(bools)
	result.Name = colname
	return result
}

// ============================================================================
// IsIn Operation (Pandas-compatible)
// ============================================================================

// IsIn returns a boolean Series indicating if values are in a set.
// Similar to pandas Series isin().
//
// Parameters:
// - colname: column to check
// - values: set of values to check against
//
// Example:
//   mask := df.IsIn("country", []string{"US", "UK", "CA"})
func (df DataFrame) IsIn(colname string, values []interface{}) series.Series {
	if df.Err != nil {
		return series.Series{Err: df.Err}
	}
	
	col := df.Col(colname)
	if col.Err != nil {
		return col
	}
	
	// Build lookup map
	lookup := make(map[string]bool)
	for _, v := range values {
		switch val := v.(type) {
		case string:
			lookup[val] = true
		default:
			lookup[fmt.Sprintf("%v", v)] = true
		}
	}
	
	bools := make([]bool, df.nrows)
	for i := 0; i < df.nrows; i++ {
		val := col.Elem(i).String()
		bools[i] = lookup[val]
	}
	
	result := series.Bools(bools)
	result.Name = colname
	return result
}

// FilterIsIn filters rows where column values are in the specified set.
// Convenient wrapper around IsIn + Filter.
//
// Example:
//   df2 := df.FilterIsIn("country", []string{"US", "UK", "CA"})
func (df DataFrame) FilterIsIn(colname string, values []interface{}) DataFrame {
	if df.Err != nil {
		return df
	}
	
	mask := df.IsIn(colname, values)
	if mask.Err != nil {
		return DataFrame{Err: mask.Err}
	}
	
	return df.Subset(mask)
}

// ============================================================================
// Assign & Explode
// ============================================================================

// Assign adds or replaces a column computed by f(df).
// If a column with the given name already exists it is replaced; otherwise it
// is appended. This is analogous to pandas df.assign().
//
// Example:
//
//	df2 := df.Assign("profit", func(d DataFrame) series.Series {
//	    rev := d.Col("revenue").Float()
//	    cost := d.Col("cost").Float()
//	    out := make([]float64, len(rev))
//	    for i := range rev { out[i] = rev[i] - cost[i] }
//	    return series.Floats(out)
//	})
func (df DataFrame) Assign(name string, f func(DataFrame) series.Series) DataFrame {
	if df.Err != nil {
		return df
	}
	col := f(df)
	if col.Err != nil {
		return DataFrame{Err: fmt.Errorf("Assign %q: %v", name, col.Err)}
	}
	col.Name = name
	return df.Mutate(col)
}

// Explode expands a column whose elements are comma-separated lists into
// individual rows, replicating all other columns.
// Each element in the list becomes its own row; the expanded column is always
// of type String.
//
// Example:
//
//	// "tags" column contains "go,python,rust" → 3 rows
//	df2 := df.Explode("tags")
func (df DataFrame) Explode(colname string) DataFrame {
	if df.Err != nil {
		return df
	}
	idx := df.ColIndex(colname)
	if idx < 0 {
		return DataFrame{Err: fmt.Errorf("Explode: column %q not found", colname)}
	}

	// Build new columns: same types, empty.
	newCols := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		newCols[i] = col.Empty()
		newCols[i].Name = col.Name
	}

	for row := 0; row < df.nrows; row++ {
		cell := df.columns[idx].Elem(row).String()
		// Split on comma; trim spaces.
		parts := strings.Split(cell, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			for ci, col := range df.columns {
				if ci == idx {
					newCols[ci].Append(part)
				} else {
					newCols[ci].Append(col.Elem(row))
				}
			}
		}
	}
	return New(newCols...)
}
