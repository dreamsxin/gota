package dataframe

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dreamsxin/gota/series"
)

// ============================================================================
// Resample — time-based grouping
// ============================================================================

// ResampleFreq defines the resampling frequency.
type ResampleFreq string

const (
	ResampleDaily   ResampleFreq = "D"
	ResampleWeekly  ResampleFreq = "W"
	ResampleMonthly ResampleFreq = "M"
	ResampleYearly  ResampleFreq = "Y"
	ResampleHourly  ResampleFreq = "H"
)

// ResampleGroups is the result of a Resample call.
type ResampleGroups struct {
	groups  map[string]DataFrame
	keyCol  string
	freq    ResampleFreq
	Err     error
}

// Resample groups rows by truncating the Time column colname to the given
// frequency. It returns a ResampleGroups that supports Aggregation.
//
// Example:
//
//	rg := df.Resample("date", dataframe.ResampleMonthly)
//	monthly := rg.Aggregation([]AggregationType{Aggregation_SUM}, []string{"revenue"})
func (df DataFrame) Resample(colname string, freq ResampleFreq) ResampleGroups {
	if df.Err != nil {
		return ResampleGroups{Err: df.Err}
	}
	col := df.Col(colname)
	if col.Err != nil {
		return ResampleGroups{Err: fmt.Errorf("Resample: %v", col.Err)}
	}
	if col.Type() != series.Time {
		return ResampleGroups{Err: fmt.Errorf("Resample: column %q is not Time type", colname)}
	}

	truncate := func(t time.Time) string {
		switch freq {
		case ResampleHourly:
			return t.Truncate(time.Hour).Format(time.RFC3339)
		case ResampleDaily:
			y, m, d := t.Date()
			return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
		case ResampleWeekly:
			y, w := t.ISOWeek()
			return fmt.Sprintf("%04d-W%02d", y, w)
		case ResampleMonthly:
			return t.Format("2006-01")
		case ResampleYearly:
			return fmt.Sprintf("%04d", t.Year())
		default:
			return t.Format(time.RFC3339)
		}
	}

	// Use int64 Unix nanoseconds as the primary grouping key to avoid
	// repeated string formatting per row. The string label is derived once
	// per unique bucket for the output column.
	type bucket struct {
		label string
	}
	bucketByNs := make(map[int64]*bucket)
	groupRows := make(map[int64][]int)

	nsKey := func(t time.Time) int64 {
		switch freq {
		case ResampleHourly:
			return t.Truncate(time.Hour).UnixNano()
		case ResampleDaily:
			y, m, d := t.Date()
			return time.Date(y, m, d, 0, 0, 0, 0, t.Location()).UnixNano()
		case ResampleWeekly:
			y, w := t.ISOWeek()
			// Anchor to Monday of that ISO week.
			jan4 := time.Date(y, 1, 4, 0, 0, 0, 0, t.Location())
			_, jan4w := jan4.ISOWeek()
			monday := jan4.AddDate(0, 0, (w-jan4w)*7-int(jan4.Weekday())+1)
			return monday.UnixNano()
		case ResampleMonthly:
			y, m, _ := t.Date()
			return time.Date(y, m, 1, 0, 0, 0, 0, t.Location()).UnixNano()
		case ResampleYearly:
			return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).UnixNano()
		default:
			return t.UnixNano()
		}
	}

	for i := 0; i < df.nrows; i++ {
		e := col.Elem(i)
		if e.IsNA() {
			continue
		}
		tv, _ := e.Time()
		ns := nsKey(tv)
		if _, ok := bucketByNs[ns]; !ok {
			bucketByNs[ns] = &bucket{label: truncate(tv)}
		}
		groupRows[ns] = append(groupRows[ns], i)
	}

	groups := make(map[string]DataFrame, len(groupRows))
	for ns, rows := range groupRows {
		label := bucketByNs[ns].label
		groups[label] = df.Subset(rows)
	}
	return ResampleGroups{groups: groups, keyCol: colname, freq: freq}
}

// Aggregation aggregates each time bucket using the given types and column names.
// The result includes a "period" column with the bucket label.
func (rg ResampleGroups) Aggregation(typs []AggregationType, colnames []string) DataFrame {
	if rg.Err != nil {
		return DataFrame{Err: rg.Err}
	}
	if len(typs) != len(colnames) {
		return DataFrame{Err: fmt.Errorf("ResampleGroups.Aggregation: len mismatch")}
	}
	if len(rg.groups) == 0 {
		return DataFrame{Err: fmt.Errorf("ResampleGroups.Aggregation: no groups")}
	}

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(rg.groups))
	for k := range rg.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	periods := make([]string, 0, len(keys))
	colData := make([][]float64, len(colnames))
	for i := range colData {
		colData[i] = make([]float64, 0, len(keys))
	}

	for _, key := range keys {
		df := rg.groups[key]
		periods = append(periods, key)
		for i, c := range colnames {
			col := df.Col(c)
			var v float64
			switch typs[i] {
			case Aggregation_SUM:
				v = col.Sum()
			case Aggregation_MEAN:
				v = col.Mean()
			case Aggregation_MAX:
				v = col.Max()
			case Aggregation_MIN:
				v = col.Min()
			case Aggregation_COUNT:
				v = float64(col.Len())
			case Aggregation_STD:
				v = col.StdDev()
			case Aggregation_MEDIAN:
				v = col.Median()
			}
			colData[i] = append(colData[i], v)
		}
	}

	cols := []series.Series{series.New(periods, series.String, "period")}
	for i, c := range colnames {
		s := series.Floats(colData[i])
		s.Name = buildAggregatedColname(c, typs[i])
		cols = append(cols, s)
	}
	return NewNoCopy(cols...)
}

// ============================================================================
// Stack / Unstack
// ============================================================================

// Stack converts a wide DataFrame to long format by stacking the given value
// columns into two new columns: one for the variable name and one for the value.
// This is equivalent to Melt but with a more intuitive name.
//
// Example:
//
//	// wide: id | q1 | q2 | q3
//	// long: id | quarter | value
//	df.Stack([]string{"id"}, []string{"q1","q2","q3"}, "quarter", "value")
func (df DataFrame) Stack(idVars, valueVars []string, varName, valueName string) DataFrame {
	return df.Melt(idVars, valueVars, varName, valueName)
}

// Unstack pivots a long DataFrame back to wide format.
// colVar is the column whose unique values become new column names.
// colVal is the column whose values fill the new columns.
// idVars are the columns that identify each row in the wide result.
//
// Example:
//
//	// long: id | quarter | value
//	// wide: id | q1 | q2 | q3
//	df.Unstack([]string{"id"}, "quarter", "value")
func (df DataFrame) Unstack(idVars []string, colVar, colVal string) DataFrame {
	if df.Err != nil {
		return df
	}
	if len(idVars) == 0 {
		return DataFrame{Err: fmt.Errorf("Unstack: idVars must not be empty")}
	}
	// Validate columns.
	for _, c := range append(idVars, colVar, colVal) {
		if df.ColIndex(c) < 0 {
			return DataFrame{Err: fmt.Errorf("Unstack: column %q not found", c)}
		}
	}

	// Collect unique values of colVar (preserving first-seen order).
	varCol := df.Col(colVar)
	seen := make(map[string]struct{})
	var varVals []string
	for i := 0; i < varCol.Len(); i++ {
		v := varCol.Elem(i).String()
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			varVals = append(varVals, v)
		}
	}
	sort.Strings(varVals)

	// Build a composite key from idVars for each row.
	// Pre-cache all idVar columns to avoid repeated df.Col() calls.
	idCols := make([]series.Series, len(idVars))
	for j, id := range idVars {
		idCols[j] = df.Col(id)
	}
	rowKey := func(i int) string {
		parts := make([]string, len(idVars))
		for j := range idVars {
			parts[j] = idCols[j].Elem(i).String()
		}
		return strings.Join(parts, "\x00")
	}

	// Collect unique row keys (preserving order) and pre-parse their parts.
	var rowKeys []string
	rowKeySet := make(map[string]struct{})
	// parsedParts[ri] holds the pre-split id values for row key ri.
	var parsedParts [][]string
	for i := 0; i < df.nrows; i++ {
		k := rowKey(i)
		if _, ok := rowKeySet[k]; !ok {
			rowKeySet[k] = struct{}{}
			rowKeys = append(rowKeys, k)
			parts := make([]string, len(idVars))
			for j := range idVars {
				parts[j] = idCols[j].Elem(i).String()
			}
			parsedParts = append(parsedParts, parts)
		}
	}

	// Build lookup: (rowKey, varVal) → value string.
	lookup := make(map[string]string, df.nrows)
	valCol := df.Col(colVal)
	for i := 0; i < df.nrows; i++ {
		k := rowKey(i) + "\x01" + varCol.Elem(i).String()
		lookup[k] = valCol.Elem(i).String()
	}

	// Build output columns: idVars first, then one column per varVal.
	idData := make([][]string, len(idVars))
	for j := range idVars {
		idData[j] = make([]string, len(rowKeys))
	}
	valData := make([][]string, len(varVals))
	for j := range varVals {
		valData[j] = make([]string, len(rowKeys))
	}

	// Determine the type of the value column for typed NaN filling.
	valColSeries := df.Col(colVal)
	valType := valColSeries.Type()
	typedNaN := func() string {
		switch valType {
		case series.Float, series.Int:
			return "NaN"
		case series.Bool:
			return "false"
		default: // String, Time
			return ""
		}
	}()

	for ri, rk := range rowKeys {
		// Use pre-parsed parts — no strings.Split per row.
		for j := range idVars {
			idData[j][ri] = parsedParts[ri][j]
		}
		for vi, vv := range varVals {
			k := rk + "\x01" + vv
			if v, ok := lookup[k]; ok {
				valData[vi][ri] = v
			} else {
				valData[vi][ri] = typedNaN
			}
		}
	}

	cols := make([]series.Series, 0, len(idVars)+len(varVals))
	for j, id := range idVars {
		s := series.New(idData[j], series.String, id)
		cols = append(cols, s)
	}
	for vi, vv := range varVals {
		s := series.New(valData[vi], series.String, vv)
		cols = append(cols, s)
	}
	return NewNoCopy(cols...)
}

// ============================================================================
// Interpolate — fill NaN values in numeric columns
// ============================================================================

// InterpolateMethod defines the interpolation strategy.
type InterpolateMethod string

const (
	// InterpolateLinear fills NaN values using linear interpolation between
	// the nearest non-NaN neighbours.
	InterpolateLinear InterpolateMethod = "linear"
	// InterpolateForward fills NaN values with the most recent non-NaN value
	// (forward fill / ffill).
	InterpolateForward InterpolateMethod = "forward"
)

// Interpolate fills NaN values in numeric (Float/Int) columns using the given
// method. Non-numeric columns are left unchanged.
//
// Supported methods:
//   - "linear"  — linear interpolation between surrounding non-NaN values
//   - "forward" — forward fill (propagate last valid value)
//
// Example:
//
//	df2 := df.Interpolate("linear")
//	df2 := df.Interpolate("forward")
func (df DataFrame) Interpolate(method InterpolateMethod) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series, df.ncols)
	for i, col := range df.columns {
		if col.Type() != series.Float && col.Type() != series.Int {
			columns[i] = col.Copy()
			continue
		}
		floats := col.Float()
		out := make([]float64, len(floats))
		copy(out, floats)

		switch method {
		case InterpolateForward:
			var last float64
			hasLast := false
			for j, v := range out {
				if !isNaNFloat(v) {
					last = v
					hasLast = true
				} else if hasLast {
					out[j] = last
				}
			}
		default: // linear
			// Find runs of NaN and interpolate linearly between neighbours.
			n := len(out)
			j := 0
			for j < n {
				if !isNaNFloat(out[j]) {
					j++
					continue
				}
				// Found start of a NaN run at j.
				start := j
				for j < n && isNaNFloat(out[j]) {
					j++
				}
				end := j // out[end] is the first non-NaN after the run (or n)
				// Determine left and right anchor values.
				leftIdx := start - 1
				rightIdx := end
				if leftIdx < 0 && rightIdx >= n {
					// All NaN — leave as-is.
					continue
				}
				if leftIdx < 0 {
					// Leading NaN: forward fill from right anchor.
					for k := start; k < end; k++ {
						out[k] = out[rightIdx]
					}
				} else if rightIdx >= n {
					// Trailing NaN: backward fill from left anchor.
					for k := start; k < end; k++ {
						out[k] = out[leftIdx]
					}
				} else {
					// Interior NaN: linear interpolation.
					leftVal := out[leftIdx]
					rightVal := out[rightIdx]
					span := float64(rightIdx - leftIdx)
					for k := start; k < end; k++ {
						t := float64(k-leftIdx) / span
						out[k] = leftVal + t*(rightVal-leftVal)
					}
				}
			}
		}
		s := series.Floats(out)
		s.Name = col.Name
		columns[i] = s
	}
	return NewNoCopy(columns...)
}

func isNaNFloat(v float64) bool {
	return v != v // IEEE 754: NaN != NaN
}

// ============================================================================
// CrossTab — contingency table
// ============================================================================

// CrossTab computes a frequency cross-tabulation (contingency table) of two
// categorical columns. The result is a DataFrame where rows correspond to
// unique values of rowCol, columns correspond to unique values of colCol, and
// each cell contains the count of rows matching that (row, col) combination.
// A leading "index" column holds the row labels.
//
// Example:
//
//	// df has columns "gender" and "grade"
//	ct := df.CrossTab("gender", "grade")
//	// Result: index | A | B | C | ...
//	//         F     | 3 | 5 | 2 | ...
//	//         M     | 4 | 2 | 6 | ...
func (df DataFrame) CrossTab(rowCol, colCol string) DataFrame {
	if df.Err != nil {
		return df
	}
	rowSeries := df.Col(rowCol)
	if rowSeries.Err != nil {
		return DataFrame{Err: fmt.Errorf("CrossTab: %v", rowSeries.Err)}
	}
	colSeries := df.Col(colCol)
	if colSeries.Err != nil {
		return DataFrame{Err: fmt.Errorf("CrossTab: %v", colSeries.Err)}
	}

	// Collect unique row and column labels (preserving first-seen order).
	rowLabels := uniqueStrings(rowSeries)
	colLabels := uniqueStrings(colSeries)
	sort.Strings(rowLabels)
	sort.Strings(colLabels)

	// Build count matrix.
	counts := make(map[string]map[string]int, len(rowLabels))
	for _, r := range rowLabels {
		counts[r] = make(map[string]int, len(colLabels))
	}
	for i := 0; i < df.nrows; i++ {
		r := rowSeries.Elem(i).String()
		c := colSeries.Elem(i).String()
		if _, ok := counts[r]; ok {
			counts[r][c]++
		}
	}

	// Build output DataFrame.
	indexCol := series.New(rowLabels, series.String, "index")
	cols := []series.Series{indexCol}
	for _, cl := range colLabels {
		vals := make([]int, len(rowLabels))
		for ri, rl := range rowLabels {
			vals[ri] = counts[rl][cl]
		}
		s := series.New(vals, series.Int, cl)
		cols = append(cols, s)
	}
	return NewNoCopy(cols...)
}

// uniqueStrings returns the unique non-NaN string values from a Series,
// preserving first-seen order.
func uniqueStrings(s series.Series) []string {
	seen := make(map[string]struct{}, s.Len())
	var out []string
	for i := 0; i < s.Len(); i++ {
		e := s.Elem(i)
		if e.IsNA() {
			continue
		}
		k := e.String()
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	return out
}
