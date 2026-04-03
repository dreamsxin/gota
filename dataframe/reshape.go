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

	groups := make(map[string]DataFrame)
	groupRows := make(map[string][]int)
	for i := 0; i < df.nrows; i++ {
		e := col.Elem(i)
		if e.IsNA() {
			continue
		}
		tv, _ := e.Time()
		key := truncate(tv)
		groupRows[key] = append(groupRows[key], i)
	}
	for key, rows := range groupRows {
		groups[key] = df.Subset(rows)
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
	return New(cols...)
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
	rowKey := func(i int) string {
		parts := make([]string, len(idVars))
		for j, id := range idVars {
			parts[j] = df.Col(id).Elem(i).String()
		}
		return strings.Join(parts, "\x00")
	}

	// Collect unique row keys (preserving order).
	var rowKeys []string
	rowKeySet := make(map[string]struct{})
	for i := 0; i < df.nrows; i++ {
		k := rowKey(i)
		if _, ok := rowKeySet[k]; !ok {
			rowKeySet[k] = struct{}{}
			rowKeys = append(rowKeys, k)
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

	for ri, rk := range rowKeys {
		parts := strings.Split(rk, "\x00")
		for j := range idVars {
			idData[j][ri] = parts[j]
		}
		for vi, vv := range varVals {
			k := rk + "\x01" + vv
			if v, ok := lookup[k]; ok {
				valData[vi][ri] = v
			} else {
				valData[vi][ri] = "NaN"
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
	return New(cols...)
}
