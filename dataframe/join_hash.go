package dataframe

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dreamsxin/gota/series"
)

// joinKeys holds resolved column indices for a join operation.
type joinKeys struct {
	iKeysA    []int
	iKeysB    []int
	iNotKeysA []int
	iNotKeysB []int
	newCols   []series.Series
}

// resolveJoinKeys validates that all key columns exist in both DataFrames,
// computes the non-key column index lists, and pre-allocates empty output columns.
func resolveJoinKeys(a, b DataFrame, keys []string) (joinKeys, error) {
	var jk joinKeys
	aCols := a.columns
	bCols := b.columns

	var errArr []string
	for _, key := range keys {
		i := a.ColIndex(key)
		if i < 0 {
			errArr = append(errArr, fmt.Sprintf("can't find key %q on left DataFrame", key))
		}
		jk.iKeysA = append(jk.iKeysA, i)

		j := b.ColIndex(key)
		if j < 0 {
			errArr = append(errArr, fmt.Sprintf("can't find key %q on right DataFrame", key))
		}
		jk.iKeysB = append(jk.iKeysB, j)
	}
	if len(errArr) != 0 {
		return jk, withSentinel(strings.Join(errArr, "\n"), ErrKeyNotFound)
	}

	// Key columns come first in output.
	for _, i := range jk.iKeysA {
		jk.newCols = append(jk.newCols, aCols[i].Empty())
	}
	// Non-key columns from a.
	for i := 0; i < a.ncols; i++ {
		if !inIntSlice(i, jk.iKeysA) {
			jk.iNotKeysA = append(jk.iNotKeysA, i)
			jk.newCols = append(jk.newCols, aCols[i].Empty())
		}
	}
	// Non-key columns from b.
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, jk.iKeysB) {
			jk.iNotKeysB = append(jk.iNotKeysB, i)
			jk.newCols = append(jk.newCols, bCols[i].Empty())
		}
	}
	return jk, nil
}

// buildJoinKey builds an unambiguous composite key for the selected columns.
// Length prefixes prevent values containing separators or type-like text from
// colliding, and the validity marker distinguishes missing values explicitly.
func buildJoinKey(cols []series.Series, keyIdxs []int, row int) string {
	var sb strings.Builder
	for _, k := range keyIdxs {
		elem := cols[k].Elem(row)
		appendLengthPrefixed(&sb, string(elem.Type()))
		if elem.IsNA() {
			sb.WriteByte('0')
			continue
		}
		sb.WriteByte('1')
		appendLengthPrefixed(&sb, elem.String())
	}
	return sb.String()
}

func appendLengthPrefixed(sb *strings.Builder, value string) {
	sb.WriteString(strconv.Itoa(len(value)))
	sb.WriteByte(':')
	sb.WriteString(value)
}

// buildHashTable builds a map from join key → list of row indices for DataFrame b.
func buildHashTable(b DataFrame, iKeysB []int) map[string][]int {
	bCols := b.columns
	ht := make(map[string][]int, b.nrows)
	for j := 0; j < b.nrows; j++ {
		k := buildJoinKey(bCols, iKeysB, j)
		ht[k] = append(ht[k], j)
	}
	return ht
}

// appendMatchedRow writes one matched (i, j) pair into newCols.
func appendMatchedRow(newCols []series.Series, aCols, bCols []series.Series, jk joinKeys, i, j int) {
	ii := 0
	for _, k := range jk.iKeysA {
		newCols[ii].Append(aCols[k].Elem(i))
		ii++
	}
	for _, k := range jk.iNotKeysA {
		newCols[ii].Append(aCols[k].Elem(i))
		ii++
	}
	for _, k := range jk.iNotKeysB {
		newCols[ii].Append(bCols[k].Elem(j))
		ii++
	}
}

// appendLeftOnlyRow writes a row from a with NULLs for b's non-key columns.
func appendLeftOnlyRow(newCols []series.Series, aCols []series.Series, jk joinKeys, i int) {
	ii := 0
	for _, k := range jk.iKeysA {
		newCols[ii].Append(aCols[k].Elem(i))
		ii++
	}
	for _, k := range jk.iNotKeysA {
		newCols[ii].Append(aCols[k].Elem(i))
		ii++
	}
	for range jk.iNotKeysB {
		newCols[ii].Append(nil)
		ii++
	}
}

// appendRightOnlyRow writes a row from b with NULLs for a's non-key columns.
func appendRightOnlyRow(newCols []series.Series, bCols []series.Series, jk joinKeys, j int) {
	ii := 0
	for _, k := range jk.iKeysB {
		newCols[ii].Append(bCols[k].Elem(j))
		ii++
	}
	for range jk.iNotKeysA {
		newCols[ii].Append(nil)
		ii++
	}
	for _, k := range jk.iNotKeysB {
		newCols[ii].Append(bCols[k].Elem(j))
		ii++
	}
}
