package dataframe

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dreamsxin/gota/v2/series"
)

// joinKeys holds resolved column indices for a join operation.
type joinKeys struct {
	iKeysA    []int
	iKeysB    []int
	iNotKeysA []int
	iNotKeysB []int
}

// resolveJoinKeys validates that all key columns exist in both DataFrames
// and computes the non-key column index lists used by the batched output
// assembly.
func resolveJoinKeys(a, b DataFrame, keys []string) (joinKeys, error) {
	var jk joinKeys

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

	// Non-key column index lists drive the batched output assembly.
	for i := 0; i < a.ncols; i++ {
		if !inIntSlice(i, jk.iKeysA) {
			jk.iNotKeysA = append(jk.iNotKeysA, i)
		}
	}
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, jk.iKeysB) {
			jk.iNotKeysB = append(jk.iNotKeysB, i)
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
		col := cols[k]
		appendLengthPrefixed(&sb, string(col.Type()))
		if col.IsNA(row) {
			sb.WriteByte('0')
			continue
		}
		sb.WriteByte('1')
		appendLengthPrefixed(&sb, col.Record(row))
	}
	return sb.String()
}

func appendLengthPrefixed(sb *strings.Builder, value string) {
	sb.WriteString(strconv.Itoa(len(value)))
	sb.WriteByte(':')
	sb.WriteString(value)
}

// keyIndexer maps join keys to row lists. Single Int/Bool/String key pairs
// of identical type hash the typed values directly, avoiding per-row string
// keys; composite keys, Time keys, and cross-type pairs keep the
// collision-safe length-prefixed string encoding. Float keys stay on the
// string path because the %f record encoding is not bit-exact.
type keyIndexer struct {
	typed bool
	typ   series.Type
	col   series.Series // single key column of the indexed (build) side
	ints  map[int64][]int
	bools map[bool][]int
	strs  map[string][]int
	na    []int

	strCols   []series.Series
	strKeyIdx []int
	strHT     map[string][]int
}

// newKeyIndexer chooses the hash strategy given both sides' key columns.
func newKeyIndexer(buildCols []series.Series, buildKeyIdxs []int, lookupCols []series.Series, lookupKeyIdxs []int) *keyIndexer {
	if len(buildKeyIdxs) == 1 {
		bc := buildCols[buildKeyIdxs[0]]
		lc := lookupCols[lookupKeyIdxs[0]]
		if bc.Type() == lc.Type() {
			switch bc.Type() {
			case series.Int, series.Bool, series.String:
				return &keyIndexer{typed: true, typ: bc.Type(), col: bc}
			}
		}
	}
	return &keyIndexer{strCols: buildCols, strKeyIdx: buildKeyIdxs}
}

// build fills the hash table from the first nrows rows of the indexed side.
func (ki *keyIndexer) build(nrows int) {
	if !ki.typed {
		ki.strHT = make(map[string][]int, nrows)
		for j := 0; j < nrows; j++ {
			k := buildJoinKey(ki.strCols, ki.strKeyIdx, j)
			ki.strHT[k] = append(ki.strHT[k], j)
		}
		return
	}
	col := ki.col
	switch ki.typ {
	case series.Int:
		ki.ints = make(map[int64][]int, nrows)
		for j := 0; j < nrows; j++ {
			if col.IsNA(j) {
				ki.na = append(ki.na, j)
				continue
			}
			v, _ := col.Int64At(j)
			ki.ints[v] = append(ki.ints[v], j)
		}
	case series.Bool:
		ki.bools = make(map[bool][]int, nrows)
		for j := 0; j < nrows; j++ {
			if col.IsNA(j) {
				ki.na = append(ki.na, j)
				continue
			}
			v, _ := col.BoolAt(j)
			ki.bools[v] = append(ki.bools[v], j)
		}
	case series.String:
		ki.strs = make(map[string][]int, nrows)
		for j := 0; j < nrows; j++ {
			if col.IsNA(j) {
				ki.na = append(ki.na, j)
				continue
			}
			ki.strs[col.Record(j)] = append(ki.strs[col.Record(j)], j)
		}
	}
}

// matches returns the indexed-side rows whose key equals the lookup row's
// key. Missing keys match missing keys, mirroring the string encoding.
func (ki *keyIndexer) matches(lookupCols []series.Series, lookupKeyIdxs []int, row int) []int {
	if !ki.typed {
		return ki.strHT[buildJoinKey(lookupCols, lookupKeyIdxs, row)]
	}
	lc := lookupCols[lookupKeyIdxs[0]]
	if lc.IsNA(row) {
		return ki.na
	}
	switch ki.typ {
	case series.Int:
		v, _ := lc.Int64At(row)
		return ki.ints[v]
	case series.Bool:
		v, _ := lc.BoolAt(row)
		return ki.bools[v]
	case series.String:
		return ki.strs[lc.Record(row)]
	}
	return nil
}

// joinPairs collects output rows as index pairs into the two input frames;
// -1 marks the absent side (left-only or right-only rows).
type joinPairs struct {
	aRows []int
	bRows []int
}

func (p *joinPairs) addMatched(i, j int) {
	p.aRows = append(p.aRows, i)
	p.bRows = append(p.bRows, j)
}

func (p *joinPairs) addLeftOnly(i int) {
	p.aRows = append(p.aRows, i)
	p.bRows = append(p.bRows, -1)
}

func (p *joinPairs) addRightOnly(j int) {
	p.aRows = append(p.aRows, -1)
	p.bRows = append(p.bRows, j)
}

// assembleJoin materializes the output frame from collected row pairs in one
// batched pass per column (RFC §5 rule 3): key columns combine both sides,
// non-key columns gather from a single side with missing rows as NA.
func assembleJoin(aCols, bCols []series.Series, jk joinKeys, p joinPairs) DataFrame {
	columns := make([]series.Series, 0, len(jk.iKeysA)+len(jk.iNotKeysA)+len(jk.iNotKeysB))
	for ki, ka := range jk.iKeysA {
		col := series.CombineRows(aCols[ka], bCols[jk.iKeysB[ki]], p.aRows, p.bRows)
		col.Name = aCols[ka].Name
		columns = append(columns, col)
	}
	for _, k := range jk.iNotKeysA {
		col := aCols[k].GatherRows(p.aRows)
		col.Name = aCols[k].Name
		columns = append(columns, col)
	}
	for _, k := range jk.iNotKeysB {
		col := bCols[k].GatherRows(p.bRows)
		col.Name = bCols[k].Name
		columns = append(columns, col)
	}
	// New applies the standard dimension checks and column-name
	// deduplication, matching the historical join output names.
	return New(columns...)
}
