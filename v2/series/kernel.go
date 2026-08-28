package series

import (
	"fmt"
	"time"
)

// Batch kernels (RFC §5): typed functions over whole columns that consume
// and produce selection masks. Missing values never satisfy a comparison,
// matching the v1 Element-based semantics the golden tests encode.

// Mask is an opaque row-selection mask produced by the comparison kernels.
// Bit semantics are handled inside the series package; DataFrames combine
// masks and materialize row indexes from them.
type Mask struct {
	m *bitmap
}

// NewMask returns an empty selection mask of n rows.
func NewMask(n int) Mask { return Mask{newSelectionMask(n)} }

// Select marks row i as selected.
func (m Mask) Select(i int) { m.m.set(i) }

// Len returns the number of rows the mask covers.
func (m Mask) Len() int { return m.m.n }

// AndInto intersects m with other (word-wise, allocation-free). Both masks
// must cover the same number of rows.
func (m Mask) AndInto(other Mask) { m.m.andInto(other.m) }

// OrInto unions m with other (word-wise, allocation-free). Both masks must
// cover the same number of rows.
func (m Mask) OrInto(other Mask) { m.m.orInto(other.m) }

// Rows returns the selected row indexes in ascending order.
func (m Mask) Rows() []int { return m.m.rows() }

// CompareMask evaluates the comparison over s and returns the selection
// mask. It is the kernel entry point used by DataFrame filtering; Compare
// keeps its Bool-Series signature on top of it.
func (s Series) CompareMask(comparator Comparator, comparando interface{}) (Mask, error) {
	// Dictionary columns compare through their string materialization;
	// results stay semantically identical to String columns.
	if elems, ok := s.elements.(dictionaryElements); ok {
		mat := Series{Name: s.Name, t: String, elements: elems.toStringColumn()}
		return mat.CompareMask(comparator, comparando)
	}
	n := s.Len()

	if comparator == CompFunc {
		f, ok := comparando.(compFunc)
		if !ok {
			return Mask{}, fmt.Errorf("comparando is not a comparison function of type func(s Series, i int) bool")
		}
		mask := newSelectionMask(n)
		for i := 0; i < n; i++ {
			if f(s, i) {
				mask.set(i)
			}
		}
		return Mask{mask}, nil
	}

	comp := New(comparando, s.t, "")

	if comparator == In || comparator == Out {
		return Mask{membershipMask(s, comp, comparator == In)}, nil
	}

	switch comparator {
	case Eq, Neq, Greater, GreaterEq, Less, LessEq:
	default:
		return Mask{}, fmt.Errorf("unknown comparator: %v", comparator)
	}

	if comp.Len() == 1 {
		return Mask{scalarCompareMask(s, comp, 0, comparator)}, nil
	}
	if n != comp.Len() {
		return Mask{}, fmt.Errorf("can't compare: length mismatch")
	}
	return Mask{bufferCompareMask(s, comp, comparator)}, nil
}

// scalarCompareMask compares every row against one valid operand value.
func scalarCompareMask(s Series, comp Series, j int, op Comparator) *bitmap {
	n := s.Len()
	mask := newSelectionMask(n)
	if comp.IsNA(j) {
		return mask
	}
	switch elems := s.elements.(type) {
	case intElements:
		opv, _ := comp.Int64At(j)
		compareInt64(elems.data, elems.validity, op, opv, mask)
	case floatElements:
		opv := comp.FloatAt(j)
		compareFloat64(elems.data, elems.validity, op, opv, mask)
	case stringElements:
		opv := comp.Record(j)
		compareString(elems.data, elems.validity, op, opv, mask)
	case boolElements:
		opv, _ := comp.BoolAt(j)
		compareBool(elems.data, elems.validity, op, opv, mask)
	case timeElements:
		opv, _ := comp.TimeAt(j)
		compareTime(elems.data, elems.validity, op, opv, mask)
	}
	return mask
}

// bufferCompareMask compares two same-type buffers row by row.
func bufferCompareMask(a, b Series, op Comparator) *bitmap {
	n := a.Len()
	mask := newSelectionMask(n)
	switch ae := a.elements.(type) {
	case intElements:
		be := b.elements.(intElements)
		for i := 0; i < n; i++ {
			if !ae.isValid(i) || !be.isValid(i) {
				continue
			}
			if cmpInt64(ae.data[i], be.data[i], op) {
				mask.set(i)
			}
		}
	case floatElements:
		be := b.elements.(floatElements)
		for i := 0; i < n; i++ {
			if !ae.isValid(i) || !be.isValid(i) {
				continue
			}
			if cmpFloat64(ae.data[i], be.data[i], op) {
				mask.set(i)
			}
		}
	case stringElements:
		be := b.elements.(stringElements)
		for i := 0; i < n; i++ {
			if !ae.isValid(i) || !be.isValid(i) {
				continue
			}
			if cmpString(ae.data[i], be.data[i], op) {
				mask.set(i)
			}
		}
	case boolElements:
		be := b.elements.(boolElements)
		for i := 0; i < n; i++ {
			if !ae.isValid(i) || !be.isValid(i) {
				continue
			}
			if cmpBool(ae.data[i], be.data[i], op) {
				mask.set(i)
			}
		}
	case timeElements:
		be := b.elements.(timeElements)
		for i := 0; i < n; i++ {
			if !ae.isValid(i) || !be.isValid(i) {
				continue
			}
			if cmpTime(ae.data[i], be.data[i], op) {
				mask.set(i)
			}
		}
	}
	return mask
}

// membershipMask implements In/Out with typed value sets: missing rows and
// missing operands never match.
func membershipMask(s, comp Series, in bool) *bitmap {
	n := s.Len()
	mask := newSelectionMask(n)
	switch elems := s.elements.(type) {
	case intElements:
		set := make(map[int64]struct{}, comp.Len())
		for j := 0; j < comp.Len(); j++ {
			if !comp.IsNA(j) {
				v, _ := comp.Int64At(j)
				set[v] = struct{}{}
			}
		}
		for i, v := range elems.data {
			if !elems.isValid(i) {
				continue
			}
			_, found := set[v]
			if found == in {
				mask.set(i)
			}
		}
	case floatElements:
		set := make(map[float64]struct{}, comp.Len())
		for j := 0; j < comp.Len(); j++ {
			if !comp.IsNA(j) {
				set[comp.FloatAt(j)] = struct{}{}
			}
		}
		for i, v := range elems.data {
			if !elems.isValid(i) {
				continue
			}
			_, found := set[v]
			if found == in {
				mask.set(i)
			}
		}
	case stringElements:
		set := make(map[string]struct{}, comp.Len())
		for j := 0; j < comp.Len(); j++ {
			if !comp.IsNA(j) {
				set[comp.Record(j)] = struct{}{}
			}
		}
		for i, v := range elems.data {
			if !elems.isValid(i) {
				continue
			}
			_, found := set[v]
			if found == in {
				mask.set(i)
			}
		}
	case boolElements:
		var haveTrue, haveFalse bool
		for j := 0; j < comp.Len(); j++ {
			if !comp.IsNA(j) {
				v, _ := comp.BoolAt(j)
				if v {
					haveTrue = true
				} else {
					haveFalse = true
				}
			}
		}
		for i, v := range elems.data {
			if !elems.isValid(i) {
				continue
			}
			found := (v && haveTrue) || (!v && haveFalse)
			if found == in {
				mask.set(i)
			}
		}
	case timeElements:
		set := make(map[time.Time]struct{}, comp.Len())
		for j := 0; j < comp.Len(); j++ {
			if !comp.IsNA(j) {
				v, _ := comp.TimeAt(j)
				set[v] = struct{}{}
			}
		}
		for i, v := range elems.data {
			if !elems.isValid(i) {
				continue
			}
			_, found := set[v]
			if found == in {
				mask.set(i)
			}
		}
	}
	return mask
}

func cmpInt64(a, b int64, op Comparator) bool {
	switch op {
	case Eq:
		return a == b
	case Neq:
		return a != b
	case Greater:
		return a > b
	case GreaterEq:
		return a >= b
	case Less:
		return a < b
	case LessEq:
		return a <= b
	}
	return false
}

func cmpFloat64(a, b float64, op Comparator) bool {
	switch op {
	case Eq:
		return a == b
	case Neq:
		return a != b
	case Greater:
		return a > b
	case GreaterEq:
		return a >= b
	case Less:
		return a < b
	case LessEq:
		return a <= b
	}
	return false
}

func cmpString(a, b string, op Comparator) bool {
	switch op {
	case Eq:
		return a == b
	case Neq:
		return a != b
	case Greater:
		return a > b
	case GreaterEq:
		return a >= b
	case Less:
		return a < b
	case LessEq:
		return a <= b
	}
	return false
}

// cmpBool keeps the v1 ordering semantics: false < true.
func cmpBool(a, b bool, op Comparator) bool {
	switch op {
	case Eq:
		return a == b
	case Neq:
		return a != b
	case Greater:
		return a && !b
	case GreaterEq:
		return a || !b
	case Less:
		return !a && b
	case LessEq:
		return !a || b
	}
	return false
}

func cmpTime(a, b time.Time, op Comparator) bool {
	switch op {
	case Eq:
		return a.Equal(b)
	case Neq:
		return !a.Equal(b)
	case Greater:
		return a.After(b)
	case GreaterEq:
		return a.After(b) || a.Equal(b)
	case Less:
		return a.Before(b)
	case LessEq:
		return a.Before(b) || a.Equal(b)
	}
	return false
}

func compareInt64(data []int64, validity *bitmap, op Comparator, operand int64, mask *bitmap) {
	for i, v := range data {
		if validity != nil && !validity.get(i) {
			continue
		}
		if cmpInt64(v, operand, op) {
			mask.set(i)
		}
	}
}

func compareFloat64(data []float64, validity *bitmap, op Comparator, operand float64, mask *bitmap) {
	for i, v := range data {
		if validity != nil && !validity.get(i) {
			continue
		}
		if cmpFloat64(v, operand, op) {
			mask.set(i)
		}
	}
}

func compareString(data []string, validity *bitmap, op Comparator, operand string, mask *bitmap) {
	for i, v := range data {
		if validity != nil && !validity.get(i) {
			continue
		}
		if cmpString(v, operand, op) {
			mask.set(i)
		}
	}
}

func compareBool(data []bool, validity *bitmap, op Comparator, operand bool, mask *bitmap) {
	for i, v := range data {
		if validity != nil && !validity.get(i) {
			continue
		}
		if cmpBool(v, operand, op) {
			mask.set(i)
		}
	}
}

func compareTime(data []time.Time, validity *bitmap, op Comparator, operand time.Time, mask *bitmap) {
	for i, v := range data {
		if validity != nil && !validity.get(i) {
			continue
		}
		if cmpTime(v, operand, op) {
			mask.set(i)
		}
	}
}

// RowLess compares two valid rows of s by typed value; it is the sort
// comparator shared by Series.Order and the parallel arrange path.
func RowLess(s Series, i, j int) bool {
	switch elems := s.elements.(type) {
	case intElements:
		return elems.data[i] < elems.data[j]
	case floatElements:
		return elems.data[i] < elems.data[j]
	case stringElements:
		return elems.data[i] < elems.data[j]
	case boolElements:
		return !elems.data[i] && elems.data[j]
	case timeElements:
		return elems.data[i].Before(elems.data[j])
	case dictionaryElements:
		a, _ := elems.strAt(i)
		b, _ := elems.strAt(j)
		return a < b
	}
	return false
}

// RowGreater compares two valid rows of s by typed value.
func RowGreater(s Series, i, j int) bool {
	switch elems := s.elements.(type) {
	case intElements:
		return elems.data[i] > elems.data[j]
	case floatElements:
		return elems.data[i] > elems.data[j]
	case stringElements:
		return elems.data[i] > elems.data[j]
	case boolElements:
		return elems.data[i] && !elems.data[j]
	case timeElements:
		return elems.data[i].After(elems.data[j])
	case dictionaryElements:
		a, _ := elems.strAt(i)
		b, _ := elems.strAt(j)
		return a > b
	}
	return false
}

// MapFloat64 applies f element-wise over a Float series as a masked loop
// over the buffer (RFC §9.4 ScalarFunc support): missing stays missing.
// Non-Float inputs are first converted through FloatAt.
func MapFloat64(s Series, f func(float64) float64) Series {
	if s.Err != nil {
		return s
	}
	if elems, ok := s.elements.(floatElements); ok {
		out := newColumn[float64](len(elems.data))
		for i, v := range elems.data {
			if !elems.isValid(i) {
				out.setNA(i)
				continue
			}
			out.data[i] = f(v)
		}
		return Series{Name: s.Name, t: Float, elements: out}
	}
	out := newColumnCap[float64](s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNA(i) {
			out.appendNA()
			continue
		}
		out.append(f(s.FloatAt(i)))
	}
	return Series{Name: s.Name, t: Float, elements: out}
}

// MapInt64 applies f element-wise over an Int series as a masked loop over
// the buffer (RFC §9.4 ScalarFunc support): missing stays missing. Non-Int
// inputs convert row by row; unconvertible rows become missing.
func MapInt64(s Series, f func(int64) int64) Series {
	if s.Err != nil {
		return s
	}
	if elems, ok := s.elements.(intElements); ok {
		out := newColumn[int64](len(elems.data))
		for i, v := range elems.data {
			if !elems.isValid(i) {
				out.setNA(i)
				continue
			}
			out.data[i] = f(v)
		}
		return Series{Name: s.Name, t: Int, elements: out}
	}
	out := newColumnCap[int64](s.Len())
	for i := 0; i < s.Len(); i++ {
		v, err := s.Int64At(i)
		if err != nil {
			out.appendNA()
			continue
		}
		out.append(f(v))
	}
	return Series{Name: s.Name, t: Int, elements: out}
}

// GatherRows returns the rows at idx; negative indexes become missing
// values. It powers batched join output assembly.
func (s Series) GatherRows(idx []int) Series {
	if s.Err != nil {
		return s
	}
	gather := func() store {
		switch elems := s.elements.(type) {
		case intElements:
			return gatherMasked(elems, idx)
		case floatElements:
			return gatherMasked(elems, idx)
		case stringElements:
			return gatherMasked(elems, idx)
		case boolElements:
			return gatherMasked(elems, idx)
		case timeElements:
			return gatherMasked(elems, idx)
		case dictionaryElements:
			return elems.gatherStore(idx)
		}
		return nil
	}
	return Series{Name: s.Name, t: s.t, elements: gather()}
}

func gatherMasked[T any](c column[T], idx []int) column[T] {
	data := make([]T, len(idx))
	var validity *bitmap
	for k, i := range idx {
		if i < 0 {
			if validity == nil {
				validity = newBitmap(len(idx))
			}
			validity.clear(k)
			continue
		}
		data[k] = c.data[i]
		if !c.isValid(i) {
			if validity == nil {
				validity = newBitmap(len(idx))
			}
			validity.clear(k)
		}
	}
	return column[T]{data: data, validity: validity}
}

// CombineRows builds a column where row r comes from a at rowsA[r] when that
// entry is non-negative, otherwise from b at rowsB[r]. It powers batched
// join key-column assembly, where matched and left-only rows take the left
// key value and right-only rows take the right one. Same-type inputs use a
// typed buffer loop; cross-type inputs convert through the parse kernels.
func CombineRows(a, b Series, rowsA, rowsB []int) Series {
	n := len(rowsA)
	if a.t == b.t {
		switch ae := a.elements.(type) {
		case intElements:
			be := b.elements.(intElements)
			return Series{Name: a.Name, t: a.t, elements: combineBuffers(ae, be, rowsA, rowsB, n)}
		case floatElements:
			be := b.elements.(floatElements)
			return Series{Name: a.Name, t: a.t, elements: combineBuffers(ae, be, rowsA, rowsB, n)}
		case stringElements:
			be := b.elements.(stringElements)
			return Series{Name: a.Name, t: a.t, elements: combineBuffers(ae, be, rowsA, rowsB, n)}
		case boolElements:
			be := b.elements.(boolElements)
			return Series{Name: a.Name, t: a.t, elements: combineBuffers(ae, be, rowsA, rowsB, n)}
		case timeElements:
			be := b.elements.(timeElements)
			return Series{Name: a.Name, t: a.t, elements: combineBuffers(ae, be, rowsA, rowsB, n)}
		}
	}
	out := emptyStore(a.t, 0)
	for r := 0; r < n; r++ {
		if rowsA[r] >= 0 {
			appendFromRow(&out, a, rowsA[r])
		} else {
			appendFromRow(&out, b, rowsB[r])
		}
	}
	return Series{Name: a.Name, t: a.t, elements: out}
}

func combineBuffers[T any](a, b column[T], rowsA, rowsB []int, n int) column[T] {
	data := make([]T, n)
	var validity *bitmap
	markInvalid := func(k int) {
		if validity == nil {
			validity = newBitmap(n)
		}
		validity.clear(k)
	}
	for r := 0; r < n; r++ {
		if rowsA[r] >= 0 {
			i := rowsA[r]
			data[r] = a.data[i]
			if !a.isValid(i) {
				markInvalid(r)
			}
		} else {
			j := rowsB[r]
			data[r] = b.data[j]
			if !b.isValid(j) {
				markInvalid(r)
			}
		}
	}
	return column[T]{data: data, validity: validity}
}

// appendFromRow appends src's row to the store, preserving missingness.
func appendFromRow(st *store, src Series, row int) {
	if src.IsNA(row) {
		appendAs(st, src.t, "NaN")
		return
	}
	appendAs(st, src.t, src.Val(row))
}
