package series

import (
	"fmt"
	"strconv"
	"time"
)

// dictionaryElements is the Dictionary DType storage (RFC §4): string values
// stored as int32 codes into a shared category slice. Code -1 marks a
// missing value. Series backed by this store report Type() == String; the
// dictionary identity is exposed through Series.DType().
type dictionaryElements struct {
	codes      []int32
	categories []string
	ordered    bool
	lookup     map[string]int32 // built lazily for encode paths
}

var _ store = dictionaryElements{}

func newDictionaryElements(categories []string, codes []int32, ordered bool) dictionaryElements {
	cats := make([]string, len(categories))
	copy(cats, categories)
	cd := make([]int32, len(codes))
	copy(cd, codes)
	return dictionaryElements{codes: cd, categories: cats, ordered: ordered}
}

func (d dictionaryElements) dType() DType {
	return dictionaryDType{categories: d.categories, ordered: d.ordered}
}

func (d dictionaryElements) storeLen() int { return len(d.codes) }

func (d dictionaryElements) storeHasNA() bool {
	for _, c := range d.codes {
		if c < 0 {
			return true
		}
	}
	return false
}

func (d dictionaryElements) storeIsNA() []bool {
	out := make([]bool, len(d.codes))
	for i, c := range d.codes {
		out[i] = c < 0
	}
	return out
}

func (d dictionaryElements) cloneStore() store {
	codes := make([]int32, len(d.codes))
	copy(codes, d.codes)
	// categories are immutable once built; share them.
	return dictionaryElements{codes: codes, categories: d.categories, ordered: d.ordered}
}

func (d dictionaryElements) gatherStore(idx []int) store {
	codes := make([]int32, len(idx))
	for k, i := range idx {
		codes[k] = d.codes[i]
	}
	return dictionaryElements{codes: codes, categories: d.categories, ordered: d.ordered}
}

// strAt returns the string value at row i; ok is false for missing rows.
func (d dictionaryElements) strAt(i int) (string, bool) {
	c := d.codes[i]
	if c < 0 {
		return "", false
	}
	return d.categories[c], true
}

func (d *dictionaryElements) ensureLookup() map[string]int32 {
	if d.lookup == nil {
		d.lookup = make(map[string]int32, len(d.categories))
		for i, c := range d.categories {
			d.lookup[c] = int32(i)
		}
	}
	return d.lookup
}

// encodeAppend appends the string values, extending the dictionary as
// needed. A negative entry in valid marks the row missing.
func (d *dictionaryElements) encodeAppend(vals []string, valid []bool) {
	lookup := d.ensureLookup()
	for i, v := range vals {
		if valid != nil && !valid[i] {
			d.codes = append(d.codes, -1)
			continue
		}
		code, ok := lookup[v]
		if !ok {
			code = int32(len(d.categories))
			d.categories = append(d.categories, v)
			lookup[v] = code
		}
		d.codes = append(d.codes, code)
	}
}

// encodeSetAt writes the encoded value at an existing row, extending the
// dictionary when the value is new.
func (d *dictionaryElements) encodeSetAt(i int, v string) {
	lookup := d.ensureLookup()
	code, ok := lookup[v]
	if !ok {
		code = int32(len(d.categories))
		d.categories = append(d.categories, v)
		lookup[v] = code
	}
	d.codes[i] = code
}

// appendStore appends another store's rows, encoding through the
// dictionary. String-like stores encode directly; other stores convert row
// by row through their record representation.
func (d dictionaryElements) appendStore(other store) store {
	switch o := other.(type) {
	case dictionaryElements:
		for _, c := range o.codes {
			if c < 0 {
				d.codes = append(d.codes, -1)
				continue
			}
			d.encodeAppend([]string{o.categories[c]}, nil)
		}
	case stringElements:
		vals := o.data
		var valid []bool
		if o.validity != nil {
			valid = make([]bool, len(vals))
			for i := range vals {
				valid[i] = o.isValid(i)
			}
		}
		d.encodeAppend(vals, valid)
	default:
		n := other.storeLen()
		recs := make([]string, n)
		valid := make([]bool, n)
		switch ot := other.(type) {
		case intElements:
			for i := 0; i < n; i++ {
				valid[i] = ot.isValid(i)
				if valid[i] {
					recs[i] = strconv.FormatInt(ot.data[i], 10)
				}
			}
		case floatElements:
			for i := 0; i < n; i++ {
				valid[i] = ot.isValid(i)
				if valid[i] {
					recs[i] = fmt.Sprintf("%f", ot.data[i])
				}
			}
		case boolElements:
			for i := 0; i < n; i++ {
				valid[i] = ot.isValid(i)
				if valid[i] {
					recs[i] = strconv.FormatBool(ot.data[i])
				}
			}
		case timeElements:
			for i := 0; i < n; i++ {
				valid[i] = ot.isValid(i)
				if valid[i] {
					recs[i] = ot.data[i].Format(time.RFC3339)
				}
			}
		}
		d.encodeAppend(recs, valid)
	}
	return d
}

// toStringColumn materializes the dictionary as a plain string buffer.
func (d dictionaryElements) toStringColumn() stringElements {
	col := newColumn[string](len(d.codes))
	for i, c := range d.codes {
		if c < 0 {
			col.setNA(i)
			continue
		}
		col.data[i] = d.categories[c]
	}
	return col
}

// NewDictionarySeries returns a Series backed by dictionary storage built
// from cat. The Series reports Type() == String and DType() Dictionary.
func NewDictionarySeries(cat Categorical, name string) Series {
	elems := dictionaryElements{
		codes:      append([]int32(nil), cat.codes...),
		categories: append([]string(nil), cat.categories...),
	}
	return Series{Name: name, t: String, elements: elems}
}

// EmptyDictionarySeries returns a zero-row dictionary-backed Series over
// categories, for schema-conforming output buffers and accumulators.
func EmptyDictionarySeries(categories []string, name string) Series {
	elems := dictionaryElements{
		categories: append([]string(nil), categories...),
	}
	return Series{Name: name, t: String, elements: elems}
}

// ToDictionarySeries converts the Categorical into a dictionary-backed
// Series (the Dictionary DType view). ToSeries keeps returning a plain
// String Series.
func (c Categorical) ToDictionarySeries() Series {
	return NewDictionarySeries(c, c.Name)
}

// DType returns the Dictionary DType describing this Categorical.
func (c Categorical) DType() DType {
	return NewDictionaryDType(c.categories, false)
}

// InternCategories returns a copy of c whose category strings are interned
// through ctx, deduplicating backing arrays across columns that share the
// same values (RFC §9.2 chain-local interning).
func (c Categorical) InternCategories(ctx *ExecutionContext) Categorical {
	if ctx == nil {
		return c
	}
	out := c
	out.categories = make([]string, len(c.categories))
	for i, cat := range c.categories {
		out.categories[i] = ctx.Intern(cat)
	}
	return out
}
