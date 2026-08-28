package series

import "time"

// column is a contiguous typed value buffer with an optional validity
// bitmap, the v2 storage unit behind every Series. Slots marked invalid hold
// unspecified data; the bitmap is the single source of truth for missing
// values. A nil validity bitmap means every row is valid.
type column[T any] struct {
	data     []T
	validity *bitmap
}

// Concrete buffer types. The names carry over from v1 so existing type
// switches keep working against the new representation.
type (
	intElements    = column[int64]
	floatElements  = column[float64]
	stringElements = column[string]
	boolElements   = column[bool]
	timeElements   = column[time.Time]
)

// store is implemented by every column buffer held in Series.elements.
type store interface {
	storeLen() int
	storeHasNA() bool
	storeIsNA() []bool
	cloneStore() store
	gatherStore(idx []int) store
	appendStore(other store) store
}

func newColumn[T any](n int) column[T] {
	return column[T]{data: make([]T, n)}
}

func newColumnCap[T any](capacity int) column[T] {
	return column[T]{data: make([]T, 0, capacity)}
}

func (c column[T]) storeLen() int { return len(c.data) }

func (c column[T]) isValid(i int) bool {
	return c.validity == nil || c.validity.get(i)
}

func (c *column[T]) ensureValidity() {
	if c.validity == nil {
		c.validity = newBitmap(len(c.data))
	}
}

// setValue writes a valid value at position i.
func (c *column[T]) setValue(i int, v T) {
	c.data[i] = v
	if c.validity != nil {
		c.validity.set(i)
	}
}

// setNA marks position i as missing.
func (c *column[T]) setNA(i int) {
	c.ensureValidity()
	c.validity.clear(i)
}

func (c *column[T]) append(v T) {
	c.data = append(c.data, v)
	if c.validity != nil {
		c.validity.push(true)
	}
}

func (c *column[T]) appendNA() {
	c.ensureValidity()
	var zero T
	c.data = append(c.data, zero)
	c.validity.push(false)
}

// appendCol appends every row of other, preserving validity.
func (c *column[T]) appendCol(other column[T]) {
	base := len(c.data)
	c.data = append(c.data, other.data...)
	if c.validity != nil || other.validity != nil {
		c.ensureValidity()
		for i := 0; i < len(other.data); i++ {
			if !other.isValid(i) {
				c.validity.clear(base + i)
			}
		}
	}
}

// appendStore returns the buffer with every row of other appended. The
// argument must hold the same element type; mismatches are no-ops.
func (c column[T]) appendStore(other store) store {
	if o, ok := other.(column[T]); ok {
		c.appendCol(o)
	}
	return c
}

func (c column[T]) clone() column[T] {
	data := make([]T, len(c.data))
	copy(data, c.data)
	return column[T]{data: data, validity: c.validity.clone()}
}

func (c column[T]) cloneStore() store { return c.clone() }

// gather materializes the rows at idx into a new buffer.
func (c column[T]) gather(idx []int) column[T] {
	data := make([]T, len(idx))
	var validity *bitmap
	for k, i := range idx {
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

func (c column[T]) gatherStore(idx []int) store { return c.gather(idx) }

func (c column[T]) storeHasNA() bool {
	if c.validity == nil {
		return false
	}
	for i := 0; i < len(c.data); i++ {
		if !c.validity.get(i) {
			return true
		}
	}
	return false
}

func (c column[T]) storeIsNA() []bool {
	ret := make([]bool, len(c.data))
	if c.validity == nil {
		return ret
	}
	for i := range ret {
		ret[i] = !c.validity.get(i)
	}
	return ret
}
