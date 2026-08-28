package series

import "math/bits"

// bitmap is the validity mask of a column buffer: one bit per row, bit set
// means the value is valid. A nil *bitmap on a column means all rows are
// valid, which is the fast common case.
type bitmap struct {
	words []uint64
	n     int // number of logical bits, always equal to the column length
}

func wordsFor(n int) int { return (n + 63) / 64 }

// newBitmap returns a bitmap with n bits, all set to valid.
func newBitmap(n int) *bitmap {
	b := &bitmap{words: make([]uint64, wordsFor(n)), n: n}
	for w := range b.words {
		b.words[w] = ^uint64(0)
	}
	return b
}

func (b *bitmap) get(i int) bool {
	return b.words[i>>6]&(1<<(i&63)) != 0
}

func (b *bitmap) set(i int) {
	b.words[i>>6] |= 1 << (i & 63)
}

func (b *bitmap) clear(i int) {
	b.words[i>>6] &^= 1 << (i & 63)
}

// clearRange marks [lo, hi) as invalid.
func (b *bitmap) clearRange(lo, hi int) {
	for i := lo; i < hi; i++ {
		b.clear(i)
	}
}

// push appends one validity bit.
func (b *bitmap) push(valid bool) {
	if b.n == len(b.words)*64 {
		b.words = append(b.words, 0)
	}
	if valid {
		b.set(b.n)
	} else {
		b.clear(b.n)
	}
	b.n++
}

func (b *bitmap) clone() *bitmap {
	if b == nil {
		return nil
	}
	c := &bitmap{words: make([]uint64, len(b.words)), n: b.n}
	copy(c.words, b.words)
	return c
}

// countInvalid counts invalid bits among the first n bits.
func (b *bitmap) countInvalid(n int) int {
	if b == nil {
		return 0
	}
	invalid := 0
	for i := 0; i < n; i++ {
		if !b.get(i) {
			invalid++
		}
	}
	return invalid
}

// Selection-mask helpers. The same bitmap type doubles as a row-selection
// mask for the filter kernels (RFC §5 rule 1): bit set means the row is
// selected. All combinators require equal logical length.

// newSelectionMask returns an all-zero mask of n bits (nothing selected).
func newSelectionMask(n int) *bitmap {
	return &bitmap{words: make([]uint64, wordsFor(n)), n: n}
}

// newAllSelectedMask returns an all-one mask of n bits. Tail bits beyond n
// in the last word are cleared so word-wise combination stays exact.
func newAllSelectedMask(n int) *bitmap {
	b := &bitmap{words: make([]uint64, wordsFor(n)), n: n}
	for w := range b.words {
		b.words[w] = ^uint64(0)
	}
	b.clearTail()
	return b
}

// clearTail zeroes bits at positions >= n in the last word.
func (b *bitmap) clearTail() {
	if rem := b.n & 63; rem != 0 && len(b.words) > 0 {
		b.words[len(b.words)-1] &= (uint64(1) << rem) - 1
	}
}

// andInto sets b = b & other (word-wise, allocation-free).
func (b *bitmap) andInto(other *bitmap) {
	for w := range b.words {
		b.words[w] &= other.words[w]
	}
}

// orInto sets b = b | other (word-wise, allocation-free).
func (b *bitmap) orInto(other *bitmap) {
	for w := range b.words {
		b.words[w] |= other.words[w]
	}
}

// rows returns the indexes of all set bits in ascending order.
func (b *bitmap) rows() []int {
	idx := make([]int, 0, b.n/8+1)
	for w, word := range b.words {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			row := w<<6 + bit
			if row >= b.n {
				return idx
			}
			idx = append(idx, row)
			word &^= 1 << bit
		}
	}
	return idx
}

// maskFromBools converts a selection slice into a mask.
func maskFromBools(bools []bool) *bitmap {
	b := newSelectionMask(len(bools))
	for i, v := range bools {
		if v {
			b.set(i)
		}
	}
	return b
}

// toBoolColumn materializes the mask as a Bool column buffer (true where the
// bit is set), used to keep the public Compare signature.
func (b *bitmap) toBoolColumn() boolElements {
	col := newColumn[bool](b.n)
	for i := 0; i < b.n; i++ {
		col.data[i] = b.get(i)
	}
	return col
}
