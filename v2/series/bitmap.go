package series

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
