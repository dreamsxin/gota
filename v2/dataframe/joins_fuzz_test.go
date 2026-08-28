package dataframe

import (
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// joinAlphabet is a small key space so fuzzed joins always exercise
// duplicate keys and empty groups.
var joinAlphabet = []string{"k0", "k1", "k2", "k3"}

func pickJoinKey(s string, i int, tag int) string {
	if len(s) == 0 {
		return joinAlphabet[i%len(joinAlphabet)]
	}
	return joinAlphabet[int(s[(i+tag)%len(s)])%len(joinAlphabet)]
}

// FuzzInnerJoinCount checks InnerJoin row counts (single and composite keys)
// against a brute-force nested-loop reference. Composite-key values include
// delimiter-like strings to guard the collision-safe key encoding fixed in
// v1.2.1.
func FuzzInnerJoinCount(f *testing.F) {
	f.Add("keys", uint8(5), uint8(4))
	f.Add("", uint8(1), uint8(1))
	f.Add("\x00|\x00", uint8(8), uint8(3))

	f.Fuzz(func(t *testing.T, s string, n1, n2 uint8) {
		leftN := int(n1) % 9
		rightN := int(n2) % 9

		lk := make([]string, leftN)
		rv := make([]int, leftN)
		for i := 0; i < leftN; i++ {
			lk[i] = pickJoinKey(s, i, 0)
			rv[i] = i
		}
		rk := make([]string, rightN)
		wv := make([]int, rightN)
		for i := 0; i < rightN; i++ {
			rk[i] = pickJoinKey(s, i, 1)
			wv[i] = 100 + i
		}

		left := New(
			series.New(lk, series.String, "key"),
			series.New(rv, series.Int, "lv"),
		)
		right := New(
			series.New(rk, series.String, "key"),
			series.New(wv, series.Int, "rv"),
		)

		got := left.InnerJoin(right, "key")
		if got.Err != nil {
			t.Fatalf("InnerJoin: %v", got.Err)
		}

		want := 0
		for _, a := range lk {
			for _, b := range rk {
				if a == b {
					want++
				}
			}
		}
		if got.Nrow() != want {
			t.Fatalf("InnerJoin rows = %d, want %d (keys %v / %v)",
				got.Nrow(), want, lk, rk)
		}
		// Every output key must exist on both sides.
		leftKeys := make(map[string]bool, len(lk))
		for _, k := range lk {
			leftKeys[k] = true
		}
		rightKeys := make(map[string]bool, len(rk))
		for _, k := range rk {
			rightKeys[k] = true
		}
		for _, k := range got.Col("key").Records() {
			if !leftKeys[k] || !rightKeys[k] {
				t.Fatalf("InnerJoin produced key %q missing from an input", k)
			}
		}
	})
}

// compositeKeyValues includes strings crafted to collide under naive
// delimiter-based key encodings.
var compositeKeyValues = []string{"a", "b", "a|b", "", "\x00", "2:int", "1:a", "s,t"}

func pickComposite(s string, i int) string {
	if len(s) == 0 {
		return compositeKeyValues[i%len(compositeKeyValues)]
	}
	return compositeKeyValues[int(s[i%len(s)])%len(compositeKeyValues)]
}

// FuzzInnerJoinCompositeCount checks two-column InnerJoin counts against a
// brute-force reference on the key tuples.
func FuzzInnerJoinCompositeCount(f *testing.F) {
	f.Add("collide", uint8(6), uint8(6))
	f.Add("a|b", uint8(4), uint8(4))

	f.Fuzz(func(t *testing.T, s string, n1, n2 uint8) {
		leftN := int(n1) % 9
		rightN := int(n2) % 9

		mk := func(n int, tag int) ([]string, []string) {
			k1 := make([]string, n)
			k2 := make([]string, n)
			for i := 0; i < n; i++ {
				k1[i] = pickComposite(s, (i+tag)%len(compositeKeyValues))
				k2[i] = pickComposite(s, (i+tag*3)%len(compositeKeyValues))
			}
			return k1, k2
		}

		lk1, lk2 := mk(leftN, 0)
		rk1, rk2 := mk(rightN, 1)

		left := New(
			series.New(lk1, series.String, "k1"),
			series.New(lk2, series.String, "k2"),
			series.New(rangeInts(leftN), series.Int, "lv"),
		)
		right := New(
			series.New(rk1, series.String, "k1"),
			series.New(rk2, series.String, "k2"),
			series.New(rangeInts(rightN), series.Int, "rv"),
		)

		got := left.InnerJoin(right, "k1", "k2")
		if got.Err != nil {
			t.Fatalf("InnerJoin composite: %v", got.Err)
		}

		want := 0
		for i := range lk1 {
			for j := range rk1 {
				if lk1[i] == rk1[j] && lk2[i] == rk2[j] {
					want++
				}
			}
		}
		if got.Nrow() != want {
			t.Fatalf("InnerJoin composite rows = %d, want %d", got.Nrow(), want)
		}
	})
}

// FuzzLeftJoinCount checks LeftJoin row counts: each left row yields
// max(1, matching right rows).
func FuzzLeftJoinCount(f *testing.F) {
	f.Add("keys", uint8(5), uint8(5))
	f.Add("", uint8(0), uint8(7))

	f.Fuzz(func(t *testing.T, s string, n1, n2 uint8) {
		leftN := int(n1) % 9
		rightN := int(n2) % 9

		lk := make([]string, leftN)
		for i := 0; i < leftN; i++ {
			lk[i] = pickJoinKey(s, i, 0)
		}
		rk := make([]string, rightN)
		for i := 0; i < rightN; i++ {
			rk[i] = pickJoinKey(s, i, 1)
		}

		left := New(
			series.New(lk, series.String, "key"),
			series.New(rangeInts(leftN), series.Int, "lv"),
		)
		right := New(
			series.New(rk, series.String, "key"),
			series.New(rangeInts(rightN), series.Int, "rv"),
		)

		got := left.LeftJoin(right, "key")
		if got.Err != nil {
			t.Fatalf("LeftJoin: %v", got.Err)
		}

		want := 0
		for _, a := range lk {
			matches := 0
			for _, b := range rk {
				if a == b {
					matches++
				}
			}
			if matches == 0 {
				matches = 1
			}
			want += matches
		}
		if got.Nrow() != want {
			t.Fatalf("LeftJoin rows = %d, want %d (keys %v / %v)",
				got.Nrow(), want, lk, rk)
		}
	})
}

func rangeInts(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}
