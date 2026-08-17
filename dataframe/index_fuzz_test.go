package dataframe

import (
	"testing"

	"github.com/dreamsxin/gota/series"
)

// indexAlphabet mixes duplicate labels, empty strings, and separator-like
// values to stress index lookups and the length-prefixed MultiIndex key
// encoding fixed in v1.2.1.
var indexAlphabet = []string{"a", "b", "", "a_b", "1:a", "b|c"}

func pickIndexLabel(s string, i int, tag int) string {
	if len(s) == 0 {
		return indexAlphabet[i%len(indexAlphabet)]
	}
	return indexAlphabet[int(s[(i+tag)%len(s)])%len(indexAlphabet)]
}

// FuzzIndexLoc checks Index-backed Loc and LocSlice results against a
// brute-force reference over the label slice.
func FuzzIndexLoc(f *testing.F) {
	f.Add("labels", uint8(6))
	f.Add("", uint8(1))
	f.Add("\x00|1:a", uint8(9))

	f.Fuzz(func(t *testing.T, s string, n uint8) {
		rows := int(n) % 10
		if rows == 0 {
			t.Skip()
		}

		labels := make([]string, rows)
		for i := range labels {
			labels[i] = pickIndexLabel(s, i, 0)
		}

		df := New(series.New(rangeInts(rows), series.Int, "row"))
		idf, err := df.WithIndex(NewIndex(labels))
		if err != nil {
			t.Fatalf("WithIndex: %v", err)
		}

		for _, label := range labels {
			got := idf.Loc(label)
			if got.Err != nil {
				t.Fatalf("Loc(%q): %v", label, got.Err)
			}
			want := 0
			for _, l := range labels {
				if l == label {
					want++
				}
			}
			if got.Nrow() != want {
				t.Fatalf("Loc(%q) rows = %d, want %d (labels %v)",
					label, got.Nrow(), want, labels)
			}
		}

		// LocSlice(start, end) covers positions first(start)..first(end).
		start, end := labels[0], labels[rows-1]
		si, ei := -1, -1
		for i, l := range labels {
			if l == start && si < 0 {
				si = i
			}
			if l == end && ei < 0 {
				ei = i
			}
		}
		got := idf.LocSlice(start, end)
		if si > ei {
			if got.Err == nil {
				t.Fatalf("LocSlice(%q,%q) should fail when start is after end", start, end)
			}
		} else {
			if got.Err != nil {
				t.Fatalf("LocSlice(%q,%q): %v", start, end, got.Err)
			}
			if want := ei - si + 1; got.Nrow() != want {
				t.Fatalf("LocSlice(%q,%q) rows = %d, want %d", start, end, got.Nrow(), want)
			}
		}

		// Missing labels must error, not panic.
		if got := idf.Loc("__missing__"); got.Err == nil {
			t.Fatal("Loc(missing) should return an error")
		}
		if got := idf.LocSlice("__missing__", "a"); got.Err == nil {
			t.Fatal("LocSlice(missing start) should return an error")
		}
	})
}

// FuzzMultiIndexLoc checks partial and full MultiIndex lookups against a
// brute-force reference over the level tuples.
func FuzzMultiIndexLoc(f *testing.F) {
	f.Add("levels", uint8(6))
	f.Add("", uint8(4))
	f.Add("1:a|b_c", uint8(8))

	f.Fuzz(func(t *testing.T, s string, n uint8) {
		rows := int(n) % 10
		if rows == 0 {
			t.Skip()
		}

		l0 := make([]string, rows)
		l1 := make([]string, rows)
		for i := 0; i < rows; i++ {
			l0[i] = pickIndexLabel(s, i, 0)
			l1[i] = pickIndexLabel(s, i, 2)
		}

		df := New(series.New(rangeInts(rows), series.Int, "row"))
		mi, err := NewMultiIndex(l0, l1)
		if err != nil {
			t.Fatalf("NewMultiIndex: %v", err)
		}
		midf, err := df.WithMultiIndex(mi)
		if err != nil {
			t.Fatalf("WithMultiIndex: %v", err)
		}

		// Full-key lookups must match exactly the rows whose tuple equals
		// the key; no cross-level bleed from separator-like labels.
		for i := 0; i < rows; i++ {
			got := midf.Loc(l0[i], l1[i])
			if got.Err != nil {
				t.Fatalf("Loc(%q,%q): %v", l0[i], l1[i], got.Err)
			}
			want := 0
			for j := 0; j < rows; j++ {
				if l0[j] == l0[i] && l1[j] == l1[i] {
					want++
				}
			}
			if got.Nrow() != want {
				t.Fatalf("Loc(%q,%q) rows = %d, want %d (levels %v / %v)",
					l0[i], l1[i], got.Nrow(), want, l0, l1)
			}
		}

		// Partial-key lookups must match all rows of the level-0 label.
		for i := 0; i < rows; i++ {
			got := midf.Loc(l0[i])
			if got.Err != nil {
				t.Fatalf("Loc(%q): %v", l0[i], got.Err)
			}
			want := 0
			for j := 0; j < rows; j++ {
				if l0[j] == l0[i] {
					want++
				}
			}
			if got.Nrow() != want {
				t.Fatalf("Loc(%q) rows = %d, want %d (levels %v / %v)",
					l0[i], got.Nrow(), want, l0, l1)
			}
		}

		// Unknown partial key must error, not panic.
		if got := midf.Loc("__missing__"); got.Err == nil {
			t.Fatal("Loc(missing) should return an error")
		}
	})
}
