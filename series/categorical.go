package series

import (
	"fmt"
	"sort"
)

// Categorical represents a memory-efficient Series for low-cardinality string
// data using dictionary encoding. Internally it stores a sorted slice of unique
// category strings (the dictionary) and a []int32 code array (one entry per row).
//
// Compared to a plain String Series, a Categorical Series uses significantly
// less memory when the number of distinct values is small relative to the total
// number of rows (e.g. country codes, status labels, enum-like columns).
//
// A Categorical can be converted to/from a regular String Series at any time.
type Categorical struct {
	// categories is the sorted dictionary of unique string values.
	categories []string
	// codes[i] is the index into categories for row i. -1 means NaN/missing.
	codes []int32
	// Name of the column.
	Name string
	// vcCache is a lazily computed ValueCounts cache; nil means stale.
	vcCache map[string]int
}

// NewCategorical creates a Categorical from a slice of strings.
// nil or empty strings are treated as NaN (code = -1).
//
// Example:
//
//	cat := series.NewCategorical([]string{"US", "UK", "US", "DE", "UK"}, "country")
func NewCategorical(values []string, name string) Categorical {
	// Build dictionary.
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		if v != "" {
			seen[v] = struct{}{}
		}
	}
	cats := make([]string, 0, len(seen))
	for k := range seen {
		cats = append(cats, k)
	}
	sort.Strings(cats)

	// Build reverse lookup.
	lookup := make(map[string]int32, len(cats))
	for i, c := range cats {
		lookup[c] = int32(i)
	}

	codes := make([]int32, len(values))
	for i, v := range values {
		if v == "" {
			codes[i] = -1
		} else {
			codes[i] = lookup[v]
		}
	}
	return Categorical{categories: cats, codes: codes, Name: name}
}

// CategoricalFromSeries converts a String Series to a Categorical.
// NaN elements become code -1.
func CategoricalFromSeries(s Series) (Categorical, error) {
	if s.Type() != String {
		return Categorical{}, fmt.Errorf("CategoricalFromSeries: expected String series, got %v", s.Type())
	}
	vals := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.Elem(i)
		if e.IsNA() {
			vals[i] = ""
		} else {
			vals[i] = e.String()
		}
	}
	return NewCategorical(vals, s.Name), nil
}

// ToSeries converts the Categorical back to a regular String Series.
func (c Categorical) ToSeries() Series {
	vals := make([]interface{}, len(c.codes))
	for i, code := range c.codes {
		if code < 0 {
			vals[i] = nil
		} else {
			vals[i] = c.categories[code]
		}
	}
	s := New(vals, String, c.Name)
	return s
}

// Len returns the number of rows.
func (c Categorical) Len() int { return len(c.codes) }

// NCategories returns the number of distinct categories (excluding NaN).
func (c Categorical) NCategories() int { return len(c.categories) }

// Categories returns a copy of the sorted category dictionary.
func (c Categorical) Categories() []string {
	out := make([]string, len(c.categories))
	copy(out, c.categories)
	return out
}

// Get returns the string value at row i, or "" if NaN.
func (c Categorical) Get(i int) string {
	if i < 0 || i >= len(c.codes) {
		return ""
	}
	code := c.codes[i]
	if code < 0 {
		return ""
	}
	return c.categories[code]
}

// IsNA returns true if row i is missing.
func (c Categorical) IsNA(i int) bool {
	if i < 0 || i >= len(c.codes) {
		return true
	}
	return c.codes[i] < 0
}

// ValueCounts returns a map from category string to occurrence count.
// NaN rows are not counted. The result is lazily cached and invalidated
// automatically when SetValue or AddCategory is called.
func (c *Categorical) ValueCounts() map[string]int {
	if c.vcCache != nil {
		// Return a copy so callers can't mutate the cache.
		out := make(map[string]int, len(c.vcCache))
		for k, v := range c.vcCache {
			out[k] = v
		}
		return out
	}
	counts := make(map[string]int, len(c.categories))
	for _, code := range c.codes {
		if code >= 0 {
			counts[c.categories[code]]++
		}
	}
	// Store a copy in the cache.
	c.vcCache = make(map[string]int, len(counts))
	for k, v := range counts {
		c.vcCache[k] = v
	}
	return counts
}

// Rename returns a new Categorical with the given name.
func (c Categorical) Rename(name string) Categorical {
	return Categorical{categories: c.categories, codes: c.codes, Name: name}
}

// MemoryBytes returns an estimate of the memory used by this Categorical.
// For comparison, a plain String Series would use roughly Len()*avgStringLen bytes.
func (c Categorical) MemoryBytes() int {
	dictBytes := 0
	for _, cat := range c.categories {
		dictBytes += len(cat) + 16 // string header
	}
	return dictBytes + len(c.codes)*4 // int32 per row
}

// AddCategory adds a new category to the dictionary without assigning it to
// any row. Useful for pre-defining valid values.
func (c *Categorical) AddCategory(cat string) {
	for _, existing := range c.categories {
		if existing == cat {
			return
		}
	}
	c.categories = append(c.categories, cat)
	sort.Strings(c.categories)
	c.vcCache = nil // invalidate cache
}

// SetValue sets the value at row i. Returns an error if cat is not in the
// dictionary. Use AddCategory first to extend the dictionary.
func (c *Categorical) SetValue(i int, cat string) error {
	if i < 0 || i >= len(c.codes) {
		return fmt.Errorf("Categorical.SetValue: index %d out of range", i)
	}
	if cat == "" {
		c.codes[i] = -1
		c.vcCache = nil // invalidate cache
		return nil
	}
	for j, existing := range c.categories {
		if existing == cat {
			c.codes[i] = int32(j)
			c.vcCache = nil // invalidate cache
			return nil
		}
	}
	return fmt.Errorf("Categorical.SetValue: %q is not in the category dictionary; call AddCategory first", cat)
}

// Filter returns a new Categorical containing only the rows where mask[i] is true.
func (c Categorical) Filter(mask []bool) (Categorical, error) {
	if len(mask) != len(c.codes) {
		return Categorical{}, fmt.Errorf("Categorical.Filter: mask length %d != series length %d", len(mask), len(c.codes))
	}
	var newCodes []int32
	for i, keep := range mask {
		if keep {
			newCodes = append(newCodes, c.codes[i])
		}
	}
	return Categorical{categories: c.categories, codes: newCodes, Name: c.Name}, nil
}
