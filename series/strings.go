package series

import (
	"fmt"
	"strings"
)

// String-transformation methods. They require a String series and return a
// new String series; missing elements stay missing. Calling them on another
// type sets Err on the result.

func (s Series) checkStringOp(op string) (stringElements, Series) {
	if s.Err != nil {
		return stringElements{}, s
	}
	if s.t != String {
		return stringElements{}, Series{Err: fmt.Errorf("%s: requires a String series, got %s", op, s.t), Name: s.Name}
	}
	return s.elements.(stringElements), Series{}
}

func mapStrings(s Series, op string, f func(string) string) Series {
	elems, err := s.checkStringOp(op)
	if err.Err != nil {
		return err
	}
	out := newColumn[string](len(elems.data))
	for i, v := range elems.data {
		if !elems.isValid(i) {
			out.setNA(i)
			continue
		}
		out.data[i] = f(v)
	}
	return Series{Name: s.Name, t: String, elements: out}
}

// Upper returns a new Series with all string elements upper-cased.
func (s Series) Upper() Series { return mapStrings(s, "Upper", strings.ToUpper) }

// Lower returns a new Series with all string elements lower-cased.
func (s Series) Lower() Series { return mapStrings(s, "Lower", strings.ToLower) }

// TrimSpace returns a new Series with leading and trailing white space
// removed from every string element.
func (s Series) TrimSpace() Series { return mapStrings(s, "TrimSpace", strings.TrimSpace) }

// Trim returns a new Series with leading and trailing UTF-8-encoded code
// points in cutset removed from every string element.
func (s Series) Trim(cutset string) Series {
	return mapStrings(s, "Trim", func(v string) string { return strings.Trim(v, cutset) })
}

// TrimPrefix returns a new Series with prefix removed from the front of
// every string element that starts with it.
func (s Series) TrimPrefix(prefix string) Series {
	return mapStrings(s, "TrimPrefix", func(v string) string { return strings.TrimPrefix(v, prefix) })
}

// TrimSuffix returns a new Series with suffix removed from the end of every
// string element that ends with it.
func (s Series) TrimSuffix(suffix string) Series {
	return mapStrings(s, "TrimSuffix", func(v string) string { return strings.TrimSuffix(v, suffix) })
}

// ReplaceAll returns a new Series with every non-overlapping instance of
// old replaced by new in each string element. For value-level replacement
// (pandas replace) see Series.Replace.
func (s Series) ReplaceAll(old, new string) Series {
	return mapStrings(s, "ReplaceAll", func(v string) string { return strings.ReplaceAll(v, old, new) })
}

// mapStringPred evaluates a predicate per element and returns a Bool series
// where missing inputs stay missing.
func mapStringPred(s Series, op string, f func(string) bool) Series {
	elems, err := s.checkStringOp(op)
	if err.Err != nil {
		return err
	}
	out := newColumn[bool](len(elems.data))
	for i, v := range elems.data {
		if !elems.isValid(i) {
			out.setNA(i)
			continue
		}
		out.data[i] = f(v)
	}
	return Series{Name: s.Name, t: Bool, elements: out}
}

// Contains returns a Bool series reporting whether each string element
// contains substr.
func (s Series) Contains(substr string) Series {
	return mapStringPred(s, "Contains", func(v string) bool { return strings.Contains(v, substr) })
}

// StartsWith returns a Bool series reporting whether each string element
// starts with prefix.
func (s Series) StartsWith(prefix string) Series {
	return mapStringPred(s, "StartsWith", func(v string) bool { return strings.HasPrefix(v, prefix) })
}

// EndsWith returns a Bool series reporting whether each string element
// ends with suffix.
func (s Series) EndsWith(suffix string) Series {
	return mapStringPred(s, "EndsWith", func(v string) bool { return strings.HasSuffix(v, suffix) })
}
