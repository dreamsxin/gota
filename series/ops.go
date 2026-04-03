package series

import (
	"fmt"
	"math"
)

// Clip returns a new Series with values clipped to [lower, upper].
// Pass nil for no bound. Non-numeric types are returned unchanged.
//
// Example:
//
//	s.Clip(nil, &upper)   // only upper bound
//	s.Clip(&lower, &upper)
func (s Series) Clip(lower, upper *float64) Series {
	if s.Err != nil {
		return s
	}
	if s.t != Float && s.t != Int {
		return s.Copy()
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		out[i] = v
		if lower != nil && v < *lower {
			out[i] = *lower
		}
		if upper != nil && v > *upper {
			out[i] = *upper
		}
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Replace returns a new Series where every element equal to toReplace is
// replaced with with. Pass nil as with to set elements to NaN.
//
// Example:
//
//	s.Replace("N/A", nil)
//	s.Replace(0, 1)
func (s Series) Replace(toReplace, with interface{}) Series {
	if s.Err != nil {
		return s
	}
	elems := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		val := s.Elem(i).Val()
		if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", toReplace) {
			elems[i] = with
		} else {
			elems[i] = val
		}
	}
	result := New(elems, s.t, s.Name)
	return result
}

// Between returns a Bool Series indicating whether each element is within
// [left, right] (or open/half-open depending on inclusive).
//
// inclusive options: "both" (default), "neither", "left", "right"
//
// Example:
//
//	s.Between(18, 65, "both")
func (s Series) Between(left, right float64, inclusive string) Series {
	if s.Err != nil {
		return Series{Err: s.Err}
	}
	floats := s.Float()
	bools := make([]bool, len(floats))
	for i, v := range floats {
		if math.IsNaN(v) {
			bools[i] = false
			continue
		}
		switch inclusive {
		case "neither":
			bools[i] = v > left && v < right
		case "left":
			bools[i] = v >= left && v < right
		case "right":
			bools[i] = v > left && v <= right
		default: // "both"
			bools[i] = v >= left && v <= right
		}
	}
	result := Bools(bools)
	result.Name = s.Name
	return result
}

// IsIn returns a Bool Series indicating whether each element is in values.
//
// Example:
//
//	s.IsIn([]interface{}{"US", "UK", "CA"})
func (s Series) IsIn(values []interface{}) Series {
	if s.Err != nil {
		return Series{Err: s.Err}
	}
	lookup := make(map[string]struct{}, len(values))
	for _, v := range values {
		lookup[fmt.Sprintf("%v", v)] = struct{}{}
	}
	bools := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		key := fmt.Sprintf("%v", s.Elem(i).Val())
		_, bools[i] = lookup[key]
	}
	result := Bools(bools)
	result.Name = s.Name
	return result
}
