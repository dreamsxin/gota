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

// ============================================================================
// Math operations
// ============================================================================

// Abs returns a new Float Series with the absolute value of each element.
// Non-numeric types are returned as NaN.
//
// Example:
//
//	s.Abs()
func (s Series) Abs() Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		out[i] = math.Abs(v)
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Round returns a new Float Series with each element rounded to the given
// number of decimal places. Negative places rounds to tens, hundreds, etc.
//
// Example:
//
//	s.Round(2)  // round to 2 decimal places
//	s.Round(0)  // round to nearest integer
func (s Series) Round(places int) Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	factor := math.Pow(10, float64(places))
	out := make([]float64, len(floats))
	for i, v := range floats {
		if math.IsNaN(v) {
			out[i] = v
		} else {
			out[i] = math.Round(v*factor) / factor
		}
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Sign returns a new Float Series with the sign of each element:
// -1 for negative, 0 for zero, +1 for positive, NaN for NaN.
//
// Example:
//
//	s.Sign()
func (s Series) Sign() Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		switch {
		case math.IsNaN(v):
			out[i] = math.NaN()
		case v > 0:
			out[i] = 1
		case v < 0:
			out[i] = -1
		default:
			out[i] = 0
		}
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Pow returns a new Float Series with each element raised to the given power.
//
// Example:
//
//	s.Pow(2)  // square each element
func (s Series) Pow(exp float64) Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		out[i] = math.Pow(v, exp)
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Sqrt returns a new Float Series with the square root of each element.
// Negative values produce NaN.
//
// Example:
//
//	s.Sqrt()
func (s Series) Sqrt() Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		out[i] = math.Sqrt(v)
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Log returns a new Float Series with the natural logarithm of each element.
// Non-positive values produce NaN.
func (s Series) Log() Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		out[i] = math.Log(v)
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Log10 returns a new Float Series with the base-10 logarithm of each element.
func (s Series) Log10() Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		out[i] = math.Log10(v)
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}

// Exp returns a new Float Series with e raised to each element.
func (s Series) Exp() Series {
	if s.Err != nil {
		return s
	}
	floats := s.Float()
	out := make([]float64, len(floats))
	for i, v := range floats {
		out[i] = math.Exp(v)
	}
	result := Floats(out)
	result.Name = s.Name
	return result
}
