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

// ============================================================================
// Statistical operations — Mode, Skewness, Kurtosis
// ============================================================================

// Mode returns the most frequent value in the Series as a new single-element
// Series of the same type. If multiple values tie, the lexicographically
// smallest string representation is returned. NaN values are ignored.
// Returns an empty Series if all values are NaN or the Series is empty.
//
// Example:
//
//	s.Mode()
func (s Series) Mode() Series {
	if s.Err != nil {
		return s
	}
	counts := make(map[string]int, s.Len())
	order := make([]string, 0, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.Elem(i)
		if e.IsNA() {
			continue
		}
		k := e.String()
		if counts[k] == 0 {
			order = append(order, k)
		}
		counts[k]++
	}
	if len(counts) == 0 {
		return s.Empty()
	}
	maxCount := 0
	for _, c := range counts {
		if c > maxCount {
			maxCount = c
		}
	}
	var modes []string
	for _, k := range order {
		if counts[k] == maxCount {
			modes = append(modes, k)
		}
	}
	// Return the lexicographically smallest mode for determinism.
	best := modes[0]
	for _, m := range modes[1:] {
		if m < best {
			best = m
		}
	}
	result := New([]string{best}, s.t, s.Name)
	return result
}

// Skew returns the sample skewness of the Series using the adjusted Fisher-Pearson
// standardized moment coefficient (same as pandas/Excel).
// Returns NaN if n < 3 or all values are NaN.
//
// Example:
//
//	s.Skew()
func (s Series) Skew() float64 {
	if s.Err != nil {
		return math.NaN()
	}
	var vals []float64
	for i := 0; i < s.Len(); i++ {
		e := s.Elem(i)
		if !e.IsNA() {
			vals = append(vals, e.Float())
		}
	}
	n := float64(len(vals))
	if n < 3 {
		return math.NaN()
	}
	// Compute mean and std.
	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / n
	var m2, m3 float64
	for _, v := range vals {
		d := v - mean
		m2 += d * d
		m3 += d * d * d
	}
	variance := m2 / (n - 1)
	if variance == 0 {
		return math.NaN()
	}
	std := math.Sqrt(variance)
	// Adjusted Fisher-Pearson: G1 = (n/((n-1)*(n-2))) * sum((x-mean)^3/std^3)
	skew := (n / ((n - 1) * (n - 2))) * (m3 / math.Pow(std, 3))
	return skew
}

// Kurt returns the excess kurtosis of the Series using the unbiased estimator
// (same as pandas default: Fisher's definition, normal == 0).
// Returns NaN if n < 4 or all values are NaN.
//
// Example:
//
//	s.Kurt()
func (s Series) Kurt() float64 {
	if s.Err != nil {
		return math.NaN()
	}
	var vals []float64
	for i := 0; i < s.Len(); i++ {
		e := s.Elem(i)
		if !e.IsNA() {
			vals = append(vals, e.Float())
		}
	}
	n := float64(len(vals))
	if n < 4 {
		return math.NaN()
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / n
	var m2, m4 float64
	for _, v := range vals {
		d := v - mean
		d2 := d * d
		m2 += d2
		m4 += d2 * d2
	}
	if m2 == 0 {
		return math.NaN()
	}
	// Unbiased excess kurtosis (pandas formula):
	// k = (n*(n+1)/((n-1)*(n-2)*(n-3))) * sum((x-mean)^4/std^4) - 3*(n-1)^2/((n-2)*(n-3))
	variance := m2 / n
	kurt := (n*(n+1)/((n-1)*(n-2)*(n-3)))*(m4/(variance*variance)) - 3*(n-1)*(n-1)/((n-2)*(n-3))
	return kurt
}
