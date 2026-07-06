package series

import "math"

// SumRows calculates the sum over selected row positions, skipping NaN values.
func (s Series) SumRows(rows []int) float64 {
	switch elems := s.elements.(type) {
	case floatElements:
		var sum float64
		for _, row := range rows {
			elem := elems[row]
			if !elem.IsNA() {
				sum += elem.e
			}
		}
		return sum
	case intElements:
		var sum float64
		for _, row := range rows {
			elem := elems[row]
			if !elem.IsNA() {
				sum += float64(elem.e)
			}
		}
		return sum
	default:
		var sum float64
		for _, row := range rows {
			elem := s.Elem(row)
			if !elem.IsNA() {
				sum += elem.Float()
			}
		}
		return sum
	}
}

// SumByGroup calculates sums for all group codes in one column scan.
func (s Series) SumByGroup(groupCodes []int, nGroups int) []float64 {
	out := make([]float64, nGroups)
	switch elems := s.elements.(type) {
	case floatElements:
		for row, groupID := range groupCodes {
			elem := elems[row]
			if !elem.IsNA() {
				out[groupID] += elem.e
			}
		}
	case intElements:
		for row, groupID := range groupCodes {
			elem := elems[row]
			if !elem.IsNA() {
				out[groupID] += float64(elem.e)
			}
		}
	default:
		for row, groupID := range groupCodes {
			elem := s.Elem(row)
			if !elem.IsNA() {
				out[groupID] += elem.Float()
			}
		}
	}
	return out
}

// MeanRows calculates the arithmetic mean over selected row positions.
// NaN values propagate, matching Mean() behavior through Float().
func (s Series) MeanRows(rows []int) float64 {
	if len(rows) == 0 {
		return 0
	}
	switch elems := s.elements.(type) {
	case floatElements:
		var sum float64
		for _, row := range rows {
			sum += elems[row].Float()
		}
		return sum / float64(len(rows))
	case intElements:
		var sum float64
		for _, row := range rows {
			sum += elems[row].Float()
		}
		return sum / float64(len(rows))
	default:
		var sum float64
		for _, row := range rows {
			sum += s.Elem(row).Float()
		}
		return sum / float64(len(rows))
	}
}

// MeanByGroup calculates means for all group codes in one column scan.
// NaN values propagate within a group, matching MeanRows/Mean behavior.
func (s Series) MeanByGroup(groupCodes []int, nGroups int) []float64 {
	out := make([]float64, nGroups)
	counts := make([]int, nGroups)
	switch elems := s.elements.(type) {
	case floatElements:
		for row, groupID := range groupCodes {
			out[groupID] += elems[row].Float()
			counts[groupID]++
		}
	case intElements:
		for row, groupID := range groupCodes {
			out[groupID] += elems[row].Float()
			counts[groupID]++
		}
	default:
		for row, groupID := range groupCodes {
			out[groupID] += s.Elem(row).Float()
			counts[groupID]++
		}
	}
	for groupID, count := range counts {
		if count > 0 {
			out[groupID] /= float64(count)
		}
	}
	return out
}

// MaxRows calculates the maximum over selected row positions.
// It preserves the existing Max/Aggregation behavior where a leading NaN keeps
// the result as NaN.
func (s Series) MaxRows(rows []int) float64 {
	if len(rows) == 0 {
		return 0
	}
	switch elems := s.elements.(type) {
	case floatElements:
		max := elems[rows[0]]
		if max.IsNA() {
			return max.Float()
		}
		for _, row := range rows[1:] {
			elem := elems[row]
			if !elem.IsNA() && elem.e > max.e {
				max = elem
			}
		}
		return max.Float()
	case intElements:
		max := elems[rows[0]]
		if max.IsNA() {
			return max.Float()
		}
		for _, row := range rows[1:] {
			elem := elems[row]
			if !elem.IsNA() && elem.e > max.e {
				max = elem
			}
		}
		return max.Float()
	default:
		max := s.Elem(rows[0])
		for _, row := range rows[1:] {
			elem := s.Elem(row)
			if elem.Greater(max) {
				max = elem
			}
		}
		return max.Float()
	}
}

// MaxByGroup calculates maximums for all group codes in one column scan.
// It returns false for unsupported types.
func (s Series) MaxByGroup(groupCodes []int, nGroups int) ([]float64, bool) {
	out := make([]float64, nGroups)
	seen := make([]bool, nGroups)
	lockedNaN := make([]bool, nGroups)
	switch elems := s.elements.(type) {
	case floatElements:
		for row, groupID := range groupCodes {
			if lockedNaN[groupID] {
				continue
			}
			elem := elems[row]
			if !seen[groupID] {
				seen[groupID] = true
				if elem.IsNA() {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = elem.e
				}
				continue
			}
			if !elem.IsNA() && elem.e > out[groupID] {
				out[groupID] = elem.e
			}
		}
	case intElements:
		for row, groupID := range groupCodes {
			if lockedNaN[groupID] {
				continue
			}
			elem := elems[row]
			if !seen[groupID] {
				seen[groupID] = true
				if elem.IsNA() {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = float64(elem.e)
				}
				continue
			}
			if !elem.IsNA() && float64(elem.e) > out[groupID] {
				out[groupID] = float64(elem.e)
			}
		}
	default:
		return nil, false
	}
	return out, true
}

// MinRows calculates the minimum over selected row positions.
// It preserves the existing Min/Aggregation behavior where a leading NaN keeps
// the result as NaN.
func (s Series) MinRows(rows []int) float64 {
	if len(rows) == 0 {
		return 0
	}
	switch elems := s.elements.(type) {
	case floatElements:
		min := elems[rows[0]]
		if min.IsNA() {
			return min.Float()
		}
		for _, row := range rows[1:] {
			elem := elems[row]
			if !elem.IsNA() && elem.e < min.e {
				min = elem
			}
		}
		return min.Float()
	case intElements:
		min := elems[rows[0]]
		if min.IsNA() {
			return min.Float()
		}
		for _, row := range rows[1:] {
			elem := elems[row]
			if !elem.IsNA() && elem.e < min.e {
				min = elem
			}
		}
		return min.Float()
	default:
		min := s.Elem(rows[0])
		for _, row := range rows[1:] {
			elem := s.Elem(row)
			if elem.Less(min) {
				min = elem
			}
		}
		return min.Float()
	}
}

// MinByGroup calculates minimums for all group codes in one column scan.
// It returns false for unsupported types.
func (s Series) MinByGroup(groupCodes []int, nGroups int) ([]float64, bool) {
	out := make([]float64, nGroups)
	seen := make([]bool, nGroups)
	lockedNaN := make([]bool, nGroups)
	switch elems := s.elements.(type) {
	case floatElements:
		for row, groupID := range groupCodes {
			if lockedNaN[groupID] {
				continue
			}
			elem := elems[row]
			if !seen[groupID] {
				seen[groupID] = true
				if elem.IsNA() {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = elem.e
				}
				continue
			}
			if !elem.IsNA() && elem.e < out[groupID] {
				out[groupID] = elem.e
			}
		}
	case intElements:
		for row, groupID := range groupCodes {
			if lockedNaN[groupID] {
				continue
			}
			elem := elems[row]
			if !seen[groupID] {
				seen[groupID] = true
				if elem.IsNA() {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = float64(elem.e)
				}
				continue
			}
			if !elem.IsNA() && float64(elem.e) < out[groupID] {
				out[groupID] = float64(elem.e)
			}
		}
	default:
		return nil, false
	}
	return out, true
}
