package series

import "math"

// GroupedAggregation contains optional per-group aggregation results.
type GroupedAggregation struct {
	Sum   []float64
	Mean  []float64
	Max   []float64
	Min   []float64
	Count []float64
}

// AggregateByGroup calculates several common aggregations for all groups in one
// scan. It supports int and float Series and returns false for other types.
func (s Series) AggregateByGroup(groupCodes []int, nGroups int, needSum, needMean, needMax, needMin, needCount bool) (GroupedAggregation, bool) {
	out := GroupedAggregation{}
	if needSum || needMean {
		out.Sum = make([]float64, nGroups)
	}
	if needMean {
		out.Mean = make([]float64, nGroups)
	}
	if needMax {
		out.Max = make([]float64, nGroups)
	}
	if needMin {
		out.Min = make([]float64, nGroups)
	}
	if needCount || needMean {
		out.Count = make([]float64, nGroups)
	}
	var seenMax, lockedMaxNaN []bool
	if needMax {
		seenMax = make([]bool, nGroups)
		lockedMaxNaN = make([]bool, nGroups)
	}
	var seenMin, lockedMinNaN []bool
	if needMin {
		seenMin = make([]bool, nGroups)
		lockedMinNaN = make([]bool, nGroups)
	}

	switch elems := s.elements.(type) {
	case floatElements:
		for row, groupID := range groupCodes {
			isNA := !elems.isValid(row)
			value := elems.data[row]
			if needCount || needMean {
				out.Count[groupID]++
			}
			if needSum || needMean {
				if !isNA {
					out.Sum[groupID] += value
				} else if needMean {
					out.Sum[groupID] += math.NaN()
				}
			}
			if needMax && !lockedMaxNaN[groupID] {
				if !seenMax[groupID] {
					seenMax[groupID] = true
					if isNA {
						out.Max[groupID] = math.NaN()
						lockedMaxNaN[groupID] = true
					} else {
						out.Max[groupID] = value
					}
				} else if !isNA && value > out.Max[groupID] {
					out.Max[groupID] = value
				}
			}
			if needMin && !lockedMinNaN[groupID] {
				if !seenMin[groupID] {
					seenMin[groupID] = true
					if isNA {
						out.Min[groupID] = math.NaN()
						lockedMinNaN[groupID] = true
					} else {
						out.Min[groupID] = value
					}
				} else if !isNA && value < out.Min[groupID] {
					out.Min[groupID] = value
				}
			}
		}
	case intElements:
		for row, groupID := range groupCodes {
			isNA := !elems.isValid(row)
			value := float64(elems.data[row])
			if needCount || needMean {
				out.Count[groupID]++
			}
			if needSum || needMean {
				if !isNA {
					out.Sum[groupID] += value
				} else if needMean {
					out.Sum[groupID] += math.NaN()
				}
			}
			if needMax && !lockedMaxNaN[groupID] {
				if !seenMax[groupID] {
					seenMax[groupID] = true
					if isNA {
						out.Max[groupID] = math.NaN()
						lockedMaxNaN[groupID] = true
					} else {
						out.Max[groupID] = value
					}
				} else if !isNA && value > out.Max[groupID] {
					out.Max[groupID] = value
				}
			}
			if needMin && !lockedMinNaN[groupID] {
				if !seenMin[groupID] {
					seenMin[groupID] = true
					if isNA {
						out.Min[groupID] = math.NaN()
						lockedMinNaN[groupID] = true
					} else {
						out.Min[groupID] = value
					}
				} else if !isNA && value < out.Min[groupID] {
					out.Min[groupID] = value
				}
			}
		}
	default:
		return GroupedAggregation{}, false
	}
	if needMean {
		for groupID, count := range out.Count {
			if count > 0 {
				out.Mean[groupID] = out.Sum[groupID] / count
			}
		}
	}
	if !needSum {
		out.Sum = nil
	}
	if !needCount {
		out.Count = nil
	}
	return out, true
}

// SumRows calculates the sum over selected row positions, skipping NaN values.
func (s Series) SumRows(rows []int) float64 {
	switch elems := s.elements.(type) {
	case floatElements:
		var sum float64
		for _, row := range rows {
			if elems.isValid(row) {
				sum += elems.data[row]
			}
		}
		return sum
	case intElements:
		var sum float64
		for _, row := range rows {
			if elems.isValid(row) {
				sum += float64(elems.data[row])
			}
		}
		return sum
	default:
		var sum float64
		for _, row := range rows {
			if !s.IsNA(row) {
				sum += s.FloatAt(row)
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
			if elems.isValid(row) {
				out[groupID] += elems.data[row]
			}
		}
	case intElements:
		for row, groupID := range groupCodes {
			if elems.isValid(row) {
				out[groupID] += float64(elems.data[row])
			}
		}
	default:
		for row, groupID := range groupCodes {
			if !s.IsNA(row) {
				out[groupID] += s.FloatAt(row)
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
			if elems.isValid(row) {
				sum += elems.data[row]
			} else {
				sum += math.NaN()
			}
		}
		return sum / float64(len(rows))
	case intElements:
		var sum float64
		for _, row := range rows {
			if elems.isValid(row) {
				sum += float64(elems.data[row])
			} else {
				sum += math.NaN()
			}
		}
		return sum / float64(len(rows))
	default:
		var sum float64
		for _, row := range rows {
			sum += s.FloatAt(row)
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
			if elems.isValid(row) {
				out[groupID] += elems.data[row]
			} else {
				out[groupID] += math.NaN()
			}
			counts[groupID]++
		}
	case intElements:
		for row, groupID := range groupCodes {
			if elems.isValid(row) {
				out[groupID] += float64(elems.data[row])
			} else {
				out[groupID] += math.NaN()
			}
			counts[groupID]++
		}
	default:
		for row, groupID := range groupCodes {
			out[groupID] += s.FloatAt(row)
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
		if !elems.isValid(rows[0]) {
			return math.NaN()
		}
		max := elems.data[rows[0]]
		for _, row := range rows[1:] {
			if elems.isValid(row) && elems.data[row] > max {
				max = elems.data[row]
			}
		}
		return max
	case intElements:
		if !elems.isValid(rows[0]) {
			return math.NaN()
		}
		max := elems.data[rows[0]]
		for _, row := range rows[1:] {
			if elems.isValid(row) && elems.data[row] > max {
				max = elems.data[row]
			}
		}
		return float64(max)
	default:
		max := s.FloatAt(rows[0])
		for _, row := range rows[1:] {
			if v := s.FloatAt(row); v > max {
				max = v
			}
		}
		return max
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
			if !seen[groupID] {
				seen[groupID] = true
				if !elems.isValid(row) {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = elems.data[row]
				}
				continue
			}
			if elems.isValid(row) && elems.data[row] > out[groupID] {
				out[groupID] = elems.data[row]
			}
		}
	case intElements:
		for row, groupID := range groupCodes {
			if lockedNaN[groupID] {
				continue
			}
			if !seen[groupID] {
				seen[groupID] = true
				if !elems.isValid(row) {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = float64(elems.data[row])
				}
				continue
			}
			if elems.isValid(row) && float64(elems.data[row]) > out[groupID] {
				out[groupID] = float64(elems.data[row])
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
		if !elems.isValid(rows[0]) {
			return math.NaN()
		}
		min := elems.data[rows[0]]
		for _, row := range rows[1:] {
			if elems.isValid(row) && elems.data[row] < min {
				min = elems.data[row]
			}
		}
		return min
	case intElements:
		if !elems.isValid(rows[0]) {
			return math.NaN()
		}
		min := elems.data[rows[0]]
		for _, row := range rows[1:] {
			if elems.isValid(row) && elems.data[row] < min {
				min = elems.data[row]
			}
		}
		return float64(min)
	default:
		min := s.FloatAt(rows[0])
		for _, row := range rows[1:] {
			if v := s.FloatAt(row); v < min {
				min = v
			}
		}
		return min
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
			if !seen[groupID] {
				seen[groupID] = true
				if !elems.isValid(row) {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = elems.data[row]
				}
				continue
			}
			if elems.isValid(row) && elems.data[row] < out[groupID] {
				out[groupID] = elems.data[row]
			}
		}
	case intElements:
		for row, groupID := range groupCodes {
			if lockedNaN[groupID] {
				continue
			}
			if !seen[groupID] {
				seen[groupID] = true
				if !elems.isValid(row) {
					out[groupID] = math.NaN()
					lockedNaN[groupID] = true
				} else {
					out[groupID] = float64(elems.data[row])
				}
				continue
			}
			if elems.isValid(row) && float64(elems.data[row]) < out[groupID] {
				out[groupID] = float64(elems.data[row])
			}
		}
	default:
		return nil, false
	}
	return out, true
}
