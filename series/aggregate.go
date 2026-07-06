package series

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
