package series

// Shift returns a Series with values shifted by periods positions. Positive
// periods shift values down and mark the leading positions as NA; negative
// periods shift values up and mark the trailing positions as NA.
func (s Series) Shift(periods int) Series {
	if err := s.Err; err != nil {
		return s
	}
	if periods == 0 {
		return s.Copy()
	}
	n := s.Len()
	if n == 0 {
		return s.Empty()
	}
	ret := Series{Name: s.Name, t: s.t}
	abs := boundedShiftAbs(periods, n)

	switch elems := s.elements.(type) {
	case stringElements:
		ret.elements = shiftColumn(elems, n, periods, abs)
	case intElements:
		ret.elements = shiftColumn(elems, n, periods, abs)
	case floatElements:
		ret.elements = shiftColumn(elems, n, periods, abs)
	case boolElements:
		ret.elements = shiftColumn(elems, n, periods, abs)
	case timeElements:
		ret.elements = shiftColumn(elems, n, periods, abs)
	default:
		return s.Copy()
	}
	return ret
}

// shiftColumn copies values by |periods| positions and marks the vacated
// edge as missing.
func shiftColumn[T any](src column[T], n, periods, abs int) column[T] {
	out := newColumn[T](n)
	if periods > 0 {
		copy(out.data[abs:], src.data[:n-abs])
	} else {
		copy(out.data, src.data[abs:])
	}
	// Carry source validity into shifted positions.
	if src.validity != nil {
		out.ensureValidity()
		if periods > 0 {
			for i := abs; i < n; i++ {
				if !src.isValid(i - abs) {
					out.validity.clear(i)
				}
			}
		} else {
			for i := 0; i < n-abs; i++ {
				if !src.isValid(i + abs) {
					out.validity.clear(i)
				}
			}
		}
	}
	// Mark the vacated edge as missing.
	out.ensureValidity()
	if periods > 0 {
		out.validity.clearRange(0, abs)
	} else {
		out.validity.clearRange(n-abs, n)
	}
	return out
}

func boundedShiftAbs(periods int, limit int) int {
	if periods >= 0 {
		if periods > limit {
			return limit
		}
		return periods
	}
	if periods <= -limit {
		return limit
	}
	return -periods
}
