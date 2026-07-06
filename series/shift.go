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
	ret := Series{Name: s.Name, t: s.t}
	if n == 0 {
		return s.Empty()
	}
	abs := boundedShiftAbs(periods, n)

	switch elems := s.elements.(type) {
	case stringElements:
		out := make(stringElements, n)
		if periods > 0 {
			markStringNA(out[:abs])
			copy(out[abs:], elems[:n-abs])
		} else {
			copy(out, elems[abs:])
			markStringNA(out[n-abs:])
		}
		ret.elements = out
	case intElements:
		out := make(intElements, n)
		if periods > 0 {
			markIntNA(out[:abs])
			copy(out[abs:], elems[:n-abs])
		} else {
			copy(out, elems[abs:])
			markIntNA(out[n-abs:])
		}
		ret.elements = out
	case floatElements:
		out := make(floatElements, n)
		if periods > 0 {
			markFloatNA(out[:abs])
			copy(out[abs:], elems[:n-abs])
		} else {
			copy(out, elems[abs:])
			markFloatNA(out[n-abs:])
		}
		ret.elements = out
	case boolElements:
		out := make(boolElements, n)
		if periods > 0 {
			markBoolNA(out[:abs])
			copy(out[abs:], elems[:n-abs])
		} else {
			copy(out, elems[abs:])
			markBoolNA(out[n-abs:])
		}
		ret.elements = out
	case timeElements:
		out := make(timeElements, n)
		if periods > 0 {
			markTimeNA(out[:abs])
			copy(out[abs:], elems[:n-abs])
		} else {
			copy(out, elems[abs:])
			markTimeNA(out[n-abs:])
		}
		ret.elements = out
	default:
		return s.Copy()
	}
	return ret
}

func markStringNA(elems stringElements) {
	for i := range elems {
		elems[i].nan = true
	}
}

func markIntNA(elems intElements) {
	for i := range elems {
		elems[i].nan = true
	}
}

func markFloatNA(elems floatElements) {
	for i := range elems {
		elems[i].nan = true
	}
}

func markBoolNA(elems boolElements) {
	for i := range elems {
		elems[i].nan = true
	}
}

func markTimeNA(elems timeElements) {
	for i := range elems {
		elems[i].nan = true
	}
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
