package series

import "strconv"

// Factorize encodes a Series into dense integer group codes in first-seen order.
// It returns false for types that do not have a typed fast path.
func (s Series) Factorize() (labels []string, codes []int, counts []int, ok bool) {
	switch elems := s.elements.(type) {
	case stringElements:
		return factorizeStringElements(elems)
	case intElements:
		return factorizeIntElements(elems)
	case boolElements:
		return factorizeBoolElements(elems)
	case floatElements:
		return factorizeFloatElements(elems)
	default:
		return nil, nil, nil, false
	}
}

// FactorizePair encodes two Series into dense tuple group codes in first-seen
// order. It covers common typed key pairs without per-row Element dispatch.
func FactorizePair(left, right Series) (labels []string, codes []int, counts []int, ok bool) {
	if left.Len() != right.Len() {
		return nil, nil, nil, false
	}
	switch l := left.elements.(type) {
	case stringElements:
		switch r := right.elements.(type) {
		case stringElements:
			return factorizeStringStringPair(l, r)
		case intElements:
			return factorizeStringIntPair(l, r)
		}
	case intElements:
		switch r := right.elements.(type) {
		case stringElements:
			return factorizeIntStringPair(l, r)
		case intElements:
			return factorizeIntIntPair(l, r)
		}
	}
	return nil, nil, nil, false
}

type stringStringKey struct {
	left, right     string
	leftNA, rightNA bool
}

type stringIntKey struct {
	left            string
	right           int64
	leftNA, rightNA bool
}

type intStringKey struct {
	left            int64
	right           string
	leftNA, rightNA bool
}

type intIntKey struct {
	left, right     int64
	leftNA, rightNA bool
}

func factorizeStringStringPair(left, right stringElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[stringStringKey]int)
	labels := make([]string, 0)
	codes := make([]int, len(left))
	counts := make([]int, 0)
	for row := range left {
		key := stringStringKey{left: left[row].e, right: right[row].e, leftNA: left[row].IsNA(), rightNA: right[row].IsNA()}
		groupID, ok := groupIDs[key]
		if !ok {
			groupID = len(labels)
			groupIDs[key] = groupID
			label := pairLabel(stringLabel(key.left, key.leftNA), stringLabel(key.right, key.rightNA))
			labels = append(labels, label)
			counts = append(counts, 0)
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}

func factorizeStringIntPair(left stringElements, right intElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[stringIntKey]int)
	labels := make([]string, 0)
	codes := make([]int, len(left))
	counts := make([]int, 0)
	for row := range left {
		key := stringIntKey{left: left[row].e, right: right[row].e, leftNA: left[row].IsNA(), rightNA: right[row].IsNA()}
		groupID, ok := groupIDs[key]
		if !ok {
			groupID = len(labels)
			groupIDs[key] = groupID
			label := pairLabel(stringLabel(key.left, key.leftNA), intLabel(key.right, key.rightNA))
			labels = append(labels, label)
			counts = append(counts, 0)
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}

func factorizeIntStringPair(left intElements, right stringElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[intStringKey]int)
	labels := make([]string, 0)
	codes := make([]int, len(left))
	counts := make([]int, 0)
	for row := range left {
		key := intStringKey{left: left[row].e, right: right[row].e, leftNA: left[row].IsNA(), rightNA: right[row].IsNA()}
		groupID, ok := groupIDs[key]
		if !ok {
			groupID = len(labels)
			groupIDs[key] = groupID
			label := pairLabel(intLabel(key.left, key.leftNA), stringLabel(key.right, key.rightNA))
			labels = append(labels, label)
			counts = append(counts, 0)
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}

func factorizeIntIntPair(left, right intElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[intIntKey]int)
	labels := make([]string, 0)
	codes := make([]int, len(left))
	counts := make([]int, 0)
	for row := range left {
		key := intIntKey{left: left[row].e, right: right[row].e, leftNA: left[row].IsNA(), rightNA: right[row].IsNA()}
		groupID, ok := groupIDs[key]
		if !ok {
			groupID = len(labels)
			groupIDs[key] = groupID
			label := pairLabel(intLabel(key.left, key.leftNA), intLabel(key.right, key.rightNA))
			labels = append(labels, label)
			counts = append(counts, 0)
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}

func pairLabel(left, right string) string {
	return left + "_" + right
}

func stringLabel(value string, isNA bool) string {
	if isNA {
		return "<nil>"
	}
	return value
}

func intLabel(value int64, isNA bool) string {
	if isNA {
		return "<nil>"
	}
	return strconv.FormatInt(value, 10)
}

func factorizeStringElements(elems stringElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[string]int)
	labels := make([]string, 0)
	codes := make([]int, len(elems))
	counts := make([]int, 0)
	for row, elem := range elems {
		key := "<nil>"
		if !elem.IsNA() {
			key = elem.e
		}
		groupID, ok := groupIDs[key]
		if !ok {
			groupID = len(labels)
			groupIDs[key] = groupID
			labels = append(labels, key)
			counts = append(counts, 0)
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}

func factorizeIntElements(elems intElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[int64]int)
	labels := make([]string, 0)
	codes := make([]int, len(elems))
	counts := make([]int, 0)
	naGroup := -1
	for row, elem := range elems {
		var groupID int
		if elem.IsNA() {
			if naGroup == -1 {
				naGroup = len(labels)
				labels = append(labels, "<nil>")
				counts = append(counts, 0)
			}
			groupID = naGroup
		} else {
			key := elem.e
			var ok bool
			groupID, ok = groupIDs[key]
			if !ok {
				groupID = len(labels)
				groupIDs[key] = groupID
				labels = append(labels, strconv.FormatInt(key, 10))
				counts = append(counts, 0)
			}
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}

func factorizeBoolElements(elems boolElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[bool]int, 2)
	labels := make([]string, 0, 3)
	codes := make([]int, len(elems))
	counts := make([]int, 0, 3)
	naGroup := -1
	for row, elem := range elems {
		var groupID int
		if elem.IsNA() {
			if naGroup == -1 {
				naGroup = len(labels)
				labels = append(labels, "<nil>")
				counts = append(counts, 0)
			}
			groupID = naGroup
		} else {
			key := elem.e
			var ok bool
			groupID, ok = groupIDs[key]
			if !ok {
				groupID = len(labels)
				groupIDs[key] = groupID
				labels = append(labels, strconv.FormatBool(key))
				counts = append(counts, 0)
			}
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}

func factorizeFloatElements(elems floatElements) ([]string, []int, []int, bool) {
	groupIDs := make(map[float64]int)
	labels := make([]string, 0)
	codes := make([]int, len(elems))
	counts := make([]int, 0)
	naGroup := -1
	for row, elem := range elems {
		var groupID int
		if elem.IsNA() {
			if naGroup == -1 {
				naGroup = len(labels)
				labels = append(labels, "<nil>")
				counts = append(counts, 0)
			}
			groupID = naGroup
		} else {
			key := elem.e
			var ok bool
			groupID, ok = groupIDs[key]
			if !ok {
				groupID = len(labels)
				groupIDs[key] = groupID
				labels = append(labels, strconv.FormatFloat(key, 'f', -1, 64))
				counts = append(counts, 0)
			}
		}
		codes[row] = groupID
		counts[groupID]++
	}
	return labels, codes, counts, true
}
