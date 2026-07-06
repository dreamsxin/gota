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
