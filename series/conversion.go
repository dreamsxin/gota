package series

import (
	"fmt"
	"strconv"
	"time"
)

// BatchConvert converts a slice of values to the target type in bulk.
// This is more efficient than creating Series with type conversion
// as it minimizes allocations and uses pre-allocated buffers.
//
// Example:
//
//	ints := []int{1, 2, 3, 4, 5}
//	strings := BatchConvert[int](ints, String, "col")
func BatchConvert[T any](src []T, dst Type, name string) Series {
	switch dst {
	case Int:
		return batchConvertToInt(src, name)
	case Float:
		return batchConvertToFloat(src, name)
	case String:
		return batchConvertToString(src, name)
	case Bool:
		return batchConvertToBool(src, name)
	case Time:
		return batchConvertToTime(src, name)
	default:
		return Series{Err: fmt.Errorf("unsupported target type: %v", dst)}
	}
}

func batchConvertToInt[T any](src []T, name string) Series {
	n := len(src)
	elems := make(intElements, n)

	for i, v := range src {
		switch val := any(v).(type) {
		case int:
			elems[i] = intElement{e: int64(val), nan: false}
		case int64:
			elems[i] = intElement{e: val, nan: false}
		case float64:
			elems[i] = intElement{e: int64(val), nan: false}
		case string:
			if parsed, err := strconv.Atoi(val); err == nil {
				elems[i] = intElement{e: int64(parsed), nan: false}
			} else {
				elems[i] = intElement{e: 0, nan: true}
			}
		default:
			elems[i] = intElement{e: 0, nan: true}
		}
	}

	return Series{Name: name, elements: elems, t: Int}
}

func batchConvertToFloat[T any](src []T, name string) Series {
	n := len(src)
	elems := make(floatElements, n)

	for i, v := range src {
		switch val := any(v).(type) {
		case float64:
			elems[i] = floatElement{e: val, nan: false}
		case float32:
			elems[i] = floatElement{e: float64(val), nan: false}
		case int:
			elems[i] = floatElement{e: float64(val), nan: false}
		case int64:
			elems[i] = floatElement{e: float64(val), nan: false}
		case string:
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				elems[i] = floatElement{e: f, nan: false}
			} else {
				elems[i] = floatElement{e: 0, nan: true}
			}
		default:
			elems[i] = floatElement{e: 0, nan: true}
		}
	}

	return Series{Name: name, elements: elems, t: Float}
}

func batchConvertToString[T any](src []T, name string) Series {
	n := len(src)
	elems := make(stringElements, n)

	for i, v := range src {
		switch val := any(v).(type) {
		case string:
			elems[i] = stringElement{e: val, nan: false}
		case fmt.Stringer:
			elems[i] = stringElement{e: val.String(), nan: false}
		default:
			elems[i] = stringElement{e: fmt.Sprintf("%v", val), nan: false}
		}
	}

	return Series{Name: name, elements: elems, t: String}
}

func batchConvertToBool[T any](src []T, name string) Series {
	n := len(src)
	elems := make(boolElements, n)

	for i, v := range src {
		switch val := any(v).(type) {
		case bool:
			elems[i] = boolElement{e: val, nan: false}
		case int:
			elems[i] = boolElement{e: val != 0, nan: false}
		case int64:
			elems[i] = boolElement{e: val != 0, nan: false}
		case string:
			elems[i] = boolElement{e: val == "true" || val == "1", nan: false}
		default:
			elems[i] = boolElement{e: false, nan: true}
		}
	}

	return Series{Name: name, elements: elems, t: Bool}
}

func batchConvertToTime[T any](src []T, name string) Series {
	n := len(src)
	elems := make(timeElements, n)

	for i, v := range src {
		switch val := any(v).(type) {
		case time.Time:
			elems[i] = timeElement{e: val, nan: false}
		case int64:
			elems[i] = timeElement{e: time.Unix(val, 0), nan: false}
		case int:
			elems[i] = timeElement{e: time.Unix(int64(val), 0), nan: false}
		case string:
			if t, err := time.ParseInLocation(time.RFC3339, val, time.Local); err == nil {
				elems[i] = timeElement{e: t, nan: false}
			} else {
				elems[i] = timeElement{e: time.Time{}, nan: true}
			}
		default:
			elems[i] = timeElement{e: time.Time{}, nan: true}
		}
	}

	return Series{Name: name, elements: elems, t: Time}
}

// BatchConvertInts converts []int to Series with specified type
func BatchConvertInts(src []int, dst Type, name string) Series {
	return BatchConvert(src, dst, name)
}

// BatchConvertFloats converts []float64 to Series with specified type
func BatchConvertFloats(src []float64, dst Type, name string) Series {
	return BatchConvert(src, dst, name)
}

// BatchConvertStrings converts []string to Series with specified type
func BatchConvertStrings(src []string, dst Type, name string) Series {
	return BatchConvert(src, dst, name)
}

// BatchConvertBools converts []bool to Series with specified type
func BatchConvertBools(src []bool, dst Type, name string) Series {
	return BatchConvert(src, dst, name)
}
