package series

import (
	"fmt"
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
	col := newColumn[int64](len(src))
	for i, v := range src {
		if parsed, ok := parseInt64Value(v); ok {
			col.data[i] = parsed
		} else {
			col.setNA(i)
		}
	}
	return Series{Name: name, elements: col, t: Int}
}

func batchConvertToFloat[T any](src []T, name string) Series {
	col := newColumn[float64](len(src))
	for i, v := range src {
		switch val := any(v).(type) {
		case float32:
			col.data[i] = float64(val)
		default:
			if parsed, ok := parseFloat64Value(v); ok {
				col.data[i] = parsed
			} else {
				col.setNA(i)
			}
		}
	}
	return Series{Name: name, elements: col, t: Float}
}

func batchConvertToString[T any](src []T, name string) Series {
	col := newColumn[string](len(src))
	for i, v := range src {
		switch val := any(v).(type) {
		case string:
			col.data[i] = val
		case fmt.Stringer:
			col.data[i] = val.String()
		default:
			col.data[i] = fmt.Sprintf("%v", val)
		}
	}
	return Series{Name: name, elements: col, t: String}
}

func batchConvertToBool[T any](src []T, name string) Series {
	col := newColumn[bool](len(src))
	for i, v := range src {
		if parsed, ok := parseBoolValue(v); ok {
			col.data[i] = parsed
		} else {
			col.setNA(i)
		}
	}
	return Series{Name: name, elements: col, t: Bool}
}

func batchConvertToTime[T any](src []T, name string) Series {
	col := newColumn[time.Time](len(src))
	for i, v := range src {
		switch val := any(v).(type) {
		case int64:
			col.data[i] = time.Unix(val, 0)
		default:
			if parsed, ok := parseTimeValue(v); ok {
				col.data[i] = parsed
			} else {
				col.setNA(i)
			}
		}
	}
	return Series{Name: name, elements: col, t: Time}
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
