package series

import (
	"strconv"
	"strings"
	"time"
)

func stringsToSeriesDirect(values []string, t Type, name string) (Series, bool) {
	switch t {
	case String:
		s := StringsDirect(values)
		s.Name = name
		return s, true
	case Int:
		elems := make(intElements, len(values))
		for i, v := range values {
			if v == "NaN" {
				elems[i] = intElement{nan: true}
				continue
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				elems[i] = intElement{nan: true}
				continue
			}
			elems[i] = intElement{e: int64(n)}
		}
		return Series{Name: name, t: Int, elements: elems}, true
	case Float:
		elems := make(floatElements, len(values))
		for i, v := range values {
			if v == "NaN" {
				elems[i] = floatElement{nan: true}
				continue
			}
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				elems[i] = floatElement{nan: true}
				continue
			}
			elems[i] = floatElement{e: n}
		}
		return Series{Name: name, t: Float, elements: elems}, true
	case Bool:
		elems := make(boolElements, len(values))
		for i, v := range values {
			if v == "NaN" {
				elems[i] = boolElement{nan: true}
				continue
			}
			switch strings.ToLower(v) {
			case "true", "t", "1":
				elems[i] = boolElement{e: true}
			case "false", "f", "0":
				elems[i] = boolElement{e: false}
			default:
				elems[i] = boolElement{nan: true}
			}
		}
		return Series{Name: name, t: Bool, elements: elems}, true
	case Time:
		elems := make(timeElements, len(values))
		for i, v := range values {
			if v == "NaN" {
				elems[i] = timeElement{nan: true}
				continue
			}
			ts, err := time.ParseInLocation(time.RFC3339, v, time.Local)
			if err != nil {
				elems[i] = timeElement{nan: true}
				continue
			}
			elems[i] = timeElement{e: ts}
		}
		return Series{Name: name, t: Time, elements: elems}, true
	default:
		return Series{}, false
	}
}
