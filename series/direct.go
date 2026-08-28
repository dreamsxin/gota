package series

import (
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
		col := newColumn[int64](len(values))
		for i, v := range values {
			if n, ok := parseInt64Value(v); ok {
				col.data[i] = n
			} else {
				col.setNA(i)
			}
		}
		return Series{Name: name, t: Int, elements: col}, true
	case Float:
		col := newColumn[float64](len(values))
		for i, v := range values {
			if f, ok := parseFloat64Value(v); ok {
				col.data[i] = f
			} else {
				col.setNA(i)
			}
		}
		return Series{Name: name, t: Float, elements: col}, true
	case Bool:
		col := newColumn[bool](len(values))
		for i, v := range values {
			if v == "NaN" {
				col.setNA(i)
				continue
			}
			switch strings.ToLower(v) {
			case "true", "t", "1":
				col.data[i] = true
			case "false", "f", "0":
				col.data[i] = false
			default:
				col.setNA(i)
			}
		}
		return Series{Name: name, t: Bool, elements: col}, true
	case Time:
		col := newColumn[time.Time](len(values))
		for i, v := range values {
			if ts, ok := parseTimeValue(v); ok {
				col.data[i] = ts
			} else {
				col.setNA(i)
			}
		}
		return Series{Name: name, t: Time, elements: col}, true
	default:
		return Series{}, false
	}
}
