package series

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Typed per-row accessors. They replace the v1 Element interface; every
// method dispatches on the Series type directly over the column buffers.
// Missing values follow the v1 element-method conventions: FloatAt returns
// NaN, Record returns "NaN", Val returns nil, and the strict converters
// return errors.

// IsNA reports whether the value at position i is missing.
func (s Series) IsNA(i int) bool {
	if s.elements == nil {
		return true
	}
	switch elems := s.elements.(type) {
	case intElements:
		return !elems.isValid(i)
	case floatElements:
		return !elems.isValid(i)
	case stringElements:
		return !elems.isValid(i)
	case boolElements:
		return !elems.isValid(i)
	case timeElements:
		return !elems.isValid(i)
	default:
		return true
	}
}

// Val returns the typed Go value at position i, or nil when missing.
func (s Series) Val(i int) interface{} {
	if s.elements == nil || s.IsNA(i) {
		return nil
	}
	switch elems := s.elements.(type) {
	case intElements:
		return int(elems.data[i])
	case floatElements:
		return elems.data[i]
	case stringElements:
		return elems.data[i]
	case boolElements:
		return elems.data[i]
	case timeElements:
		return elems.data[i]
	default:
		return nil
	}
}

// FloatAt returns the value at position i as float64, or NaN when missing
// or not convertible.
func (s Series) FloatAt(i int) float64 {
	if s.elements == nil || s.IsNA(i) {
		return math.NaN()
	}
	switch elems := s.elements.(type) {
	case intElements:
		return float64(elems.data[i])
	case floatElements:
		return elems.data[i]
	case stringElements:
		f, err := strconv.ParseFloat(elems.data[i], 64)
		if err != nil {
			return math.NaN()
		}
		return f
	case boolElements:
		if elems.data[i] {
			return 1
		}
		return 0
	case timeElements:
		return float64(elems.data[i].Unix())
	default:
		return math.NaN()
	}
}

// IntAt returns the value at position i as int, matching the v1 strict
// conversion rules.
func (s Series) IntAt(i int) (int, error) {
	v, err := s.Int64At(i)
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

// Int64At returns the value at position i as int64, matching the v1 strict
// conversion rules.
func (s Series) Int64At(i int) (int64, error) {
	if s.elements == nil || s.IsNA(i) {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	switch elems := s.elements.(type) {
	case intElements:
		return elems.data[i], nil
	case floatElements:
		f := elems.data[i]
		if math.IsInf(f, 1) || math.IsInf(f, -1) {
			return 0, fmt.Errorf("can't convert Inf to int")
		}
		if math.IsNaN(f) {
			return 0, fmt.Errorf("can't convert NaN to int")
		}
		return int64(f), nil
	case stringElements:
		return strconv.ParseInt(elems.data[i], 10, 64)
	case boolElements:
		if elems.data[i] {
			return 1, nil
		}
		return 0, nil
	case timeElements:
		return elems.data[i].Unix(), nil
	default:
		return 0, fmt.Errorf("can't convert NaN to int")
	}
}

// BoolAt returns the value at position i as bool, matching the v1 strict
// conversion rules.
func (s Series) BoolAt(i int) (bool, error) {
	if s.elements == nil || s.IsNA(i) {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch elems := s.elements.(type) {
	case boolElements:
		return elems.data[i], nil
	case intElements:
		switch elems.data[i] {
		case 1:
			return true, nil
		case 0:
			return false, nil
		}
		return false, fmt.Errorf("can't convert Int \"%v\" to bool", elems.data[i])
	case floatElements:
		switch elems.data[i] {
		case 1:
			return true, nil
		case 0:
			return false, nil
		}
		return false, fmt.Errorf("can't convert Float \"%v\" to bool", elems.data[i])
	case stringElements:
		switch strings.ToLower(elems.data[i]) {
		case "true", "t", "1":
			return true, nil
		case "false", "f", "0":
			return false, nil
		}
		return false, fmt.Errorf("can't convert String \"%v\" to bool", elems.data[i])
	case timeElements:
		return elems.data[i].IsZero(), nil
	default:
		return false, fmt.Errorf("can't convert NaN to bool")
	}
}

// TimeAt returns the value at position i as time.Time, matching the v1
// conversion rules.
func (s Series) TimeAt(i int) (time.Time, error) {
	if s.elements == nil || s.IsNA(i) {
		return time.Time{}, fmt.Errorf("can't convert NaN to time")
	}
	switch elems := s.elements.(type) {
	case timeElements:
		return elems.data[i], nil
	case intElements:
		return time.Unix(elems.data[i], 0), nil
	case floatElements:
		return time.Unix(int64(elems.data[i]), 0), nil
	case boolElements:
		return time.Time{}, nil
	case stringElements:
		t, err := time.ParseInLocation(time.RFC3339, elems.data[i], time.Local)
		if err != nil {
			return time.Time{}, err
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("can't convert NaN to time")
	}
}

// Record returns the string representation of the value at position i;
// missing values are rendered as "NaN", matching v1 Records output.
func (s Series) Record(i int) string {
	if s.elements == nil || s.IsNA(i) {
		return "NaN"
	}
	switch elems := s.elements.(type) {
	case intElements:
		return strconv.FormatInt(elems.data[i], 10)
	case floatElements:
		return fmt.Sprintf("%f", elems.data[i])
	case stringElements:
		return elems.data[i]
	case boolElements:
		if elems.data[i] {
			return "true"
		}
		return "false"
	case timeElements:
		return elems.data[i].Format(time.RFC3339)
	default:
		return "NaN"
	}
}

// appendAs appends the parsed form of value to the store, which must hold
// the buffer matching t. Unparseable values are appended as missing.
func appendAs(st *store, t Type, value interface{}) {
	switch t {
	case Int:
		col := (*st).(intElements)
		if v, ok := parseInt64Value(value); ok {
			col.append(v)
		} else {
			col.appendNA()
		}
		*st = col
	case Float:
		col := (*st).(floatElements)
		if v, ok := parseFloat64Value(value); ok {
			col.append(v)
		} else {
			col.appendNA()
		}
		*st = col
	case String:
		col := (*st).(stringElements)
		if v, ok := parseStringValue(value); ok {
			col.append(v)
		} else {
			col.appendNA()
		}
		*st = col
	case Bool:
		col := (*st).(boolElements)
		if v, ok := parseBoolValue(value); ok {
			col.append(v)
		} else {
			col.appendNA()
		}
		*st = col
	case Time:
		col := (*st).(timeElements)
		if v, ok := parseTimeValue(value); ok {
			col.append(v)
		} else {
			col.appendNA()
		}
		*st = col
	}
}

// setAt writes the parsed form of value at position i of the store, which
// must hold the buffer matching t. Unparseable values mark the slot missing.
func setAt(st *store, t Type, i int, value interface{}) {
	switch t {
	case Int:
		col := (*st).(intElements)
		if v, ok := parseInt64Value(value); ok {
			col.setValue(i, v)
		} else {
			col.setNA(i)
		}
		*st = col
	case Float:
		col := (*st).(floatElements)
		if v, ok := parseFloat64Value(value); ok {
			col.setValue(i, v)
		} else {
			col.setNA(i)
		}
		*st = col
	case String:
		col := (*st).(stringElements)
		if v, ok := parseStringValue(value); ok {
			col.setValue(i, v)
		} else {
			col.setNA(i)
		}
		*st = col
	case Bool:
		col := (*st).(boolElements)
		if v, ok := parseBoolValue(value); ok {
			col.setValue(i, v)
		} else {
			col.setNA(i)
		}
		*st = col
	case Time:
		col := (*st).(timeElements)
		if v, ok := parseTimeValue(value); ok {
			col.setValue(i, v)
		} else {
			col.setNA(i)
		}
		*st = col
	}
}

// AppendValueFrom appends the value at row i of src to s. It is the typed
// replacement for the v1 Append(src.Elem(i)) pattern used by joins.
func (s *Series) AppendValueFrom(src Series, i int) {
	if src.IsNA(i) {
		appendAs(&s.elements, s.t, "NaN")
		return
	}
	appendAs(&s.elements, s.t, src.Val(i))
}
