package series

import (
	"fmt"
	"math"
	"time"
)

type timeElement struct {
	e   time.Time
	nan bool
}

// force timeElement struct to implement Element interface
var _ Element = (*timeElement)(nil)

func (e *timeElement) Set(value interface{}) {
	e.nan = false
	switch val := value.(type) {
	case string:
		s := string(val)
		if s == "NaN" {
			e.nan = true
			return
		}
		t, err := time.ParseInLocation(time.RFC3339, s, time.Local)
		if err != nil {
			fmt.Println("error parsing time:", err)
			e.nan = true
			return
		}
		e.e = t
	case int:
		e.e = time.Unix(int64(val), 0)
	case float64:
		e.e = time.Unix(int64(val), 0)
	case time.Time:
		e.e = val
	case Element:
		e.e, _ = val.Time()
	default:
		e.nan = true
		return
	}
}

func (e timeElement) Copy() Element {
	if e.IsNA() {
		return &timeElement{time.Time{}, true}
	}
	return &timeElement{e.e, false}
}

func (e timeElement) IsNA() bool {
	return e.nan
}

func (e timeElement) Type() Type {
	return Time
}

func (e timeElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return time.Time(e.e)
}

func (e timeElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return e.e.Format(time.RFC3339)
}

func (e timeElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return int(e.e.Unix()), nil
}

func (e timeElement) Int64() (int64, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return e.e.Unix(), nil
}

func (e timeElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return float64(e.e.Unix())
}

func (e timeElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}

	return e.e.IsZero(), nil
}

func (e timeElement) Time() (time.Time, error) {
	return e.e, nil
}

func (e timeElement) Eq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.Equal(t)
}

func (e timeElement) Neq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return !(e.e.Equal(t))
}

func (e timeElement) Less(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.Before(t)
}

func (e timeElement) LessEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.Before(t) && e.e.Equal(t)
}

func (e timeElement) Greater(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.After(t)
}

func (e timeElement) GreaterEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	t, _ := elem.Time()
	return e.e.After(t) && e.e.Equal(t)
}
