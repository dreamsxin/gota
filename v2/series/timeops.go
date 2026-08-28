package series

import (
	"fmt"
	"time"
)

// Time-component accessors. They require a Time series and return an Int
// series; missing elements stay missing. Calling them on another type sets
// Err on the result. Weekday follows time.Weekday: Sunday = 0.

func (s Series) mapTime(op string, f func(t time.Time) int64) Series {
	if s.Err != nil {
		return s
	}
	if s.t != Time {
		return Series{Err: fmt.Errorf("%s: requires a Time series, got %s", op, s.t), Name: s.Name}
	}
	elems := s.elements.(timeElements)
	out := newColumn[int64](len(elems.data))
	for i, v := range elems.data {
		if !elems.isValid(i) {
			out.setNA(i)
			continue
		}
		out.data[i] = f(v)
	}
	return Series{Name: s.Name, t: Int, elements: out}
}

// Year returns an Int series with the year of each Time element.
func (s Series) Year() Series {
	return s.mapTime("Year", func(t time.Time) int64 { return int64(t.Year()) })
}

// Month returns an Int series with the month (1-12) of each Time element.
func (s Series) Month() Series {
	return s.mapTime("Month", func(t time.Time) int64 { return int64(t.Month()) })
}

// Day returns an Int series with the day of month (1-31) of each Time element.
func (s Series) Day() Series {
	return s.mapTime("Day", func(t time.Time) int64 { return int64(t.Day()) })
}

// Hour returns an Int series with the hour (0-23) of each Time element.
func (s Series) Hour() Series {
	return s.mapTime("Hour", func(t time.Time) int64 { return int64(t.Hour()) })
}

// Minute returns an Int series with the minute (0-59) of each Time element.
func (s Series) Minute() Series {
	return s.mapTime("Minute", func(t time.Time) int64 { return int64(t.Minute()) })
}

// Second returns an Int series with the second (0-59) of each Time element.
func (s Series) Second() Series {
	return s.mapTime("Second", func(t time.Time) int64 { return int64(t.Second()) })
}

// Weekday returns an Int series with the day of week of each Time element,
// following time.Weekday numbering: Sunday = 0, Saturday = 6.
func (s Series) Weekday() Series {
	return s.mapTime("Weekday", func(t time.Time) int64 { return int64(t.Weekday()) })
}
