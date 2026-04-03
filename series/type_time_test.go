package series

import (
	"reflect"
	"testing"
	"time"
)

// -----------------------------------------------------------------------
// Time Series — Copy / Append / Subset / Concat / FillNaN*
// -----------------------------------------------------------------------

var (
	t0 = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	t2 = time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
	t3 = time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)
)

func timeSeries(vals ...interface{}) Series {
	return New(vals, Time, "t")
}

func timeRecords(s Series) []string { return s.Records() }

func TestTimeSeries_Copy(t *testing.T) {
	s := timeSeries(t0, t1, t2)
	c := s.Copy()
	if c.Err != nil {
		t.Fatal(c.Err)
	}
	if !reflect.DeepEqual(s.Records(), c.Records()) {
		t.Errorf("Copy: got %v want %v", c.Records(), s.Records())
	}
	// Mutating copy must not affect original.
	c.Elem(0).Set(t3)
	if s.Elem(0).String() == c.Elem(0).String() {
		t.Error("Copy: modifying copy affected original")
	}
}

func TestTimeSeries_Append(t *testing.T) {
	s := timeSeries(t0, t1)
	s.Append(t2)
	if s.Len() != 3 {
		t.Fatalf("Append: len=%d want 3", s.Len())
	}
	if s.Elem(2).String() != t2.Format(time.RFC3339) {
		t.Errorf("Append: elem[2]=%s want %s", s.Elem(2).String(), t2.Format(time.RFC3339))
	}
}

func TestTimeSeries_Subset(t *testing.T) {
	s := timeSeries(t0, t1, t2, t3)
	sub := s.Subset([]int{1, 3})
	if sub.Err != nil {
		t.Fatal(sub.Err)
	}
	if sub.Len() != 2 {
		t.Fatalf("Subset: len=%d want 2", sub.Len())
	}
	if sub.Elem(0).String() != t1.Format(time.RFC3339) {
		t.Errorf("Subset[0]: got %s want %s", sub.Elem(0).String(), t1.Format(time.RFC3339))
	}
	if sub.Elem(1).String() != t3.Format(time.RFC3339) {
		t.Errorf("Subset[1]: got %s want %s", sub.Elem(1).String(), t3.Format(time.RFC3339))
	}
}

func TestTimeSeries_Concat(t *testing.T) {
	a := timeSeries(t0, t1)
	b := timeSeries(t2, t3)
	c := a.Concat(b)
	if c.Err != nil {
		t.Fatal(c.Err)
	}
	if c.Len() != 4 {
		t.Fatalf("Concat: len=%d want 4", c.Len())
	}
	want := []string{
		t0.Format(time.RFC3339),
		t1.Format(time.RFC3339),
		t2.Format(time.RFC3339),
		t3.Format(time.RFC3339),
	}
	if !reflect.DeepEqual(c.Records(), want) {
		t.Errorf("Concat: got %v want %v", c.Records(), want)
	}
}

func TestTimeSeries_FillNaN(t *testing.T) {
	s := New([]interface{}{t0, nil, t2}, Time, "t")
	filled := s.FillNaN(timeSeries(t1))
	if filled.Err != nil {
		t.Fatal(filled.Err)
	}
	if filled.Elem(1).IsNA() {
		t.Error("FillNaN: elem[1] still NaN after fill")
	}
	if filled.Elem(1).String() != t1.Format(time.RFC3339) {
		t.Errorf("FillNaN: elem[1]=%s want %s", filled.Elem(1).String(), t1.Format(time.RFC3339))
	}
}

func TestTimeSeries_FillNaNForward(t *testing.T) {
	s := New([]interface{}{t0, nil, nil, t3}, Time, "t")
	filled := s.FillNaNForward()
	if filled.Err != nil {
		t.Fatal(filled.Err)
	}
	// [t0, t0, t0, t3]
	for i, want := range []time.Time{t0, t0, t0, t3} {
		if filled.Elem(i).String() != want.Format(time.RFC3339) {
			t.Errorf("FillNaNForward[%d]: got %s want %s", i, filled.Elem(i).String(), want.Format(time.RFC3339))
		}
	}
}

func TestTimeSeries_FillNaNBackward(t *testing.T) {
	s := New([]interface{}{nil, nil, t2, t3}, Time, "t")
	filled := s.FillNaNBackward()
	if filled.Err != nil {
		t.Fatal(filled.Err)
	}
	// [t2, t2, t2, t3]
	for i, want := range []time.Time{t2, t2, t2, t3} {
		if filled.Elem(i).String() != want.Format(time.RFC3339) {
			t.Errorf("FillNaNBackward[%d]: got %s want %s", i, filled.Elem(i).String(), want.Format(time.RFC3339))
		}
	}
}

func TestTimeSeries_FillNaNForwardLimit(t *testing.T) {
	s := New([]interface{}{t0, nil, nil, nil, t3}, Time, "t")
	filled := s.FillNaNForwardLimit(2)
	// [t0, t0, t0, NaN, t3]
	if filled.Elem(1).IsNA() || filled.Elem(2).IsNA() {
		t.Error("FillNaNForwardLimit(2): elem[1] or [2] should be filled")
	}
	if !filled.Elem(3).IsNA() {
		t.Error("FillNaNForwardLimit(2): elem[3] should remain NaN")
	}
}

func TestTimeSeries_FillNaNBackwardLimit(t *testing.T) {
	s := New([]interface{}{nil, nil, nil, t3, t2}, Time, "t")
	filled := s.FillNaNBackwardLimit(1)
	// [NaN, NaN, t3, t3, t2]
	if !filled.Elem(0).IsNA() || !filled.Elem(1).IsNA() {
		t.Error("FillNaNBackwardLimit(1): elem[0] and [1] should remain NaN")
	}
	if filled.Elem(2).IsNA() {
		t.Error("FillNaNBackwardLimit(1): elem[2] should be filled")
	}
}

func TestTimeSeries_Order(t *testing.T) {
	s := timeSeries(t2, t0, t3, t1)
	order := s.Order(false)
	// ascending: t0(1), t1(3), t2(0), t3(2)
	want := []int{1, 3, 0, 2}
	if !reflect.DeepEqual(order, want) {
		t.Errorf("Order asc: got %v want %v", order, want)
	}
	orderDesc := s.Order(true)
	wantDesc := []int{2, 0, 3, 1}
	if !reflect.DeepEqual(orderDesc, wantDesc) {
		t.Errorf("Order desc: got %v want %v", orderDesc, wantDesc)
	}
}

func TestTimeSeries_NaN(t *testing.T) {
	s := New([]interface{}{t0, nil, t2}, Time, "t")
	if !s.HasNaN() {
		t.Error("HasNaN: expected true")
	}
	nans := s.IsNaN()
	want := []bool{false, true, false}
	if !reflect.DeepEqual(nans, want) {
		t.Errorf("IsNaN: got %v want %v", nans, want)
	}
}

func TestTimeSeries_Elem_Conversions(t *testing.T) {
	s := timeSeries(t0)
	e := s.Elem(0)

	// Int() → unix timestamp
	i, err := e.Int()
	if err != nil {
		t.Fatalf("Int(): %v", err)
	}
	if int64(i) != t0.Unix() {
		t.Errorf("Int(): got %d want %d", i, t0.Unix())
	}

	// Float() → unix timestamp as float
	f := e.Float()
	if f != float64(t0.Unix()) {
		t.Errorf("Float(): got %v want %v", f, float64(t0.Unix()))
	}

	// Time() → original value
	tv, err := e.Time()
	if err != nil {
		t.Fatalf("Time(): %v", err)
	}
	if !tv.Equal(t0) {
		t.Errorf("Time(): got %v want %v", tv, t0)
	}
}
