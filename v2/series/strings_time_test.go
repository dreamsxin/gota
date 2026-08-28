package series

import (
	"reflect"
	"testing"
	"time"
)

func TestStringOps(t *testing.T) {
	s := New([]interface{}{"Hello, Go", "  world  ", nil, ""}, String, "s")

	if got := s.Upper().Records(); !reflect.DeepEqual(got, []string{"HELLO, GO", "  WORLD  ", "NaN", ""}) {
		t.Fatalf("Upper = %v", got)
	}
	if got := s.Lower().Records(); !reflect.DeepEqual(got, []string{"hello, go", "  world  ", "NaN", ""}) {
		t.Fatalf("Lower = %v", got)
	}
	if got := s.TrimSpace().Records(); !reflect.DeepEqual(got, []string{"Hello, Go", "world", "NaN", ""}) {
		t.Fatalf("TrimSpace = %v", got)
	}
	if got := s.Trim("Ho").Records(); !reflect.DeepEqual(got, []string{"ello, G", "  world  ", "NaN", ""}) {
		t.Fatalf("Trim = %v", got)
	}
	if got := s.TrimPrefix("Hello").Records(); !reflect.DeepEqual(got, []string{", Go", "  world  ", "NaN", ""}) {
		t.Fatalf("TrimPrefix = %v", got)
	}
	if got := s.TrimSuffix("world  ").Records(); !reflect.DeepEqual(got, []string{"Hello, Go", "  ", "NaN", ""}) {
		t.Fatalf("TrimSuffix = %v", got)
	}
	if got := s.ReplaceAll("o", "0").Records(); !reflect.DeepEqual(got, []string{"Hell0, G0", "  w0rld  ", "NaN", ""}) {
		t.Fatalf("ReplaceAll = %v", got)
	}

	// Predicates return Bool series; NaN stays NaN.
	if got := s.Contains("Go").Records(); !reflect.DeepEqual(got, []string{"true", "false", "NaN", "false"}) {
		t.Fatalf("Contains = %v", got)
	}
	if got := s.StartsWith("Hello").Records(); !reflect.DeepEqual(got, []string{"true", "false", "NaN", "false"}) {
		t.Fatalf("StartsWith = %v", got)
	}
	if got := s.EndsWith(" ").Records(); !reflect.DeepEqual(got, []string{"false", "true", "NaN", "false"}) {
		t.Fatalf("EndsWith = %v", got)
	}

	if ty := s.Contains("x").Type(); ty != Bool {
		t.Fatalf("Contains type = %v, want Bool", ty)
	}
	if ty := s.Upper().Type(); ty != String {
		t.Fatalf("Upper type = %v, want String", ty)
	}
}

func TestStringOps_TypeMismatch(t *testing.T) {
	nums := New([]int{1, 2}, Int, "n")
	for name, fn := range map[string]func(Series) Series{
		"Upper":      Series.Upper,
		"Contains":   func(s Series) Series { return s.Contains("x") },
		"StartsWith": func(s Series) Series { return s.StartsWith("x") },
	} {
		if got := fn(nums); got.Err == nil {
			t.Fatalf("%s on Int series should set Err", name)
		}
	}

	// Errors propagate stickily through further string ops.
	bad := nums.Upper()
	if got := bad.Lower(); got.Err == nil {
		t.Fatal("error should propagate through Lower")
	}
}

func TestTimeAccessors(t *testing.T) {
	base := time.Date(2024, time.March, 5, 14, 30, 45, 0, time.UTC)

	// Build with an explicit NaN in the middle for NA propagation.
	s := New([]interface{}{base, nil, base.Add(26 * time.Hour)}, Time, "t")

	if got := s.Year().Records(); !reflect.DeepEqual(got, []string{"2024", "NaN", "2024"}) {
		t.Fatalf("Year = %v", got)
	}
	if got := s.Month().Records(); !reflect.DeepEqual(got, []string{"3", "NaN", "3"}) {
		t.Fatalf("Month = %v", got)
	}
	if got := s.Day().Records(); !reflect.DeepEqual(got, []string{"5", "NaN", "6"}) {
		t.Fatalf("Day = %v", got)
	}
	if got := s.Hour().Records(); !reflect.DeepEqual(got, []string{"14", "NaN", "16"}) {
		t.Fatalf("Hour = %v", got)
	}
	if got := s.Minute().Records(); !reflect.DeepEqual(got, []string{"30", "NaN", "30"}) {
		t.Fatalf("Minute = %v", got)
	}
	if got := s.Second().Records(); !reflect.DeepEqual(got, []string{"45", "NaN", "45"}) {
		t.Fatalf("Second = %v", got)
	}
	// 2024-03-05 is a Tuesday (2); 2024-03-06 is a Wednesday (3).
	if got := s.Weekday().Records(); !reflect.DeepEqual(got, []string{"2", "NaN", "3"}) {
		t.Fatalf("Weekday = %v", got)
	}

	if ty := s.Year().Type(); ty != Int {
		t.Fatalf("Year type = %v, want Int", ty)
	}
}

func TestTimeAccessors_TypeMismatch(t *testing.T) {
	strs := New([]string{"a"}, String, "s")
	if got := strs.Year(); got.Err == nil {
		t.Fatal("Year on String series should set Err")
	}
	if got := strs.Year().Month(); got.Err == nil {
		t.Fatal("error should propagate through Month")
	}
}
