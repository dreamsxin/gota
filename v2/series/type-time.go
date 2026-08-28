package series

import (
	"time"
)

// parseTimeValue converts an arbitrary scalar to a Time value, reporting
// false when the value must be stored as missing. The rules carry over from
// the v1 timeElement.Set semantics.
func parseTimeValue(value interface{}) (time.Time, bool) {
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			return time.Time{}, false
		}
		t, err := time.ParseInLocation(time.RFC3339, val, time.Local)
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	case int:
		return time.Unix(int64(val), 0), true
	case float64:
		return time.Unix(int64(val), 0), true
	case time.Time:
		return val, true
	default:
		return time.Time{}, false
	}
}
