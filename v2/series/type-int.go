package series

import (
	"math"
	"strconv"
	"time"
)

// parseInt64Value converts an arbitrary scalar to an Int value, reporting
// false when the value must be stored as missing. The rules carry over from
// the v1 intElement.Set semantics.
func parseInt64Value(value interface{}) (int64, bool) {
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			return 0, false
		}
		i, err := strconv.Atoi(val)
		if err != nil {
			return 0, false
		}
		return int64(i), true
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, false
		}
		return int64(val), true
	case bool:
		if val {
			return 1, true
		}
		return 0, true
	case time.Time:
		if val.IsZero() {
			return 0, true
		}
		return val.Unix(), true
	default:
		return 0, false
	}
}
