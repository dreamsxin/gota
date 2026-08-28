package series

import (
	"math"
	"strconv"
	"time"
)

// parseFloat64Value converts an arbitrary scalar to a Float value, reporting
// false when the value must be stored as missing. The rules carry over from
// the v1 floatElement semantics, where a NaN payload is always treated as
// missing.
func parseFloat64Value(value interface{}) (float64, bool) {
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			return 0, false
		}
		f, err := strconv.ParseFloat(val, 64)
		if err != nil || math.IsNaN(f) {
			return 0, false
		}
		return f, true
	case int:
		return float64(val), true
	case float64:
		if math.IsNaN(val) {
			return 0, false
		}
		return val, true
	case bool:
		if val {
			return 1, true
		}
		return 0, true
	case time.Time:
		if val.IsZero() {
			return 0, true
		}
		return float64(val.Unix()), true
	default:
		return 0, false
	}
}
