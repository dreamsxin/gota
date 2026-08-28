package series

import (
	"strings"
	"time"
)

// parseBoolValue converts an arbitrary scalar to a Bool value, reporting
// false when the value must be stored as missing. The rules carry over from
// the v1 boolElement.Set semantics.
func parseBoolValue(value interface{}) (bool, bool) {
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			return false, false
		}
		switch strings.ToLower(val) {
		case "true", "t", "1":
			return true, true
		case "false", "f", "0":
			return false, true
		default:
			return false, false
		}
	case int:
		switch val {
		case 1:
			return true, true
		case 0:
			return false, true
		default:
			return false, false
		}
	case float64:
		switch val {
		case 1:
			return true, true
		case 0:
			return false, true
		default:
			return false, false
		}
	case bool:
		return val, true
	case time.Time:
		return !val.IsZero(), true
	default:
		return false, false
	}
}
