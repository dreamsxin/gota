package series

import (
	"strconv"
	"time"
)

// parseStringValue converts an arbitrary scalar to a String value, reporting
// false when the value must be stored as missing. The literal "NaN" is a
// missing sentinel for String columns, matching v1 stringElement.Set.
func parseStringValue(value interface{}) (string, bool) {
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			return "", false
		}
		return val, true
	case int:
		return strconv.Itoa(val), true
	case float64:
		return strconv.FormatFloat(val, 'f', 6, 64), true
	case bool:
		if val {
			return "true", true
		}
		return "false", true
	case time.Time:
		return val.Format(time.RFC3339), true
	default:
		return "", false
	}
}
