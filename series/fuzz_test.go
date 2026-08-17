package series

import "testing"

// FuzzBatchConvertStrings converts fuzzed strings to every supported type.
// Conversion must never panic and must always preserve the input length.
func FuzzBatchConvertStrings(f *testing.F) {
	seeds := []string{
		"1", "-1", "0", "3.14", "-2.5e10", "NaN", "nan", "Inf", "-Inf",
		"true", "false", "t", "f", "1", "0", "TRUE", "False",
		"2024-01-02T03:04:05Z", "2024-01-02", "not a number",
		"", " ", "0x10", "1_000", "１２３", "\x00",
		"9223372036854775807", "9223372036854775808", // int64 overflow
	}
	for _, s := range seeds {
		f.Add(s)
	}

	types := []Type{Int, Float, Bool, Time, String}

	f.Fuzz(func(t *testing.T, s string) {
		for _, typ := range types {
			out := BatchConvertStrings([]string{s}, typ, "x")
			if out.Len() != 1 {
				t.Fatalf("BatchConvertStrings(%q, %v): len = %d, want 1", s, typ, out.Len())
			}
		}
	})
}
