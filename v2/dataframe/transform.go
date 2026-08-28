package dataframe

import (
	"fmt"

	"github.com/dreamsxin/gota/v2/series"
)

// BatchTransform is the v2 UDF contract (RFC §9.4): a function over column
// batches - all columns in, transformed columns out. Row-oriented apply was
// removed with the columnar kernel; scalar row logic belongs in the
// series.MapFloat64 / series.MapInt64 masked-loop kernels instead.
type BatchTransform func(cols []series.Series) ([]series.Series, error)

// ApplyBatch runs t over the frame's columns and returns the transformed
// frame. Output columns replace the inputs positionally; their lengths must
// stay consistent so the result is a valid frame. Sticky errors propagate:
// an input frame carrying Err is returned unchanged, and an error from t
// becomes the result's Err.
func (df DataFrame) ApplyBatch(t BatchTransform) DataFrame {
	if df.Err != nil {
		return df
	}
	out, err := t(df.columns)
	if err != nil {
		return DataFrame{Err: fmt.Errorf("ApplyBatch: %w", err)}
	}
	if len(out) == 0 {
		return DataFrame{Err: fmt.Errorf("ApplyBatch: transform returned no columns")}
	}
	// Preserve input names unless the transform named its outputs.
	for i := range out {
		if out[i].Name == "" && i < len(df.columns) {
			out[i].Name = df.columns[i].Name
		}
	}
	return New(out...)
}
