package dataframe

import (
	"fmt"

	"github.com/dreamsxin/gota/series"
)

// DataFrameRolling applies rolling-window statistics to the numeric columns
// of a DataFrame. Non-numeric columns are carried through unchanged.
//
//	df.Rolling(3).Mean()             // all numeric columns
//	df.Rolling(3).Mean("close")      // selected columns only
type DataFrameRolling struct {
	df     DataFrame
	window int
}

// Rolling returns a DataFrameRolling builder for the given window size.
func (df DataFrame) Rolling(window int) DataFrameRolling {
	return DataFrameRolling{df: df, window: window}
}

func (r DataFrameRolling) apply(op string, f func(s series.Series) series.Series, cols ...string) DataFrame {
	if r.df.Err != nil {
		return r.df
	}
	if r.window < 1 {
		return DataFrame{Err: fmt.Errorf("Rolling(%d): window must be >= 1", r.window)}
	}

	names := r.df.Names()
	target := make([]bool, r.df.ncols)
	if len(cols) > 0 {
		for _, c := range cols {
			i := findInStringSlice(c, names)
			if i < 0 {
				return DataFrame{Err: withSentinel(fmt.Sprintf("Rolling: column %q not found", c), ErrColumnNotFound)}
			}
			target[i] = true
		}
	} else {
		// No explicit selection: every numeric column is transformed and
		// the rest pass through unchanged.
		for i, col := range r.df.columns {
			target[i] = col.Type() == series.Int || col.Type() == series.Float
		}
	}

	out := make([]series.Series, r.df.ncols)
	for i, col := range r.df.columns {
		numeric := col.Type() == series.Int || col.Type() == series.Float
		if target[i] && numeric {
			transformed := f(col)
			if transformed.Err != nil {
				return DataFrame{Err: fmt.Errorf("%s: %w", op, transformed.Err)}
			}
			transformed.Name = col.Name
			out[i] = transformed
			continue
		}
		if target[i] && !numeric {
			return DataFrame{Err: withSentinel(fmt.Sprintf("%s: column %q is not numeric", op, col.Name), ErrTypeMismatch)}
		}
		out[i] = col.Copy()
	}
	return New(out...)
}

// Mean returns a DataFrame with the rolling mean of the target columns.
func (r DataFrameRolling) Mean(cols ...string) DataFrame {
	return r.apply("RollingMean", func(s series.Series) series.Series { return s.Rolling(r.window).Mean() }, cols...)
}

// Sum returns a DataFrame with the rolling sum of the target columns.
func (r DataFrameRolling) Sum(cols ...string) DataFrame {
	return r.apply("RollingSum", func(s series.Series) series.Series { return s.Rolling(r.window).Sum() }, cols...)
}

// Min returns a DataFrame with the rolling minimum of the target columns.
func (r DataFrameRolling) Min(cols ...string) DataFrame {
	return r.apply("RollingMin", func(s series.Series) series.Series { return s.Rolling(r.window).Min() }, cols...)
}

// Max returns a DataFrame with the rolling maximum of the target columns.
func (r DataFrameRolling) Max(cols ...string) DataFrame {
	return r.apply("RollingMax", func(s series.Series) series.Series { return s.Rolling(r.window).Max() }, cols...)
}

// StdDev returns a DataFrame with the rolling standard deviation of the
// target columns.
func (r DataFrameRolling) StdDev(cols ...string) DataFrame {
	return r.apply("RollingStdDev", func(s series.Series) series.Series { return s.Rolling(r.window).StdDev() }, cols...)
}

// DataFrameEWM applies exponentially weighted statistics to the numeric
// columns of a DataFrame. Non-numeric columns are carried through unchanged.
type DataFrameEWM struct {
	df    DataFrame
	ewmOf func(s series.Series) series.EWM
}

// EWM returns a DataFrameEWM builder with alpha = 2/(span+1), mirroring
// pandas' span convention.
func (df DataFrame) EWM(span float64) DataFrameEWM {
	return DataFrameEWM{df: df, ewmOf: func(s series.Series) series.EWM { return s.EWM(span) }}
}

// EWMAlpha returns a DataFrameEWM builder with an explicit smoothing factor.
func (df DataFrame) EWMAlpha(alpha float64) DataFrameEWM {
	return DataFrameEWM{df: df, ewmOf: func(s series.Series) series.EWM { return s.EWMAlpha(alpha) }}
}

func (e DataFrameEWM) apply(op string, f func(w series.EWM) series.Series, cols ...string) DataFrame {
	if e.df.Err != nil {
		return e.df
	}

	names := e.df.Names()
	target := make([]bool, e.df.ncols)
	if len(cols) > 0 {
		for _, c := range cols {
			i := findInStringSlice(c, names)
			if i < 0 {
				return DataFrame{Err: withSentinel(fmt.Sprintf("EWM: column %q not found", c), ErrColumnNotFound)}
			}
			target[i] = true
		}
	} else {
		// No explicit selection: every numeric column is transformed and
		// the rest pass through unchanged.
		for i, col := range e.df.columns {
			target[i] = col.Type() == series.Int || col.Type() == series.Float
		}
	}

	out := make([]series.Series, e.df.ncols)
	for i, col := range e.df.columns {
		numeric := col.Type() == series.Int || col.Type() == series.Float
		if target[i] && numeric {
			transformed := f(e.ewmOf(col))
			if transformed.Err != nil {
				return DataFrame{Err: fmt.Errorf("%s: %w", op, transformed.Err)}
			}
			transformed.Name = col.Name
			out[i] = transformed
			continue
		}
		if target[i] && !numeric {
			return DataFrame{Err: withSentinel(fmt.Sprintf("%s: column %q is not numeric", op, col.Name), ErrTypeMismatch)}
		}
		out[i] = col.Copy()
	}
	return New(out...)
}

// Mean returns a DataFrame with the exponentially weighted mean of the
// target columns.
func (e DataFrameEWM) Mean(cols ...string) DataFrame {
	return e.apply("EWMMean", func(w series.EWM) series.Series { return w.Mean() }, cols...)
}

// Var returns a DataFrame with the exponentially weighted variance of the
// target columns.
func (e DataFrameEWM) Var(cols ...string) DataFrame {
	return e.apply("EWMVar", func(w series.EWM) series.Series { return w.Var() }, cols...)
}

// Std returns a DataFrame with the exponentially weighted standard deviation
// of the target columns.
func (e DataFrameEWM) Std(cols ...string) DataFrame {
	return e.apply("EWMStd", func(w series.EWM) series.Series { return w.Std() }, cols...)
}
