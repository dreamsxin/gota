package dataframe

import "fmt"

// Concat stacks any number of DataFrames vertically. Column order follows
// the first frame; columns missing from a later frame are filled with NaN,
// matching DataFrame.Concat. A single argument is copied and returned.
func Concat(dfs ...DataFrame) DataFrame {
	if len(dfs) == 0 {
		return DataFrame{Err: withSentinel("concat: no DataFrames provided", ErrEmptyDataFrame)}
	}
	out := dfs[0].Copy()
	for _, df := range dfs[1:] {
		out = out.Concat(df)
		if out.Err != nil {
			return DataFrame{Err: fmt.Errorf("concat: %w", out.Err)}
		}
	}
	return out
}

// ConcatColumns binds any number of DataFrames horizontally, matching
// CBind semantics: all frames must have the same number of rows and
// distinct column names. A single argument is copied and returned.
func ConcatColumns(dfs ...DataFrame) DataFrame {
	if len(dfs) == 0 {
		return DataFrame{Err: withSentinel("concat columns: no DataFrames provided", ErrEmptyDataFrame)}
	}
	out := dfs[0].Copy()
	for _, df := range dfs[1:] {
		out = out.CBind(df)
		if out.Err != nil {
			return DataFrame{Err: fmt.Errorf("concat columns: %w", out.Err)}
		}
	}
	return out
}

// FillNaNStrategy fills missing values using the strategy. It is the
// series-spelled alias of FillNAStrategy.
func (df DataFrame) FillNaNStrategy(strategy NAFillStrategy, subset ...string) DataFrame {
	return df.FillNAStrategy(strategy, subset...)
}

// FillNaNStrategyLimit fills missing values using the strategy with a cap
// on consecutive fills. It is the series-spelled alias of
// FillNAStrategyLimit.
func (df DataFrame) FillNaNStrategyLimit(strategy NAFillStrategy, limit int, subset ...string) DataFrame {
	return df.FillNAStrategyLimit(strategy, limit, subset...)
}
