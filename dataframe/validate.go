package dataframe

import (
	"fmt"

	"github.com/dreamsxin/gota/series"
)

// validateBasic checks basic DataFrame validity
func (df DataFrame) validateBasic() error {
	if df.Err != nil {
		return df.Err
	}

	if len(df.columns) == 0 {
		return ErrNoColumns
	}

	// Use Types() method instead of direct field access
	types := df.Types()
	if len(df.columns) != len(types) {
		return &Error{
			Message: fmt.Sprintf("columns/types length mismatch: %d vs %d", len(df.columns), len(types)),
			Op:      "validate",
		}
	}

	// Check that all columns have consistent row counts
	for i, col := range df.columns {
		if col.Len() != df.nrows {
			return &Error{
				Message: fmt.Sprintf("column %d has inconsistent row count: expected %d, got %d", i, df.nrows, col.Len()),
				Op:      "validate",
			}
		}
	}

	return nil
}

// validateColumnIndex checks if column index is valid
func (df DataFrame) validateColumnIndex(idx int) error {
	if idx < 0 || idx >= len(df.columns) {
		return &Error{
			Message: fmt.Sprintf("index %d out of range [0, %d)", idx, len(df.columns)),
			Op:      "validate",
		}
	}
	return nil
}

// validateRowIndex checks if row index is valid
func (df DataFrame) validateRowIndex(idx int) error {
	if df.nrows == 0 {
		return ErrNoRows
	}
	if idx < 0 || idx >= df.nrows {
		return &Error{
			Message: fmt.Sprintf("row index %d out of range [0, %d)", idx, df.nrows),
			Op:      "validate",
		}
	}
	return nil
}

// validateColumnName checks if column name exists and returns its index
func (df DataFrame) validateColumnName(name string) (int, error) {
	for i, c := range df.columns {
		if c.Name == name {
			return i, nil
		}
	}
	return -1, &Error{
		Message: fmt.Sprintf("column %q not found", name),
		Op:      "validate",
		Col:     name,
	}
}

// validateColumnType checks if column has expected type
func (df DataFrame) validateColumnType(colIdx int, expected series.Type) error {
	if err := df.validateColumnIndex(colIdx); err != nil {
		return err
	}

	actualType := df.columns[colIdx].Type()
	if actualType != expected {
		return &Error{
			Message: fmt.Sprintf("type mismatch: expected %v, got %v", expected, actualType),
			Op:      "validate",
			Col:     df.columns[colIdx].Name,
		}
	}
	return nil
}

// validateSeriesLength checks if series length matches DataFrame row count
func (df DataFrame) validateSeriesLength(s series.Series, colName string) error {
	expectedLen := df.Nrow()
	if s.Len() != expectedLen {
		return &Error{
			Message: fmt.Sprintf("length mismatch: expected %d, got %d", expectedLen, s.Len()),
			Op:      "validate",
			Col:     colName,
		}
	}
	return nil
}

// validateColumnNames checks for duplicate column names
func validateColumnNames(names []string) error {
	seen := make(map[string]bool)
	for _, name := range names {
		if name == "" {
			return &Error{
				Message: "empty column name not allowed",
				Op:      "validate",
			}
		}
		if seen[name] {
			return &Error{
				Message: fmt.Sprintf("duplicate column name: %q", name),
				Op:      "validate",
				Col:     name,
			}
		}
		seen[name] = true
	}
	return nil
}

// validateSeriesTypes checks if series types match expected types
func validateSeriesTypes(seriesList []series.Series, types []series.Type) error {
	if len(seriesList) != len(types) {
		return &Error{
			Message: fmt.Sprintf("series/types count mismatch: %d vs %d", len(seriesList), len(types)),
			Op:      "validate",
		}
	}

	for i, s := range seriesList {
		if s.Type() != types[i] {
			return &Error{
				Message: fmt.Sprintf("series[%d] type mismatch: expected %v, got %v", i, types[i], s.Type()),
				Op:      "validate",
			}
		}
	}
	return nil
}

// validateNotEmpty checks if DataFrame has data
func (df DataFrame) validateNotEmpty() error {
	if df.Nrow() == 0 {
		return ErrNoRows
	}
	return nil
}

// validateFilter checks if filter references valid columns
func (df DataFrame) validateFilter(colName string) error {
	_, err := df.validateColumnName(colName)
	if err != nil {
		return &Error{
			Message: fmt.Sprintf("invalid filter column: %v", err),
			Op:      "validateFilter",
			Col:     colName,
		}
	}
	return nil
}
