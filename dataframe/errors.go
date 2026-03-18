package dataframe

import (
	"errors"
	"fmt"
	"strconv"
)

// Common DataFrame errors
var (
	// ErrEmptyDataFrame is returned when operating on an empty DataFrame
	ErrEmptyDataFrame = errors.New("dataframe: empty DataFrame")

	// ErrNoColumns is returned when DataFrame has no columns
	ErrNoColumns = errors.New("dataframe: no columns defined")

	// ErrNoRows is returned when DataFrame has no rows
	ErrNoRows = errors.New("dataframe: no rows")

	// ErrColumnNotFound is returned when a column name doesn't exist
	ErrColumnNotFound = errors.New("dataframe: column not found")

	// ErrColumnExists is returned when trying to add a column that already exists
	ErrColumnExists = errors.New("dataframe: column already exists")

	// ErrIndexOutOfRange is returned when row/column index is invalid
	ErrIndexOutOfRange = errors.New("dataframe: index out of range")

	// ErrTypeMismatch is returned when column types don't match expected type
	ErrTypeMismatch = errors.New("dataframe: type mismatch")

	// ErrLengthMismatch is returned when series lengths don't match
	ErrLengthMismatch = errors.New("dataframe: length mismatch")

	// ErrDuplicateColumns is returned when duplicate column names are detected
	ErrDuplicateColumns = errors.New("dataframe: duplicate column names")

	// ErrInvalidFilter is returned when filter expression is invalid
	ErrInvalidFilter = errors.New("dataframe: invalid filter")

	// ErrInvalidAggregation is returned when aggregation method is unknown
	ErrInvalidAggregation = errors.New("dataframe: invalid aggregation method")

	// ErrInvalidJoin is returned when join type is unknown
	ErrInvalidJoin = errors.New("dataframe: invalid join type")

	// ErrKeyNotFound is returned when join key column doesn't exist
	ErrKeyNotFound = errors.New("dataframe: join key not found")

	// ErrEmptyKeys is returned when no join keys are provided
	ErrEmptyKeys = errors.New("dataframe: no join keys provided")

	// ErrIncompatibleSchema is returned when joining DataFrames with incompatible schemas
	ErrIncompatibleSchema = errors.New("dataframe: incompatible schemas")

	// ErrReadOnly is returned when trying to modify a read-only DataFrame
	ErrReadOnly = errors.New("dataframe: read-only operation")

	// ErrConcurrentFailure is returned when concurrent operation fails
	ErrConcurrentFailure = errors.New("dataframe: concurrent operation failed")
)

// Errorf creates a new error with format
func Errorf(format string, args ...interface{}) error {
	return &Error{
		Message: fmt.Sprintf(format, args...),
	}
}

// Error is a DataFrame error with context
type Error struct {
	Message string
	Op      string // operation name
	Col     string // column name (if applicable)
	Row     int    // row index (if applicable)
	Err     error  // underlying error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	s := e.Message
	if e.Op != "" {
		s = e.Op + ": " + s
	}
	if e.Col != "" {
		s = s + " (column: " + e.Col + ")"
	}
	if e.Row >= 0 {
		s = s + " (row: " + strconv.Itoa(e.Row) + ")"
	}
	if e.Err != nil {
		s = s + ": " + e.Err.Error()
	}
	return s
}

// Unwrap returns the underlying error
func (e *Error) Unwrap() error {
	return e.Err
}
