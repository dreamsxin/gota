package dataframe

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/dreamsxin/gota/series"
)

// FromSQL creates a DataFrame from a *sql.Rows result set.
// The column names and types are inferred from the SQL metadata.
// Supported SQL types are mapped to series types; everything else becomes String.
//
// Example:
//
//	rows, _ := db.Query("SELECT * FROM users")
//	df := dataframe.FromSQL(rows)
func FromSQL(rows *sql.Rows) DataFrame {
	if rows == nil {
		return DataFrame{Err: fmt.Errorf("FromSQL: rows is nil")}
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return DataFrame{Err: fmt.Errorf("FromSQL: %v", err)}
	}

	ncols := len(colTypes)
	colNames := make([]string, ncols)
	seriesTypes := make([]series.Type, ncols)

	for i, ct := range colTypes {
		colNames[i] = ct.Name()
		seriesTypes[i] = sqlTypeToSeriesType(ct.DatabaseTypeName())
	}

	// Collect raw string values row by row (simplest approach; avoids
	// reflect gymnastics and handles NULLs uniformly).
	rawCols := make([][]string, ncols)
	for i := range rawCols {
		rawCols[i] = []string{}
	}

	scanDest := make([]interface{}, ncols)
	scanPtrs := make([]interface{}, ncols)
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return DataFrame{Err: fmt.Errorf("FromSQL: scan error: %v", err)}
		}
		for i, val := range scanDest {
			if val == nil {
				rawCols[i] = append(rawCols[i], "NaN")
			} else {
				rawCols[i] = append(rawCols[i], fmt.Sprintf("%v", val))
			}
		}
	}
	if err := rows.Err(); err != nil {
		return DataFrame{Err: fmt.Errorf("FromSQL: rows error: %v", err)}
	}

	// Build series.
	cols := make([]series.Series, ncols)
	for i, name := range colNames {
		cols[i] = series.New(rawCols[i], seriesTypes[i], name)
		if cols[i].Err != nil {
			return DataFrame{Err: fmt.Errorf("FromSQL: series error on column %q: %v", name, cols[i].Err)}
		}
	}
	return New(cols...)
}

// sqlTypeToSeriesType maps common SQL type names to series.Type.
func sqlTypeToSeriesType(dbTypeName string) series.Type {
	switch strings.ToUpper(dbTypeName) {
	case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
		"INT2", "INT4", "INT8", "INT64", "UNSIGNED BIG INT":
		return series.Int
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "NUMERIC", "DECIMAL",
		"FLOAT4", "FLOAT8":
		return series.Float
	case "BOOL", "BOOLEAN":
		return series.Bool
	case "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ":
		return series.Time
	default:
		return series.String
	}
}

// SQLInsertOption configures DataFrame.WriteSQL behaviour.
type SQLInsertOption func(*sqlInsertOptions)

type sqlInsertOptions struct {
	batchSize    int    // rows per INSERT statement (default 500)
	createTable  bool   // create the table if it doesn't exist
	truncateFirst bool  // TRUNCATE / DELETE FROM before inserting
}

// WithBatchSize sets how many rows are inserted per statement.
func WithBatchSize(n int) SQLInsertOption {
	return func(o *sqlInsertOptions) { o.batchSize = n }
}

// WithCreateTable tells WriteSQL to issue a CREATE TABLE IF NOT EXISTS before inserting.
func WithCreateTable(b bool) SQLInsertOption {
	return func(o *sqlInsertOptions) { o.createTable = b }
}

// WithTruncateFirst tells WriteSQL to delete all rows in the table before inserting.
func WithTruncateFirst(b bool) SQLInsertOption {
	return func(o *sqlInsertOptions) { o.truncateFirst = b }
}

// WriteSQL inserts the DataFrame into a SQL table using db.
// tableName is the destination table.  Column names are taken from the DataFrame.
//
// Example:
//
//	err := df.WriteSQL(db, "my_table", dataframe.WithCreateTable(true))
func (df DataFrame) WriteSQL(db *sql.DB, tableName string, opts ...SQLInsertOption) error {
	if df.Err != nil {
		return df.Err
	}
	cfg := sqlInsertOptions{batchSize: 500}
	for _, o := range opts {
		o(&cfg)
	}

	colNames := df.Names()
	ncols := df.ncols
	nrows := df.nrows

	// Optionally create table.
	if cfg.createTable {
		ddl := buildCreateTable(tableName, colNames, df.Types())
		if _, err := db.Exec(ddl); err != nil {
			return fmt.Errorf("WriteSQL: CREATE TABLE: %v", err)
		}
	}

	// Optionally truncate.
	if cfg.truncateFirst {
		if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", tableName)); err != nil {
			return fmt.Errorf("WriteSQL: truncate: %v", err)
		}
	}

	// Build quoted column list.
	quotedCols := make([]string, ncols)
	for i, n := range colNames {
		quotedCols[i] = fmt.Sprintf(`"%s"`, n)
	}
	colList := strings.Join(quotedCols, ", ")
	placeholders := "(" + strings.Join(repeatStr("?", ncols), ", ") + ")"

	// Insert in batches.
	records := df.Records() // first row is header
	rows := records[1:]

	for start := 0; start < nrows; start += cfg.batchSize {
		end := start + cfg.batchSize
		if end > nrows {
			end = nrows
		}
		batch := rows[start:end]
		batchLen := len(batch)

		allPlaceholders := make([]string, batchLen)
		for i := range batch {
			allPlaceholders[i] = placeholders
		}
		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			tableName, colList, strings.Join(allPlaceholders, ", "))

		args := make([]interface{}, 0, batchLen*ncols)
		for _, row := range batch {
			for _, cell := range row {
				if cell == "NaN" {
					args = append(args, nil)
				} else {
					args = append(args, cell)
				}
			}
		}
		if _, err := db.Exec(stmt, args...); err != nil {
			return fmt.Errorf("WriteSQL: insert batch starting at row %d: %v", start, err)
		}
	}
	return nil
}

// buildCreateTable generates a simple CREATE TABLE IF NOT EXISTS DDL statement.
func buildCreateTable(tableName string, colNames []string, colTypes []series.Type) string {
	cols := make([]string, len(colNames))
	for i, n := range colNames {
		cols[i] = fmt.Sprintf(`"%s" %s`, n, seriesTypeToSQL(colTypes[i]))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, strings.Join(cols, ", "))
}

func seriesTypeToSQL(t series.Type) string {
	switch t {
	case series.Int:
		return "INTEGER"
	case series.Float:
		return "REAL"
	case series.Bool:
		return "BOOLEAN"
	case series.Time:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

func repeatStr(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}
