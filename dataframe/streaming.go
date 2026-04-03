package dataframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dreamsxin/gota/series"
)

// ============================================================================
// ReadCSV streaming mode
// ============================================================================

// ScanCSV reads a CSV stream row by row and calls fn for each batch of rows.
// This avoids loading the entire file into memory.
//
// Parameters:
//   - r: the CSV reader source
//   - batchSize: number of rows per batch (0 = all rows at once, same as ReadCSV)
//   - fn: callback receiving each batch DataFrame; return an error to stop early
//   - options: same LoadOptions as ReadCSV
//
// Example:
//
//	err := dataframe.ScanCSV(f, 1000, func(batch dataframe.DataFrame) error {
//	    fmt.Println(batch.Nrow(), "rows processed")
//	    return nil
//	})
func ScanCSV(r io.Reader, batchSize int, fn func(DataFrame) error, options ...LoadOption) error {
	cfg := loadOptions{
		delimiter:   ',',
		lazyQuotes:  false,
		comment:     0,
		hasHeader:   true,
		detectTypes: true,
		defaultType: series.String,
		nanValues:   []string{"NA", "NaN", "<nil>", ""},
	}
	for _, opt := range options {
		opt(&cfg)
	}

	csvReader := csv.NewReader(r)
	csvReader.Comma = cfg.delimiter
	csvReader.LazyQuotes = cfg.lazyQuotes
	csvReader.Comment = cfg.comment

	// Read header row.
	var header []string
	if cfg.hasHeader {
		var err error
		header, err = csvReader.Read()
		if err != nil {
			return fmt.Errorf("ScanCSV: reading header: %v", err)
		}
	}

	if batchSize <= 0 {
		// Fall back to reading all at once.
		records, err := csvReader.ReadAll()
		if err != nil {
			return fmt.Errorf("ScanCSV: %v", err)
		}
		if cfg.hasHeader {
			records = append([][]string{header}, records...)
		}
		return fn(LoadRecords(records, options...))
	}

	batch := make([][]string, 0, batchSize+1)
	if cfg.hasHeader {
		batch = append(batch, header)
	}

	flush := func() error {
		if len(batch) == 0 || (cfg.hasHeader && len(batch) == 1) {
			return nil
		}
		df := LoadRecords(batch, options...)
		if df.Err != nil {
			return df.Err
		}
		if err := fn(df); err != nil {
			return err
		}
		// Reset batch, keeping header.
		if cfg.hasHeader {
			batch = batch[:1]
		} else {
			batch = batch[:0]
		}
		return nil
	}

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("ScanCSV: %v", err)
		}
		batch = append(batch, row)
		dataRows := len(batch)
		if cfg.hasHeader {
			dataRows--
		}
		if dataRows >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// ============================================================================
// DataFrame.Query — simple expression-based row filter
// ============================================================================

// Query filters rows using a simple expression string.
// Supported syntax: "<colname> <op> <value>"
// Operators: ==, !=, >, >=, <, <=, in, not in
// Multiple conditions can be combined with AND or OR (case-insensitive).
//
// Examples:
//
//	df.Query("age > 18")
//	df.Query("status == active")
//	df.Query("age >= 18 AND age <= 65")
//	df.Query("country in US,UK,CA")
//	df.Query("score > 0.5 OR label == good")
func (df DataFrame) Query(expr string) DataFrame {
	if df.Err != nil {
		return df
	}
	if strings.TrimSpace(expr) == "" {
		return df.Copy()
	}

	// Split on AND / OR (case-insensitive), preserving the operator.
	type clause struct {
		op   string // "AND" or "OR" (empty for first)
		cond string
	}
	var clauses []clause
	rest := strings.TrimSpace(expr)
	for rest != "" {
		// Find next AND/OR boundary.
		andIdx := indexWordCI(rest, "AND")
		orIdx := indexWordCI(rest, "OR")

		var splitAt int
		var splitOp string
		switch {
		case andIdx == -1 && orIdx == -1:
			splitAt = -1
		case andIdx == -1:
			splitAt, splitOp = orIdx, "OR"
		case orIdx == -1:
			splitAt, splitOp = andIdx, "AND"
		case andIdx < orIdx:
			splitAt, splitOp = andIdx, "AND"
		default:
			splitAt, splitOp = orIdx, "OR"
		}

		if splitAt == -1 {
			clauses = append(clauses, clause{cond: strings.TrimSpace(rest)})
			break
		}
		clauses = append(clauses, clause{cond: strings.TrimSpace(rest[:splitAt])})
		rest = strings.TrimSpace(rest[splitAt+len(splitOp):])
		// Tag the *next* clause with the operator that precedes it.
		if len(clauses) > 0 {
			clauses[len(clauses)-1].op = splitOp
		}
	}

	// Evaluate each clause into a []bool mask.
	masks := make([][]bool, len(clauses))
	for i, c := range clauses {
		mask, err := df.evalQueryClause(c.cond)
		if err != nil {
			return DataFrame{Err: fmt.Errorf("Query: %v", err)}
		}
		masks[i] = mask
	}

	// Combine masks.
	result := masks[0]
	for i := 1; i < len(clauses); i++ {
		op := clauses[i-1].op
		for j := range result {
			switch strings.ToUpper(op) {
			case "OR":
				result[j] = result[j] || masks[i][j]
			default: // AND
				result[j] = result[j] && masks[i][j]
			}
		}
	}
	return df.Subset(result)
}

// evalQueryClause evaluates a single "col op value" clause.
func (df DataFrame) evalQueryClause(cond string) ([]bool, error) {
	cond = strings.TrimSpace(cond)

	// Try two-word operators first (not in).
	ops2 := []string{"not in", ">=", "<=", "!=", "==", ">", "<", "in"}
	var op, colPart, valPart string
	for _, candidate := range ops2 {
		idx := strings.Index(strings.ToLower(cond), candidate)
		if idx < 0 {
			continue
		}
		colPart = strings.TrimSpace(cond[:idx])
		valPart = strings.TrimSpace(cond[idx+len(candidate):])
		op = candidate
		break
	}
	if op == "" {
		return nil, fmt.Errorf("unrecognised expression: %q", cond)
	}

	col := df.Col(colPart)
	if col.Err != nil {
		return nil, fmt.Errorf("column %q not found", colPart)
	}

	n := df.nrows
	result := make([]bool, n)

	switch strings.ToLower(op) {
	case "in", "not in":
		vals := strings.Split(valPart, ",")
		lookup := make(map[string]struct{}, len(vals))
		for _, v := range vals {
			lookup[strings.TrimSpace(v)] = struct{}{}
		}
		for i := 0; i < n; i++ {
			_, found := lookup[col.Elem(i).String()]
			if strings.ToLower(op) == "in" {
				result[i] = found
			} else {
				result[i] = !found
			}
		}
	default:
		// Numeric comparison if possible, else string.
		numVal, numErr := strconv.ParseFloat(valPart, 64)
		for i := 0; i < n; i++ {
			elem := col.Elem(i)
			if elem.IsNA() {
				result[i] = false
				continue
			}
			if numErr == nil {
				ev := elem.Float()
				switch op {
				case "==":
					result[i] = ev == numVal
				case "!=":
					result[i] = ev != numVal
				case ">":
					result[i] = ev > numVal
				case ">=":
					result[i] = ev >= numVal
				case "<":
					result[i] = ev < numVal
				case "<=":
					result[i] = ev <= numVal
				}
			} else {
				es := elem.String()
				switch op {
				case "==":
					result[i] = es == valPart
				case "!=":
					result[i] = es != valPart
				case ">":
					result[i] = es > valPart
				case ">=":
					result[i] = es >= valPart
				case "<":
					result[i] = es < valPart
				case "<=":
					result[i] = es <= valPart
				}
			}
		}
	}
	return result, nil
}

// indexWordCI finds the byte index of word (case-insensitive) as a whole word
// (surrounded by spaces or at string boundaries) in s. Returns -1 if not found.
func indexWordCI(s, word string) int {
	lower := strings.ToLower(s)
	lword := strings.ToLower(word)
	start := 0
	for {
		idx := strings.Index(lower[start:], lword)
		if idx < 0 {
			return -1
		}
		abs := start + idx
		// Check word boundaries.
		before := abs == 0 || lower[abs-1] == ' '
		after := abs+len(lword) >= len(lower) || lower[abs+len(lword)] == ' '
		if before && after {
			return abs
		}
		start = abs + 1
	}
}
