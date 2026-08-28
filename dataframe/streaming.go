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

	prepared, delimiter, err := prepareCSVReader(r, cfg)
	if err != nil {
		return fmt.Errorf("ScanCSV: %v", err)
	}
	csvReader := csv.NewReader(prepared)
	csvReader.Comma = delimiter
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
		// Copy batch to avoid data races: LoadRecords stores string slices
		// by reference, and we reuse the batch slice for the next window.
		snapshot := make([][]string, len(batch))
		copy(snapshot, batch)
		df := LoadRecords(snapshot, options...)
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
// AND binds more tightly than OR, and parentheses override precedence.
// Column names and values may be quoted with single or double quotes.
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

	tokens, err := tokenizeQueryExpression(expr)
	if err != nil {
		return DataFrame{Err: fmt.Errorf("Query: %w", err)}
	}
	parser := queryMaskParser{df: df, tokens: tokens}
	result, err := parser.parse()
	if err != nil {
		return DataFrame{Err: fmt.Errorf("Query: %w", err)}
	}
	return df.Subset(result)
}

type queryTokenKind uint8

const (
	queryTokenCondition queryTokenKind = iota
	queryTokenAnd
	queryTokenOr
	queryTokenLeftParen
	queryTokenRightParen
)

type queryToken struct {
	kind queryTokenKind
	text string
}

func tokenizeQueryExpression(expr string) ([]queryToken, error) {
	var tokens []queryToken
	start := 0
	var quote byte
	escaped := false

	flushCondition := func(end int) {
		if condition := strings.TrimSpace(expr[start:end]); condition != "" {
			tokens = append(tokens, queryToken{kind: queryTokenCondition, text: condition})
		}
	}

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}

		switch {
		case ch == '(':
			flushCondition(i)
			tokens = append(tokens, queryToken{kind: queryTokenLeftParen, text: "("})
			start = i + 1
		case ch == ')':
			flushCondition(i)
			tokens = append(tokens, queryToken{kind: queryTokenRightParen, text: ")"})
			start = i + 1
		case queryKeywordAt(expr, i, "AND"):
			flushCondition(i)
			tokens = append(tokens, queryToken{kind: queryTokenAnd, text: "AND"})
			i += len("AND") - 1
			start = i + 1
		case queryKeywordAt(expr, i, "OR"):
			flushCondition(i)
			tokens = append(tokens, queryToken{kind: queryTokenOr, text: "OR"})
			i += len("OR") - 1
			start = i + 1
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quoted value")
	}
	flushCondition(len(expr))
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty expression")
	}
	return tokens, nil
}

func queryKeywordAt(expr string, pos int, keyword string) bool {
	end := pos + len(keyword)
	if end > len(expr) || !strings.EqualFold(expr[pos:end], keyword) {
		return false
	}
	before := pos == 0 || isQueryBoundary(expr[pos-1])
	after := end == len(expr) || isQueryBoundary(expr[end])
	return before && after
}

func isQueryBoundary(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '(' || ch == ')'
}

type queryMaskParser struct {
	df     DataFrame
	tokens []queryToken
	pos    int
}

func (p *queryMaskParser) parse() ([]bool, error) {
	mask, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.tokens) {
		return nil, fmt.Errorf("unexpected token %q", p.tokens[p.pos].text)
	}
	return mask, nil
}

func (p *queryMaskParser) parseOr() ([]bool, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.pos < len(p.tokens) && p.tokens[p.pos].kind == queryTokenOr {
		p.pos++
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		combineQueryMasks(left, right, false)
	}
	return left, nil
}

func (p *queryMaskParser) parseAnd() ([]bool, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.pos < len(p.tokens) && p.tokens[p.pos].kind == queryTokenAnd {
		p.pos++
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		combineQueryMasks(left, right, true)
	}
	return left, nil
}

func (p *queryMaskParser) parsePrimary() ([]bool, error) {
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("incomplete expression")
	}
	token := p.tokens[p.pos]
	switch token.kind {
	case queryTokenCondition:
		p.pos++
		return p.df.evalQueryClause(token.text)
	case queryTokenLeftParen:
		p.pos++
		mask, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.pos >= len(p.tokens) || p.tokens[p.pos].kind != queryTokenRightParen {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		p.pos++
		return mask, nil
	default:
		return nil, fmt.Errorf("unexpected token %q", token.text)
	}
}

func combineQueryMasks(left, right []bool, and bool) {
	for i := range left {
		if and {
			left[i] = left[i] && right[i]
		} else {
			left[i] = left[i] || right[i]
		}
	}
}

// indexASCIIFold returns the byte index of the first ASCII case-insensitive
// match of sub in s, or -1. sub must be pure ASCII; the returned indexes stay
// valid for slicing s even when s contains non-UTF-8 bytes.
func indexASCIIFold(s, sub string) int {
	if len(sub) == 0 || len(sub) > len(s) {
		return -1
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if strings.EqualFold(s[i:i+len(sub)], sub) {
			return i
		}
	}
	return -1
}

// evalQueryClause evaluates a single "col op value" clause.
func (df DataFrame) evalQueryClause(cond string) ([]bool, error) {
	cond = strings.TrimSpace(cond)

	// Support quoted column names: `"col name" > 5` or `'col name' == foo`
	// Strip the surrounding quotes and extract the column name first.
	var quotedCol string
	if len(cond) > 0 && (cond[0] == '"' || cond[0] == '\'') {
		quote := cond[0]
		end := strings.IndexByte(cond[1:], quote)
		if end >= 0 {
			quotedCol = cond[1 : end+1]
			cond = strings.TrimSpace(cond[end+2:])
		}
	}

	// Operators ordered longest-first to avoid prefix ambiguity.
	// "not in" must come before "in"; ">=" before ">"; "<=" before "<".
	ops := []string{"not in", ">=", "<=", "!=", "==", ">", "<", "in"}
	var op, colPart, valPart string

	if quotedCol != "" {
		// Column name was quoted; the remainder is "op value".
		for _, candidate := range ops {
			lc := strings.ToLower(candidate)
			lower := strings.ToLower(cond)
			if strings.HasPrefix(lower, lc+" ") || lower == lc {
				op = candidate
				valPart = strings.TrimSpace(cond[len(candidate):])
				break
			}
		}
		colPart = quotedCol
	} else {
		for _, candidate := range ops {
			// Search case-insensitively, but require the operator to be surrounded
			// by spaces (or at string boundaries) so that column names like
			// "income" don't accidentally match "in". The search runs on cond
			// itself: strings.ToLower can change the byte length of non-UTF-8
			// input, which would desynchronize indexes against cond.
			idx := 0
			for {
				pos := indexASCIIFold(cond[idx:], candidate)
				if pos < 0 {
					break
				}
				abs := idx + pos
				before := abs == 0 || cond[abs-1] == ' '
				after := abs+len(candidate) >= len(cond) || cond[abs+len(candidate)] == ' '
				if before && after {
					colPart = strings.TrimSpace(cond[:abs])
					valPart = strings.TrimSpace(cond[abs+len(candidate):])
					op = candidate
					break
				}
				idx = abs + 1
			}
			if op != "" {
				break
			}
		}
	}

	if op == "" {
		return nil, fmt.Errorf("unrecognised expression: %q", cond)
	}
	if colPart == "" {
		return nil, fmt.Errorf("missing column name in expression: %q", cond)
	}

	col := df.Col(colPart)
	if col.Err != nil {
		return nil, withSentinel(fmt.Sprintf("column %q not found", colPart), ErrColumnNotFound)
	}

	n := df.nrows
	result := make([]bool, n)

	switch strings.ToLower(op) {
	case "in", "not in":
		vals, err := parseQueryList(valPart)
		if err != nil {
			return nil, err
		}
		lookup := make(map[string]struct{}, len(vals))
		for _, v := range vals {
			lookup[v] = struct{}{}
		}
		isIn := strings.ToLower(op) == "in"
		// Cache string representations to avoid repeated conversion.
		strs := make([]string, n)
		for i := 0; i < n; i++ {
			strs[i] = col.Record(i)
		}
		for i, s := range strs {
			_, found := lookup[s]
			result[i] = found == isIn
		}
	default:
		comparisonValue, quoted, err := parseQueryValue(valPart)
		if err != nil {
			return nil, err
		}
		// Numeric comparison if possible, else string.
		numVal, numErr := strconv.ParseFloat(comparisonValue, 64)
		if quoted {
			numErr = fmt.Errorf("quoted value")
		}
		// Cache string/float representations once to avoid repeated fmt.Sprintf per row.
		if numErr == nil {
			floats := col.Float()
			for i := 0; i < n; i++ {
				if col.IsNA(i) {
					result[i] = false
					continue
				}
				ev := floats[i]
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
			}
		} else {
			strs := col.Records() // single allocation, no per-row fmt.Sprintf
			for i := 0; i < n; i++ {
				if col.IsNA(i) {
					result[i] = false
					continue
				}
				es := strs[i]
				switch op {
				case "==":
					result[i] = es == comparisonValue
				case "!=":
					result[i] = es != comparisonValue
				case ">":
					result[i] = es > comparisonValue
				case ">=":
					result[i] = es >= comparisonValue
				case "<":
					result[i] = es < comparisonValue
				case "<=":
					result[i] = es <= comparisonValue
				}
			}
		}
	}
	return result, nil
}

func parseQueryList(input string) ([]string, error) {
	var values []string
	start := 0
	var quote byte
	escaped := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == ',' {
			value, _, err := parseQueryValue(input[start:i])
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			start = i + 1
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quoted value")
	}
	value, _, err := parseQueryValue(input[start:])
	if err != nil {
		return nil, err
	}
	return append(values, value), nil
}

func parseQueryValue(input string) (value string, quoted bool, err error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", false, fmt.Errorf("missing comparison value")
	}
	if input[0] != '\'' && input[0] != '"' {
		return input, false, nil
	}

	quote := input[0]
	var sb strings.Builder
	escaped := false
	for i := 1; i < len(input); i++ {
		ch := input[i]
		if escaped {
			if ch != quote && ch != '\\' {
				sb.WriteByte('\\')
			}
			sb.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == quote {
			if strings.TrimSpace(input[i+1:]) != "" {
				return "", false, fmt.Errorf("unexpected text after quoted value")
			}
			return sb.String(), true, nil
		}
		sb.WriteByte(ch)
	}
	return "", false, fmt.Errorf("unterminated quoted value")
}
