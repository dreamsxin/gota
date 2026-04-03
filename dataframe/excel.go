package dataframe

import (
	"fmt"
	"io"
	"os"

	"github.com/dreamsxin/gota/series"
	"github.com/xuri/excelize/v2"
)

func openFile(path string) (*os.File, error)   { return os.Open(path) }
func createFile(path string) (*os.File, error) { return os.Create(path) }

// WithSheet returns a LoadOption that selects a specific sheet by name when
// reading XLSX files. If not specified, the first sheet is used.
//
// Example:
//
//	df := dataframe.ReadXLSXFile("data.xlsx", dataframe.WithSheet("Sheet2"))
func WithSheet(name string) LoadOption {
	return func(cfg *loadOptions) {
		cfg.sheet = name
	}
}

// ReadXLSX reads the first (or named) sheet of an XLSX file from r and
// returns a DataFrame.  The first row is used as column headers by default.
//
// Options:
//   - HasHeader(bool)     – whether the first row contains column names (default true)
//   - Names(...)          – override column names
//   - WithTypes(map)      – specify column types explicitly
//   - NaNValues([]string) – additional strings to treat as NaN
//   - WithSheet(name)     – sheet name to read (default: first sheet)
func ReadXLSX(r io.Reader, options ...LoadOption) DataFrame {
	cfg := loadOptions{
		defaultType: series.String,
		detectTypes: true,
		hasHeader:   true,
		nanValues:   []string{"NA", "NaN", "<nil>", ""},
	}
	for _, opt := range options {
		opt(&cfg)
	}

	f, err := excelize.OpenReader(r)
	if err != nil {
		return DataFrame{Err: fmt.Errorf("ReadXLSX: %v", err)}
	}
	defer f.Close()

	sheetName := cfg.sheet
	if sheetName == "" {
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			return DataFrame{Err: fmt.Errorf("ReadXLSX: workbook has no sheets")}
		}
		sheetName = sheets[0]
	}

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return DataFrame{Err: fmt.Errorf("ReadXLSX: %v", err)}
	}
	if len(rows) == 0 {
		return DataFrame{Err: fmt.Errorf("ReadXLSX: sheet %q is empty", sheetName)}
	}

	// Normalise all rows to the same width.
	maxCols := 0
	for _, row := range rows {
		if len(row) > maxCols {
			maxCols = len(row)
		}
	}
	for i := range rows {
		for len(rows[i]) < maxCols {
			rows[i] = append(rows[i], "")
		}
	}

	return LoadRecords(rows, options...)
}

// WriteXLSX writes the DataFrame to w as an XLSX file.  The first row
// contains the column headers.
//
// Options:
//   - WriteHeader(bool) – whether to write the header row (default true)
//   - WithSheetName(name) – sheet name (default "Sheet1")
func (df DataFrame) WriteXLSX(w io.Writer, options ...WriteOption) error {
	if df.Err != nil {
		return df.Err
	}
	cfg := writeOptions{writeHeader: true}
	for _, opt := range options {
		opt(&cfg)
	}

	f := excelize.NewFile()
	defer f.Close()

	sheetName := "Sheet1"
	records := df.Records() // first row is header
	startRow := 0
	if !cfg.writeHeader {
		startRow = 1
	}

	for i := startRow; i < len(records); i++ {
		for j, cell := range records[i] {
			coord, err := excelize.CoordinatesToCellName(j+1, i-startRow+1)
			if err != nil {
				return fmt.Errorf("WriteXLSX: %v", err)
			}
			if err := f.SetCellValue(sheetName, coord, cell); err != nil {
				return fmt.Errorf("WriteXLSX: %v", err)
			}
		}
	}

	_, err := f.WriteTo(w)
	return err
}

// ReadXLSXFile is a convenience wrapper that opens a file path and calls ReadXLSX.
func ReadXLSXFile(path string, options ...LoadOption) DataFrame {
	f, err := openFile(path)
	if err != nil {
		return DataFrame{Err: fmt.Errorf("ReadXLSXFile: %v", err)}
	}
	defer f.Close()
	return ReadXLSX(f, options...)
}

// WriteXLSXFile is a convenience wrapper that creates/truncates a file and calls WriteXLSX.
func (df DataFrame) WriteXLSXFile(path string, options ...WriteOption) error {
	f, err := createFile(path)
	if err != nil {
		return fmt.Errorf("WriteXLSXFile: %v", err)
	}
	defer f.Close()
	return df.WriteXLSX(f, options...)
}
