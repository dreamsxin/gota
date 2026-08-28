// Package excel reads and writes XLSX files for gota/v2 DataFrames. It is
// the v2 adapter module for Excel I/O: the heavy excelize dependency lives
// here, not in the core dataframe module.
package excel

import (
	"fmt"
	"io"
	"os"

	"github.com/dreamsxin/gota/v2/dataframe"
	"github.com/dreamsxin/gota/v2/series"
	"github.com/xuri/excelize/v2"
)

func openFile(path string) (*os.File, error)   { return os.Open(path) }
func createFile(path string) (*os.File, error) { return os.Create(path) }

// Option configures XLSX reading.
type Option func(*options)

type options struct {
	// sheet selects the sheet to read by name; empty means the first sheet.
	sheet string
	// loadOpts are passed through to dataframe.LoadRecords.
	loadOpts []dataframe.LoadOption
}

// WithSheet selects a specific sheet by name when reading XLSX files. If not
// specified, the first sheet is used.
//
// Example:
//
//	df := excel.ReadXLSXFile("data.xlsx", excel.WithSheet("Sheet2"))
func WithSheet(name string) Option {
	return func(cfg *options) {
		cfg.sheet = name
	}
}

// WithLoadOptions passes core load options (HasHeader, Names, WithTypes,
// NaNValues, ...) through to dataframe.LoadRecords.
func WithLoadOptions(opts ...dataframe.LoadOption) Option {
	return func(cfg *options) {
		cfg.loadOpts = append(cfg.loadOpts, opts...)
	}
}

// WriteOption configures XLSX writing.
type WriteOption func(*writeOptions)

type writeOptions struct {
	writeHeader   bool
	sheetName     string
	boldHeader    bool
	columnWidths  map[string]float64
	numberFormats map[string]string
}

// WithHeader sets whether the header row is written (default true).
func WithHeader(b bool) WriteOption {
	return func(c *writeOptions) { c.writeHeader = b }
}

// WithSheetName sets the sheet name used by WriteXLSX and WriteXLSXFile.
func WithSheetName(name string) WriteOption {
	return func(c *writeOptions) { c.sheetName = name }
}

// WithBoldHeader writes the header row in bold.
func WithBoldHeader(b bool) WriteOption {
	return func(c *writeOptions) { c.boldHeader = b }
}

// WithColumnWidths sets column widths by DataFrame column name.
func WithColumnWidths(widths map[string]float64) WriteOption {
	return func(c *writeOptions) { c.columnWidths = widths }
}

// WithNumberFormats sets custom number formats by DataFrame column name.
func WithNumberFormats(formats map[string]string) WriteOption {
	return func(c *writeOptions) { c.numberFormats = formats }
}

// ReadXLSX reads the first (or named) sheet of an XLSX file from r and
// returns a DataFrame. The first row is used as column headers by default.
//
// Options:
//   - WithSheet(name)               - sheet name to read (default: first sheet)
//   - WithLoadOptions(...)          - core load options, e.g.
//     dataframe.HasHeader(bool), dataframe.Names(...), dataframe.WithTypes(map),
//     dataframe.NaNValues([]string)
func ReadXLSX(r io.Reader, opts ...Option) dataframe.DataFrame {
	cfg := options{}
	for _, opt := range opts {
		opt(&cfg)
	}
	loadOpts := append([]dataframe.LoadOption{
		dataframe.DefaultType(series.String),
		dataframe.HasHeader(true),
		dataframe.NaNValues([]string{"NA", "NaN", "<nil>", ""}),
	}, cfg.loadOpts...)

	f, err := excelize.OpenReader(r)
	if err != nil {
		return dataframe.ErrorFrame(fmt.Errorf("ReadXLSX: %v", err))
	}
	defer f.Close()

	sheetName := cfg.sheet
	if sheetName == "" {
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			return dataframe.ErrorFrame(fmt.Errorf("ReadXLSX: workbook has no sheets: %w", dataframe.ErrEmptyDataFrame))
		}
		sheetName = sheets[0]
	}

	return readXLSXSheet(f, sheetName, loadOpts...)
}

func readXLSXSheet(f *excelize.File, sheetName string, loadOpts ...dataframe.LoadOption) dataframe.DataFrame {
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return dataframe.ErrorFrame(fmt.Errorf("ReadXLSX: %v", err))
	}
	if len(rows) == 0 {
		return dataframe.ErrorFrame(fmt.Errorf("ReadXLSX: sheet %q is empty: %w", sheetName, dataframe.ErrEmptyDataFrame))
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

	return dataframe.LoadRecords(rows, loadOpts...)
}

// ReadXLSXSheets reads every sheet in an XLSX file into a map keyed by sheet
// name. This mirrors pandas read_excel(sheet_name=None).
func ReadXLSXSheets(r io.Reader, opts ...Option) (map[string]dataframe.DataFrame, error) {
	cfg := options{}
	for _, opt := range opts {
		opt(&cfg)
	}
	loadOpts := append([]dataframe.LoadOption{
		dataframe.DefaultType(series.String),
		dataframe.HasHeader(true),
		dataframe.NaNValues([]string{"NA", "NaN", "<nil>", ""}),
	}, cfg.loadOpts...)

	f, err := excelize.OpenReader(r)
	if err != nil {
		return nil, fmt.Errorf("ReadXLSXSheets: %v", err)
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("ReadXLSXSheets: workbook has no sheets")
	}

	out := make(map[string]dataframe.DataFrame, len(sheets))
	for _, sheetName := range sheets {
		df := readXLSXSheet(f, sheetName, loadOpts...)
		if df.Err != nil {
			return nil, fmt.Errorf("ReadXLSXSheets: sheet %q: %v", sheetName, df.Err)
		}
		out[sheetName] = df
	}
	return out, nil
}

// WriteXLSX writes the DataFrame to w as an XLSX file. The first row
// contains the column headers.
//
// Options:
//   - WithHeader(bool)      - whether to write the header row (default true)
//   - WithSheetName(name)   - sheet name (default "Sheet1")
func WriteXLSX(df dataframe.DataFrame, w io.Writer, opts ...WriteOption) error {
	if df.Err != nil {
		return df.Err
	}
	cfg := writeOptions{writeHeader: true}
	for _, opt := range opts {
		opt(&cfg)
	}

	f := excelize.NewFile()
	defer f.Close()

	sheetName := cfg.sheetName
	if sheetName == "" {
		sheetName = "Sheet1"
	}
	if sheetName != "Sheet1" {
		if err := f.SetSheetName("Sheet1", sheetName); err != nil {
			return fmt.Errorf("WriteXLSX: sheet name: %v", err)
		}
	}

	if err := writeXLSXToSheet(f, sheetName, df, cfg); err != nil {
		return fmt.Errorf("WriteXLSX: %w", err)
	}

	_, err := f.WriteTo(w)
	return err
}

// ReadXLSXFile is a convenience wrapper that opens a file path and calls ReadXLSX.
func ReadXLSXFile(path string, opts ...Option) dataframe.DataFrame {
	f, err := openFile(path)
	if err != nil {
		return dataframe.ErrorFrame(fmt.Errorf("ReadXLSXFile: %v", err))
	}
	defer f.Close()
	return ReadXLSX(f, opts...)
}

// ReadXLSXFileSheets is a convenience wrapper that opens a file path and
// calls ReadXLSXSheets.
func ReadXLSXFileSheets(path string, opts ...Option) (map[string]dataframe.DataFrame, error) {
	f, err := openFile(path)
	if err != nil {
		return nil, fmt.Errorf("ReadXLSXFileSheets: %v", err)
	}
	defer f.Close()
	return ReadXLSXSheets(f, opts...)
}

// WriteXLSXFile is a convenience wrapper that creates/truncates a file and
// calls WriteXLSX.
func WriteXLSXFile(df dataframe.DataFrame, path string, opts ...WriteOption) error {
	f, err := createFile(path)
	if err != nil {
		return fmt.Errorf("WriteXLSXFile: %w", err)
	}
	defer f.Close()
	return WriteXLSX(df, f, opts...)
}

// WriteXLSXSheet writes the DataFrame to a specific sheet in an existing
// excelize.File. This allows building multi-sheet workbooks.
//
// Example:
//
//	f := excelize.NewFile()
//	defer f.Close()
//	excel.WriteXLSXSheet(f, "Sales", df1)
//	excel.WriteXLSXSheet(f, "Inventory", df2)
//	f.SaveAs("report.xlsx")
func WriteXLSXSheet(f *excelize.File, sheetName string, df dataframe.DataFrame, opts ...WriteOption) error {
	if df.Err != nil {
		return df.Err
	}
	cfg := writeOptions{writeHeader: true}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Create sheet if it doesn't exist.
	idx, _ := f.GetSheetIndex(sheetName)
	if idx == -1 {
		if _, err := f.NewSheet(sheetName); err != nil {
			return fmt.Errorf("WriteXLSXSheet: %v", err)
		}
	}

	if err := writeXLSXToSheet(f, sheetName, df, cfg); err != nil {
		return fmt.Errorf("WriteXLSXSheet: %v", err)
	}
	return nil
}

func writeXLSXToSheet(f *excelize.File, sheetName string, df dataframe.DataFrame, cfg writeOptions) error {
	records := df.Records()
	startRow := 0
	if !cfg.writeHeader {
		startRow = 1
	}
	for i := startRow; i < len(records); i++ {
		for j, cell := range records[i] {
			coord, err := excelize.CoordinatesToCellName(j+1, i-startRow+1)
			if err != nil {
				return err
			}
			if err := f.SetCellValue(sheetName, coord, cell); err != nil {
				return err
			}
		}
	}
	return applyXLSXStyles(f, sheetName, df, cfg)
}

func applyXLSXStyles(f *excelize.File, sheetName string, df dataframe.DataFrame, cfg writeOptions) error {
	if cfg.boldHeader && cfg.writeHeader && df.Ncol() > 0 {
		styleID, err := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true}})
		if err != nil {
			return err
		}
		endCell, err := excelize.CoordinatesToCellName(df.Ncol(), 1)
		if err != nil {
			return err
		}
		if err := f.SetCellStyle(sheetName, "A1", endCell, styleID); err != nil {
			return err
		}
	}

	nameToIndex := make(map[string]int, df.Ncol())
	for i, name := range df.Names() {
		nameToIndex[name] = i + 1
	}

	for name, width := range cfg.columnWidths {
		colIdx, ok := nameToIndex[name]
		if !ok {
			return fmt.Errorf("unknown XLSX column %q: %w", name, dataframe.ErrColumnNotFound)
		}
		colName, err := excelize.ColumnNumberToName(colIdx)
		if err != nil {
			return err
		}
		if err := f.SetColWidth(sheetName, colName, colName, width); err != nil {
			return err
		}
	}

	dataStart := 1
	if cfg.writeHeader {
		dataStart = 2
	}
	for name, format := range cfg.numberFormats {
		colIdx, ok := nameToIndex[name]
		if !ok {
			return fmt.Errorf("unknown XLSX column %q: %w", name, dataframe.ErrColumnNotFound)
		}
		if df.Nrow() == 0 {
			continue
		}
		startCell, err := excelize.CoordinatesToCellName(colIdx, dataStart)
		if err != nil {
			return err
		}
		endCell, err := excelize.CoordinatesToCellName(colIdx, dataStart+df.Nrow()-1)
		if err != nil {
			return err
		}
		numberFormat := format
		styleID, err := f.NewStyle(&excelize.Style{CustomNumFmt: &numberFormat})
		if err != nil {
			return err
		}
		if err := f.SetCellStyle(sheetName, startCell, endCell, styleID); err != nil {
			return err
		}
	}
	return nil
}

// SheetData pairs a sheet name with the DataFrame written to it.
type SheetData struct {
	Name string
	DF   dataframe.DataFrame
}

// WriteXLSXMultiSheet writes multiple DataFrames to separate sheets in a
// single XLSX file.
//
// Example:
//
//	err := excel.WriteXLSXMultiSheet(w,
//	    excel.SheetData{"Sales", salesDF},
//	    excel.SheetData{"Inventory", invDF},
//	)
func WriteXLSXMultiSheet(w io.Writer, sheets ...SheetData) error {
	if len(sheets) == 0 {
		return fmt.Errorf("WriteXLSXMultiSheet: no sheets provided")
	}
	f := excelize.NewFile()
	defer f.Close()

	for i, sd := range sheets {
		if i == 0 {
			// Rename the default Sheet1.
			if err := f.SetSheetName("Sheet1", sd.Name); err != nil {
				return fmt.Errorf("WriteXLSXMultiSheet: rename sheet: %v", err)
			}
		} else {
			if _, err := f.NewSheet(sd.Name); err != nil {
				return fmt.Errorf("WriteXLSXMultiSheet: new sheet %q: %v", sd.Name, err)
			}
		}
		records := sd.DF.Records()
		for ri, row := range records {
			for ci, cell := range row {
				coord, err := excelize.CoordinatesToCellName(ci+1, ri+1)
				if err != nil {
					return fmt.Errorf("WriteXLSXMultiSheet: %v", err)
				}
				if err := f.SetCellValue(sd.Name, coord, cell); err != nil {
					return fmt.Errorf("WriteXLSXMultiSheet: %v", err)
				}
			}
		}
	}
	_, err := f.WriteTo(w)
	return err
}
