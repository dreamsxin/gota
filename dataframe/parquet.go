package dataframe

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/dreamsxin/gota/series"
)

// WriteParquet writes the DataFrame to w in Apache Parquet format.
func (df DataFrame) WriteParquet(w io.Writer) error {
	if df.Err != nil {
		return df.Err
	}

	schema := parquetSchemaFromDataFrame(df)
	columnMeta, err := json.Marshal(df.Names())
	if err != nil {
		return fmt.Errorf("WriteParquet: %v", err)
	}
	writer := parquet.NewGenericWriter[map[string]interface{}](
		w,
		schema,
		parquet.KeyValueMetadata("gota.columns", string(columnMeta)),
	)

	rows, err := parquetRowsFromDataFrame(df)
	if err != nil {
		return err
	}
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("WriteParquet: %v", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("WriteParquet: %v", err)
	}
	return nil
}

// WriteParquetFile is a convenience wrapper that creates/truncates a file and
// calls WriteParquet.
func (df DataFrame) WriteParquetFile(path string) error {
	f, err := createFile(path)
	if err != nil {
		return fmt.Errorf("WriteParquetFile: %v", err)
	}
	defer f.Close()
	return df.WriteParquet(f)
}

// ReadParquet reads an Apache Parquet file from r and builds a DataFrame.
// The size parameter must be the total byte size of the Parquet input.
func ReadParquet(r io.ReaderAt, size int64) DataFrame {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return DataFrame{Err: fmt.Errorf("ReadParquet: %v", err)}
	}

	schema := file.Schema()
	reader := parquet.NewGenericReader[map[string]interface{}](io.NewSectionReader(r, 0, size), schema)
	defer reader.Close()

	fields := schema.Fields()
	fieldMap := make(map[string]parquet.Field, len(fields))
	for _, field := range fields {
		fieldMap[field.Name()] = field
	}

	names := make([]string, len(fields))
	types := make(map[string]series.Type, len(fields))
	if metaNames := parquetColumnOrder(file, fieldMap); len(metaNames) == len(fields) {
		copy(names, metaNames)
	} else {
		for i, field := range fields {
			names[i] = field.Name()
		}
	}
	for _, name := range names {
		field := fieldMap[name]
		types[name] = seriesTypeFromParquet(field)
	}

	records := [][]string{names}
	batch := make([]map[string]interface{}, 256)
	for i := range batch {
		batch[i] = make(map[string]interface{}, len(names))
	}

	for {
		n, err := reader.Read(batch)
		if n > 0 {
			for i := 0; i < n; i++ {
				record := make([]string, len(names))
				for j, name := range names {
					record[j] = parquetCellString(batch[i][name])
				}
				records = append(records, record)
				clear(batch[i])
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return DataFrame{Err: fmt.Errorf("ReadParquet: %v", err)}
		}
	}

	return LoadRecords(records, WithTypes(types))
}

// ReadParquetFile is a convenience wrapper that opens a file path and calls
// ReadParquet.
func ReadParquetFile(path string) DataFrame {
	f, err := openFile(path)
	if err != nil {
		return DataFrame{Err: fmt.Errorf("ReadParquetFile: %v", err)}
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return DataFrame{Err: fmt.Errorf("ReadParquetFile: %v", err)}
	}
	return ReadParquet(f, info.Size())
}

func parquetSchemaFromDataFrame(df DataFrame) *parquet.Schema {
	fields := make(parquet.Group, df.Ncol())
	for _, col := range df.columns {
		fields[col.Name] = parquetNodeFromSeriesType(col.Type())
	}
	return parquet.NewSchema("gota", fields)
}

func parquetNodeFromSeriesType(t series.Type) parquet.Node {
	switch t {
	case series.Int:
		return parquet.Int(64)
	case series.Float:
		return parquet.Leaf(parquet.DoubleType)
	case series.Bool:
		return parquet.Leaf(parquet.BooleanType)
	case series.Time:
		return parquet.Timestamp(parquet.Millisecond)
	default:
		return parquet.String()
	}
}

func parquetRowsFromDataFrame(df DataFrame) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, df.Nrow())
	for row := 0; row < df.Nrow(); row++ {
		out := make(map[string]interface{}, df.Ncol())
		for _, col := range df.columns {
			value, err := parquetValueFromElement(col, row)
			if err != nil {
				return nil, err
			}
			out[col.Name] = value
		}
		rows[row] = out
	}
	return rows, nil
}

func parquetValueFromElement(col series.Series, row int) (interface{}, error) {
	elem := col.Elem(row)
	switch col.Type() {
	case series.Int:
		v, err := elem.Int()
		if err != nil {
			return nil, fmt.Errorf("WriteParquet: column %q row %d: %v", col.Name, row, err)
		}
		return int64(v), nil
	case series.Float:
		return elem.Float(), nil
	case series.Bool:
		v, err := elem.Bool()
		if err != nil {
			return nil, fmt.Errorf("WriteParquet: column %q row %d: %v", col.Name, row, err)
		}
		return v, nil
	case series.Time:
		return elem.Val(), nil
	default:
		return elem.String(), nil
	}
}

func seriesTypeFromParquet(field parquet.Field) series.Type {
	if field.Leaf() {
		if logical := field.Type().LogicalType(); logical != nil {
			switch {
			case logical.UTF8 != nil:
				return series.String
			case logical.Timestamp != nil:
				return series.Time
			}
		}
		switch field.Type().Kind() {
		case parquet.Boolean:
			return series.Bool
		case parquet.Int32, parquet.Int64:
			return series.Int
		case parquet.Float, parquet.Double:
			return series.Float
		default:
			return series.String
		}
	}
	return series.String
}

func parquetCellString(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "NaN"
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func parquetColumnOrder(file *parquet.File, fields map[string]parquet.Field) []string {
	for _, kv := range file.Metadata().KeyValueMetadata {
		if kv.Key != "gota.columns" {
			continue
		}
		var names []string
		if err := json.Unmarshal([]byte(kv.Value), &names); err == nil {
			for _, name := range names {
				if fields[name] == nil {
					return nil
				}
			}
			return names
		}
	}
	return nil
}
