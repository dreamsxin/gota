package dataframe

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/dreamsxin/gota/series"
)

// ReadNDJSON reads a newline-delimited JSON (NDJSON / JSON Lines) stream.
// Each line must be a JSON object; keys become column names.
// Empty lines and lines starting with '#' are skipped.
//
// Example:
//
//	f, _ := os.Open("data.ndjson")
//	df := dataframe.ReadNDJSON(f)
func ReadNDJSON(r io.Reader, options ...LoadOption) DataFrame {
	scanner := bufio.NewScanner(r)
	var records []map[string]interface{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			return DataFrame{Err: fmt.Errorf("ReadNDJSON: %v", err)}
		}
		records = append(records, obj)
	}
	if err := scanner.Err(); err != nil {
		return DataFrame{Err: fmt.Errorf("ReadNDJSON: %v", err)}
	}
	if len(records) == 0 {
		return DataFrame{}
	}
	return LoadMaps(records, options...)
}

// WriteNDJSON writes the DataFrame as newline-delimited JSON to w.
// Each row becomes one JSON object on its own line.
// NaN values are written as JSON null.
//
// Example:
//
//	f, _ := os.Create("out.ndjson")
//	err := df.WriteNDJSON(f)
func (df DataFrame) WriteNDJSON(w io.Writer) error {
	if df.Err != nil {
		return df.Err
	}
	names := df.Names()
	types := df.Types()
	enc := json.NewEncoder(w)
	for i := 0; i < df.nrows; i++ {
		obj := make(map[string]interface{}, df.ncols)
		for j, name := range names {
			elem := df.columns[j].Elem(i)
			if elem.IsNA() {
				obj[name] = nil
				continue
			}
			switch types[j] {
			case series.Int:
				v, _ := elem.Int()
				obj[name] = v
			case series.Float:
				obj[name] = elem.Float()
			case series.Bool:
				v, _ := elem.Bool()
				obj[name] = v
			default:
				obj[name] = elem.String()
			}
		}
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("WriteNDJSON row %d: %v", i, err)
		}
	}
	return nil
}
