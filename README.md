Gota: DataFrames, Series and Data Wrangling for Go
==================================================

`github.com/dreamsxin/gota` — Go 1.24.9+

An embeddable, single-process, in-memory implementation of DataFrames, Series,
and data wrangling methods for Go, inspired by pandas. Gota provides an eager Go
API rather than pandas API parity. The API is still evolving, so review release
notes when upgrading.

**v2 released:** the `v2/` directory carries the columnar kernel module
`github.com/dreamsxin/gota/v2` (tagged `v2/v2.0.0`) — see
**[v2/README.md](v2/README.md)** for the v2 API documentation, the
performance report, and [v2/MIGRATION.md](v2/MIGRATION.md) for the upgrade
guide; design: [v2/docs/rfc-columnar-kernel.md](v2/docs/rfc-columnar-kernel.md).
It is developed per Go's major-version subdirectory layout and is a clean
break from the API documented here: the `Element`/`Elem` surface and the
`Rapply` family are removed in favor of typed Series accessors and
column-wise apply, and the Excel/Parquet/SQL adapters move to the
`v2/excel`, `v2/parquet`, `v2/sql` submodules. This README documents the
v1.x line, which receives fixes only.

## Table of Contents

- [Installation](#installation)
- [DataFrame](#dataframe)
  - [Loading data](#loading-data)
  - [Get row data](#get-row-data)
  - [Subsetting & Slicing](#subsetting--slicing)
  - [Column selection](#column-selection)
  - [Schema](#schema)
  - [Updating values](#updating-values)
  - [Filtering](#filtering)
  - [GroupBy, Aggregation, Apply & Transform](#groupby-aggregation-apply--transform)
  - [Pivot](#pivot)
  - [Arrange](#arrange)
  - [Mutate](#mutate)
  - [Joins](#joins)
  - [Concat](#concat)
  - [Function application](#function-application)
  - [Cumulative statistics](#cumulative-statistics-dataframe)
  - [Diff & PctChange](#diff--pctchange-dataframe)
  - [Rolling & EWM (DataFrame)](#rolling--ewm-dataframe)
  - [FillNA with strategy and limit](#fillna-with-strategy-and-limit)
  - [Correlation & Covariance](#correlation--covariance-dataframe)
  - [Melt (wide → long)](#melt-wide--long)
  - [Excel I/O](#excel-io)
  - [Parquet I/O](#parquet-io)
  - [SQL I/O](#sql-io)
  - [Index & MultiIndex](#index--multiindex)
- [Chaining operations](#chaining-operations)
- [Error handling](#error-handling)
- [Print to console](#print-to-console)
  - [Interfacing with gonum](#interfacing-with-gonum)
  - [Data Exploration](#data-exploration)
  - [Missing Data Handling](#missing-data-handling)
  - [Value Operations](#value-operations)
  - [Pipe](#pipe)
- [Series](#series)
  - [FillNaN](#fillnan)
  - [FillNaN with limit](#fillnan-with-forward--backward-limit)
  - [Rolling Window](#rolling-window)
  - [EWM (Exponentially Weighted Moving)](#ewm-exponentially-weighted-moving)
  - [Cumulative statistics](#cumulative-statistics-series)
  - [Diff & PctChange](#diff--pctchange-series)
  - [Correlation & Covariance](#correlation--covariance-series)
  - [Type Conversion](#type-conversion)
  - [String operations](#string-operations)
  - [Time accessors](#time-accessors)
  - [Categorical](#categorical)
- [Additional DataFrame APIs (v1.2.1)](#additional-dataframe-apis-v121)
  - [Shift](#shift)
  - [Assign](#assign)
  - [Explode](#explode)
  - [Query](#query)
  - [Stack / Unstack](#stack--unstack)
  - [Resample](#resample)
  - [Parallel operations](#parallel-operations)
- [Additional I/O APIs (v1.2.1)](#additional-io-apis-v121)
  - [JSON Lines (NDJSON)](#json-lines-ndjson)
  - [Excel — sheet selection](#excel--sheet-selection)
  - [SQL — named placeholders](#sql--named-placeholders)
  - [CSV streaming](#csv-streaming)
- [License](#license)

---

## Installation

```bash
go get github.com/dreamsxin/gota
```

Requires Go 1.24.9+. Key dependencies:

| Package | Purpose |
|---|---|
| `gonum.org/v1/gonum` | Numeric operations |
| `github.com/xuri/excelize/v2` | Excel I/O (no CGO) |
| `github.com/parquet-go/parquet-go` | Parquet I/O |
| `modernc.org/sqlite` | SQL tests (pure Go SQLite) |
| `github.com/olekukonko/tablewriter` | Table formatting |

Note: the Excel, Parquet, and SQL adapters live in the core `dataframe`
package, so their dependencies appear in `go.mod` even if you only use
in-memory operations or CSV/JSON. Go only compiles what you import, but the
module graph and downloads include all of them; splitting the adapters
requires an API break and is deferred to v2 (see ROADMAP design decisions).

---

DataFrame
---------

A DataFrame is a two-dimensional tabular dataset where columns represent
features and rows represent observations. Columns maintain type integrity
and support NaN (missing) values.

### Loading data

Construct from Series directly:

```go
df := dataframe.New(
    series.New([]string{"b", "a"}, series.String, "COL.1"),
    series.New([]int{1, 2}, series.Int, "COL.2"),
    series.New([]float64{3.0, 4.0}, series.Float, "COL.3"),
)
```

From `[][]string` records:

```go
df := dataframe.LoadRecords(
    [][]string{
        {"A", "B", "C", "D"},
        {"a", "4", "5.1", "true"},
        {"k", "5", "7.0", "true"},
        {"k", "4", "6.0", "true"},
        {"a", "2", "7.1", "false"},
    },
)
```

From a slice of structs:

```go
type User struct {
    Name     string
    Age      int
    Accuracy float64
    ignored  bool // unexported fields are ignored
}
users := []User{
    {"Aram", 17, 0.2, true},
    {"Juan", 18, 0.8, true},
    {"Ana", 22, 0.5, true},
}
df := dataframe.LoadStructs(users)
```

With explicit type configuration:

```go
df := dataframe.LoadRecords(
    records,
    dataframe.DetectTypes(false),
    dataframe.DefaultType(series.Float),
    dataframe.WithTypes(map[string]series.Type{
        "A": series.String,
        "D": series.Bool,
    }),
)
```

From `[]map[string]interface{}`:

```go
df := dataframe.LoadMaps(
    []map[string]interface{}{
        {"A": "a", "B": 1, "C": true, "D": 0},
        {"A": "b", "B": 2, "C": true, "D": 0.5},
    },
)
```

From CSV / JSON readers:

```go
df := dataframe.ReadCSV(strings.NewReader(csvStr))
df := dataframe.ReadJSON(strings.NewReader(jsonStr))

// Infer comma, tab, semicolon or pipe delimiters.
df := dataframe.ReadCSV(strings.NewReader(tsvOrCsv), dataframe.DetectDelimiter(true))
```

From HTML tables:

```go
dfs := dataframe.ReadHTML(r) // returns []DataFrame, one per table
```

### Get row data

```go
row := df.GetRow(0) // map[string]interface{}
```

### Subsetting & Slicing

```go
sub := df.Subset([]int{0, 2})       // rows by index
sub := df.SliceRow(1, 4)            // rows [1, 4) half-open range
```

### Column selection

```go
sel1 := df.Select([]int{0, 2})
sel2 := df.Select([]string{"A", "C"})
dropped := df.Drop([]string{"B"})
```

**`Col` returns a copy.** `df.Col("x")` hands you a detached copy of the
column - modifying it never writes back to the DataFrame, and nothing errors:

```go
col := df.Col("x")
col.Set([]int{0}, series.New([]int{9}, series.Int, "x")) // silently discarded

// To change a column, replace it on the frame instead:
df = df.Mutate(series.New([]int{9, 1}, series.Int, "x"))  // replace by name
df = df.Assign("x2", func(d dataframe.DataFrame) series.Series { ... })
df = df.Set([]int{0}, replacementFrame)                    // cell-level
```

### Schema

`Schema` is the ordered column layout: name, physical type, and nullability.
Use it to check join/concat compatibility or to build conforming empty frames
for output buffers.

```go
sch := df.Schema()

sch.Names()  // []string{"A", "B"}
sch.Types()  // []series.Type{series.String, series.Int}
sch.Field("A")         // (Field, bool)
sch.Equal(otherSchema) // fast layout compatibility check

// Zero-row frame with the same layout (streaming accumulators, output buffers)
empty := dataframe.FromSchema(sch)
```

Every v1.x column is nullable (`Field.Nullable` is always `true`); the field
exists so future non-nullable storage can appear without an API change.

### Updating values

```go
df2 := df.Set(
    []int{0, 2},
    dataframe.LoadRecords(
        [][]string{
            {"A", "B", "C", "D"},
            {"b", "4", "6.0", "true"},
            {"c", "3", "6.0", "false"},
        },
    ),
)
```

### Filtering

```go
// OR filter (default)
fil := df.Filter(
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"B", series.Greater, 4},
)

// Explicit OR
fil := df.FilterAggregation(dataframe.Or,
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"B", series.Greater, 4},
)

// AND filter
fil := df.FilterAggregation(dataframe.And,
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"D", series.Eq, true},
)
```

Built-in comparators: `Eq`, `Neq`, `Greater`, `GreaterEq`, `Less`, `LessEq`, `In`, `Out`.

Custom comparator with `series.CompFunc`:

```go
hasPrefix := func(prefix string) func(series.Element) bool {
    return func(el series.Element) bool {
        if val, ok := el.Val().(string); ok {
            return strings.HasPrefix(val, prefix)
        }
        return false
    }
}
fil := df.Filter(dataframe.F{"A", series.CompFunc, hasPrefix("aa")})
```

### GroupBy, Aggregation, Apply & Transform

```go
groups := df.GroupBy("key1", "key2")
aggre  := groups.Aggregation(
    []AggregationType{Aggregation_MAX, Aggregation_MIN},
    []string{"values", "values2"},
)
```

**Apply** — arbitrary function per group (like `pandas groupby().apply()`):

```go
result := df.GroupBy("category").Apply(func(g dataframe.DataFrame) dataframe.DataFrame {
    return g.Capply(func(s series.Series) series.Series {
        return series.Floats(s.Mean())
    })
})
```

**Transform** — per-group column transform aligned to original row order:

```go
groups := df.GroupBy("category")
transformed, err := groups.Transform("value", func(s series.Series) series.Series {
    mean := s.Mean()
    vals := s.Float()
    out := make([]float64, len(vals))
    for i, v := range vals {
        out[i] = v - mean
    }
    return series.Floats(out...)
})
```

**GetGroups** — access the underlying group map:

```go
groupMap := groups.GetGroups() // map[string]DataFrame
```

### Pivot

```go
pivot := df.Pivot(
    []string{"A", "B"},   // row keys
    []string{"C", "D"},   // column keys
    []PivotValue{
        {Colname: "E", AggregationType: Aggregation_SUM},
        {Colname: "F", AggregationType: Aggregation_COUNT},
    },
)
```

### Arrange

```go
sorted := df.Arrange(
    dataframe.Sort("A"),    // ascending
    dataframe.RevSort("B"), // descending
)
```

### Mutate

```go
// Replace or add a column
mut := df.Mutate(series.New([]string{"a", "b", "c", "d"}, series.String, "C"))
```

### Joins

`InnerJoin`, `LeftJoin`, `RightJoin`, `OuterJoin`, `CrossJoin`:

```go
join := df.InnerJoin(df2, "D")
```

### Concat

Variadic stacking in both directions; a single frame is copied, and sticky
errors propagate:

```go
all := dataframe.Concat(a, b, c)         // vertical; unmatched columns -> NaN
wide := dataframe.ConcatColumns(a, b, c) // horizontal; CBind semantics
```

### Function application

```go
mean := func(s series.Series) series.Series {
    floats := s.Float()
    sum := 0.0
    for _, f := range floats { sum += f }
    return series.Floats(sum / float64(len(floats)))
}
df.Capply(mean) // column-wise
df.Rapply(mean) // row-wise
```

### Cumulative statistics (DataFrame)

```go
cumDF  := df.CumSum()              // running sum, all numeric columns
cumProd := df.CumProd("price", "qty") // selected columns only
```

### Diff & PctChange (DataFrame)

```go
diffDF := df.Diff(1)                        // row[i] - row[i-1]
pct    := df.PctChange(2, "close", "volume") // % change over 2 periods
```

### Rolling & EWM (DataFrame)

DataFrame-level builders mirror the Series API. Without a column list every
numeric column is transformed and other columns pass through unchanged; an
explicit column list must name numeric columns or the call errors
(`ErrTypeMismatch`, `ErrColumnNotFound` via `errors.Is`).

```go
rolled := df.Rolling(3).Mean()               // all numeric columns
rolled := df.Rolling(3).StdDev("close", "volume")

smooth := df.EWM(3).Mean()                   // span convention matches pandas
smooth := df.EWMAlpha(0.5).Mean("close")     // explicit alpha
vol    := df.EWM(3).Std("close")
```

Rolling statistics: `Mean`, `Sum`, `Min`, `Max`, `StdDev`. EWM statistics:
`Mean`, `Var`, `Std`.

### FillNA with strategy and limit

```go
// forward-fill, at most 2 consecutive NaNs
filled := df.FillNAStrategyLimit(dataframe.NAFillForward, 2)

// backward-fill with no limit (0 = unlimited), specific columns
filled := df.FillNAStrategyLimit(dataframe.NAFillBackward, 0, "col1", "col2")
```

Also available: `df.FillNAStrategy(strategy, subset...)` (no limit),
`df.DropNA(how, subset...)` to drop rows with missing values,
`df.DropDuplicates(subset...)` to remove duplicate rows.
`FillNaNStrategy` / `FillNaNStrategyLimit` are spelling-compatible aliases
matching the Series-side `FillNaN` naming.

### Correlation & Covariance (DataFrame)

Returns a square DataFrame whose row/column names match the original numeric columns:

```go
corrMatrix := df.Corr() // Pearson correlation matrix
covMatrix  := df.Cov()  // sample covariance matrix
```

### Melt (wide → long)

```go
long := df.Melt(
    []string{"id", "date"},                    // id columns
    []string{"open", "high", "low", "close"},  // value columns (empty = all others)
    "field",                                   // variable column name
    "value",                                   // value column name
)
```

### Excel I/O

Uses [excelize](https://github.com/xuri/excelize) — no CGO required.

```go
// Read
df := dataframe.ReadXLSX(r)
df := dataframe.ReadXLSXFile("data.xlsx",
    dataframe.HasHeader(true),
    dataframe.WithTypes(map[string]series.Type{"price": series.Float}),
)
sheets, err := dataframe.ReadXLSXSheets(r) // map[string]DataFrame, one per sheet

// Write
err := df.WriteXLSX(w)
err := df.WriteXLSXFile("output.xlsx")
err := df.WriteXLSX(w,
    dataframe.WithXLSXBoldHeader(true),
    dataframe.WithXLSXColumnWidths(map[string]float64{"name": 18}),
    dataframe.WithXLSXNumberFormats(map[string]string{"amount": "#,##0.00"}),
)
```

### Parquet I/O

Uses [parquet-go](https://github.com/parquet-go/parquet-go). Supports Gota
`String`, `Int`, `Float`, `Bool` and `Time` columns. Missing values are stored
as Parquet nulls, and writes are processed in bounded row batches.

```go
err := df.WriteParquet(w)
err := df.WriteParquetFile("data.parquet")

df := dataframe.ReadParquet(readerAt, size)
df := dataframe.ReadParquetFile("data.parquet")
```

### SQL I/O

**FromSQL** — build a DataFrame from `*sql.Rows`:

```go
rows, _ := db.Query("SELECT id, name, score FROM users WHERE active = 1")
df := dataframe.FromSQL(rows)
```

**WriteSQL** — insert into a database table:

```go
err := df.WriteSQL(db, "users",
    dataframe.WithCreateTable(true),   // CREATE TABLE IF NOT EXISTS
    dataframe.WithTruncateFirst(true), // DELETE FROM before inserting
    dataframe.WithBatchSize(200),      // rows per INSERT (default 500)
)

// SQLite / PostgreSQL upsert on unique or primary-key columns.
err := df.WriteSQL(db, "users",
    dataframe.WithUpsert("id"),
    dataframe.WithUpsertUpdateColumns("name", "score"),
)
```

SQL ↔ Series type mapping:

| SQL type | Series type |
|---|---|
| INT / INTEGER / BIGINT … | `series.Int` |
| REAL / FLOAT / DOUBLE … | `series.Float` |
| BOOL / BOOLEAN | `series.Bool` |
| DATE / DATETIME / TIMESTAMP | `series.Time` |
| everything else | `series.String` |

### Index & MultiIndex

**Single-level Index**

```go
idx := dataframe.NewIndex([]string{"a", "b", "c", "d"})
idf, err := df.WithIndex(idx)

rows := idf.Loc("b")           // all rows with label "b"
rows := idf.LocSlice("a", "c") // inclusive label slice

// Use a column as the index (drops that column)
idf, err := df.WithColumnIndex("id")

// Restore to plain DataFrame
plain := idf.ResetIndex("id")
```

**Multi-level Index**

```go
mi, err := dataframe.NewMultiIndex(
    []string{"2024", "2024", "2025", "2025"}, // level 0
    []string{"Q1",   "Q2",   "Q1",   "Q2"},   // level 1
)
midf, err := df.WithMultiIndex(mi)

rows := midf.Loc("2024", "Q1") // full key
rows := midf.Loc("2024")       // partial key (all 2024 rows)
```

`IndexedDataFrame` and `MultiIndexedDataFrame` are lookup wrappers. Operations
such as `Loc` return a regular `DataFrame`; indexes are not automatically
propagated through arbitrary DataFrame transformations in v1.2.1.

### Chaining operations

Most transformation methods return a new DataFrame. `DataFrame.Set`,
`DataFrame.FillNaN`, and `DataFrame.SetNames` mutate shared column storage;
`Series.Set`, `Series.Append`, and `Series.FillNaN` also mutate the original
Series. Call `Copy` before these methods when the original value must remain
unchanged.

`NewNoCopy` shares the element storage of its input Series, not the Series
headers: in-place element writes (`Series.Set`) are visible through the
DataFrame, but `Series.Append` reassigns the slice header and is **not**
visible. The safe rule is to stop touching the Series after handing it to
`NewNoCopy`.

DataFrame-returning operations propagate sticky errors: once an error occurs,
subsequent chain operations become no-ops until the error is inspected:

```go
a = a.Rename("Origin", "Country").
    Filter(dataframe.F{"Age", series.Less, 50}).
    Filter(dataframe.F{"Origin", series.Eq, "United States"}).
    Select([]string{"Id", "Origin", "Date"}).
    Subset([]int{1, 3})
if a.Err != nil {
    log.Fatal(a.Err)
}
```

### Error handling

Common failure kinds wrap exported sentinel errors, so `errors.Is` works
while message text stays stable:

```go
import "errors"

col := df.Col("missing")
if errors.Is(col.Err, dataframe.ErrColumnNotFound) {
    // handle missing column
}

sel := df.Select([]string{"missing"})
if errors.Is(sel.Err, dataframe.ErrColumnNotFound) { /* ... */ }
```

Available sentinels include `ErrEmptyDataFrame`, `ErrColumnNotFound`,
`ErrIndexOutOfRange`, `ErrLengthMismatch`, `ErrKeyNotFound` (join keys and
index labels), `ErrEmptyKeys`, and `ErrInvalidAggregation`.

### Save a DataFrame to file

```go
file, _ := os.Create("output.csv")
defer file.Close()
df.WriteCSV(file)

df.WriteJSON(w)
```

### Print to console

```go
fmt.Println(flights)

> [336776x20] DataFrame
>
>     X0    year  month day   dep_time sched_dep_time dep_delay arr_time ...
>  0: 1     2013  1     1     517      515            2         830      ...
>  ...
```

### Interfacing with gonum

```go
type matrix struct{ dataframe.DataFrame }

func (m matrix) At(i, j int) float64  { return m.Elem(i, j).Float() }
func (m matrix) T() mat.Matrix        { return mat.Transpose{m} }
```

Load a `gonum/mat.Matrix`:

```go
df := dataframe.LoadMatrix(mat)
```

---

### Data Exploration

#### Head & Tail

```go
df.Head(5)   // first 5 rows
df.Tail(10)  // last 10 rows
```

#### Describe

```go
df.Describe() // summary statistics (count, mean, std, min, max, quartiles)
```

#### Info

```go
df.Info(os.Stdout)
// Prints dimensions, column types, non-null counts, memory estimate
```

#### Value Counts

```go
vc := df.ValueCounts("category", false, false) // counts, descending
vc := df.ValueCounts("category", true, false)  // proportions
```

#### Top N

```go
top10   := df.NLargest(10, "revenue")
bottom5 := df.NSmallest(5, "price")
```

#### Random Sampling

```go
sample := df.Sample(100, -1, false, 42)   // 100 rows, fixed seed
sample := df.Sample(-1, 0.1, false, 42)   // 10% of rows
sample := df.Sample(1000, -1, true, 42)   // with replacement
```

---

### Missing Data Handling

```go
mask := df.IsNull()  // or df.IsNA()  — true where value is NaN
mask := df.NotNull() // or df.NotNA() — true where value is present

// Drop rows with any NaN (or all NaN) in subset of columns
df2 := df.DropNA(dataframe.NAHowAny, "col1", "col2")
df2 := df.DropNA(dataframe.NAHowAll) // only drop rows where ALL columns are NaN

// Drop duplicate rows
df2 := df.DropDuplicates("key1", "key2")
```

---

### Value Operations

#### Clip

```go
lower, upper := 0.0, 100.0
df2 := df.Clip(&lower, &upper)                    // all numeric columns
df2 := df.ClipColumn("discount", &lower, &upper)  // single column
```

#### Replace

```go
df2 := df.Replace("N/A", nil)                        // whole DataFrame
df2 := df.ReplaceInColumn("status", "unknown", nil)  // single column
```

#### Astype

```go
df2 := df.Astype(map[string]series.Type{
    "price":  series.Float,
    "qty":    series.Int,
    "active": series.Bool,
})
```

#### Between / IsIn

```go
mask := df.Between("age", 18, 65, "both") // "both"|"neither"|"left"|"right"
mask := df.IsIn("country", []interface{}{"US", "UK", "CA"})
df2  := df.FilterIsIn("country", []interface{}{"US", "UK", "CA"})
```

### Pipe

```go
result := df.
    Filter(dataframe.F{"age", series.Greater, 18}).
    Pipe(customTransform).
    Arrange(dataframe.Sort("name"))

// With extra arguments
result := df.PipeWithArgs(customFunc, arg1, arg2)

// Element-wise map
df2 := df.ApplyMap(func(val interface{}) interface{} {
    if s, ok := val.(string); ok {
        return strings.ToUpper(s)
    }
    return val
})
```

---

Series
------

Series are typed vectors with NaN support. They are the building blocks
for DataFrame columns.

Supported types: `Int`, `Float`, `String`, `Bool`, `Time`

### Usage

```go
s := series.New([]string{"b", "a"}, series.String, "COL.1")

// Convenience constructors
series.Strings(values)
series.Ints(values)
series.Floats(values)
series.Bools(values)
series.Times(values)
```

Core methods: `Len`, `Elem`, `Val`, `Float`, `Int`, `Int64`, `Bool`, `Records`,
`Copy`, `Subset`, `Set`, `Append`, `Concat`, `Slice`, `Map`, `Order`, `Unique`,
`NUnique`, `ValueCounts`, `HasNaN`, `IsNaN`, `FillNaN`, `Compare`, `Empty`.

Statistics: `Mean`, `StdDev`, `Median`, `Min`, `Max`, `MinStr`, `MaxStr`,
`Sum`, `Quantile`.

NaN behaviour:
- `nil` values and the string `"NaN"` are treated as missing
- `Int(math.Inf(...))` → NaN; `Float(math.NaN())` → NaN element
- Comparison operators (`Eq`, `Less`, etc.) always return `false` when either operand is NaN
- `Bool` only accepts `0/1`, `true/false`, `t/f` — other values become NaN

### FillNaN

```go
s := series.New([]interface{}{"a", "b", nil}, series.String, "COL.1")
s.FillNaN(series.Strings("c"))
s.FillNaNForward()   // ffill: propagate last valid value forward
s.FillNaNBackward()  // bfill: propagate next valid value backward
```

### FillNaN with forward / backward limit

```go
s := series.New([]interface{}{1.0, nil, nil, nil, 5.0}, series.Float, "x")

s.FillNaNForwardLimit(1)  // → [1, 1, NaN, NaN, 5]  (fill at most 1 gap)
s.FillNaNBackwardLimit(0) // → [1, 5, 5, 5, 5]       (0 = unlimited)
```

### Rolling Window

```go
s := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "x")

s.Rolling(3).Mean()
s.Rolling(3).MinPeriods(1).Mean() // emit result with at least 1 observation
s.Rolling(3).Sum()
s.Rolling(3).Min()   // O(n) monotonic deque algorithm
s.Rolling(3).Max()   // O(n) monotonic deque algorithm
s.Rolling(3).StdDev() // Bessel-corrected (ddof=1)
s.Rolling(3).Apply(func(w []float64) float64 {
    return w[len(w)-1] - w[0]
})
```

By default `minPeriods` equals the window size — leading positions without a
full window emit NaN. Use `MinPeriods(1)` to emit results as soon as one
observation is available.

### EWM (Exponentially Weighted Moving)

Mirrors the `pandas.ewm()` interface. `alpha = 2 / (span + 1)`.

```go
s := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "price")

s.EWM(3).Mean()               // adjusted mode (pandas default), span=3
s.EWMAlpha(0.5).Mean()        // specify alpha directly (equivalent to span=3)
s.EWM(3).Adjust(false).Mean() // recursive mode: y[i] = α·x[i] + (1-α)·y[i-1]
s.EWM(3).MinPeriods(2).Mean() // NaN until 2 valid observations seen
s.EWM(3).IgnoreNA(true).Mean()
s.EWM(3).Var()  // exponentially weighted variance (ddof=1)
s.EWM(3).Std()  // sqrt of Var
```

| Mode | Formula |
|---|---|
| `Adjust(true)` (default) | `Σ (1-α)^k · x[i-k] / Σ (1-α)^k` |
| `Adjust(false)` | `y[i] = α·x[i] + (1-α)·y[i-1]` |

### Cumulative statistics (Series)

```go
s.CumSum()  // [1, 3, 6, 10, 15]
s.CumProd() // [1, 2, 6, 24, 120]
s.CumMax()  // running maximum
s.CumMin()  // running minimum
```

NaN propagates: once a NaN appears, all subsequent values are also NaN.

### Diff & PctChange (Series)

```go
s := series.New([]float64{10, 12, 15, 11}, series.Float, "close")

s.Diff(1)      // [NaN, 2, 3, -4]
s.Diff(2)      // [NaN, NaN, 5, -1]
s.PctChange(1) // [NaN, 0.20, 0.25, -0.267]
```

`PctChange` divides by `abs(prev)`, returning NaN when the previous value is 0.

### Correlation & Covariance (Series)

NaN pairs are excluded. Returns `NaN` if fewer than 2 valid pairs exist.

```go
x := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "x")
y := series.New([]float64{2, 4, 6, 8, 10}, series.Float, "y")

corr := x.Corr(y) // 1.0  (Pearson)
cov  := x.Cov(y)  // 5.0  (sample covariance, ddof=1)
```

### Type Conversion

Generic batch conversion using Go generics (Go 1.18+). Allocates directly
without pool reuse, so the returned Series owns its memory safely.

```go
// Generic — works with any source slice type
s := series.BatchConvert([]int{1, 2, 3}, series.Float, "values")

// Typed convenience helpers
s := series.BatchConvertInts([]int{1, 2, 3}, series.Float, "values")
s := series.BatchConvertFloats([]float64{1.5, 2.5}, series.String, "values")
s := series.BatchConvertStrings([]string{"1", "2", "invalid"}, series.Int, "values")
s := series.BatchConvertBools([]bool{true, false}, series.Int, "values")
```

Conversion rules:
- Invalid string → NaN (e.g. `"abc"` to Int)
- `int/int64` → `time.Time` via `time.Unix(v, 0)`
- `string` → `time.Time` requires RFC3339 format; others become NaN

### Categorical

`Categorical` is a standalone memory-efficient representation for
low-cardinality string data (country codes, status labels, enum-like columns).
It uses dictionary encoding: a sorted slice of unique strings plus a `[]int32`
code array. In v1.2.1 it is not a native `Series.Type`; use `ToSeries` before
placing it in a DataFrame.

```go
// Create from string slice
cat := series.NewCategorical([]string{"US", "UK", "US", "DE"}, "country")

// Convert from/to regular String Series
cat, err := series.CategoricalFromSeries(s)
s := cat.ToSeries()

// Inspect
cat.Len()          // number of rows
cat.NCategories()  // number of distinct values
cat.Categories()   // sorted dictionary slice
cat.Get(i)         // string value at row i
cat.IsNA(i)        // true if row i is missing

// Frequency counts
counts := cat.ValueCounts() // map[string]int

// Modify
cat.AddCategory("FR")          // extend dictionary
cat.SetValue(0, "FR")          // set row value (must be in dictionary)

// Filter
filtered, err := cat.Filter([]bool{true, false, true, false})

// Memory estimate
bytes := cat.MemoryBytes()
```

### String operations

Transformations return a new String series (NaN stays NaN); predicates return
a Bool series. Calling them on a non-String series sets Err.

```go
s := series.New([]string{"Go", " py ", nil}, series.String, "lang")

s.Upper()               // ["GO", " PY ", NaN]
s.Lower()
s.TrimSpace()           // ["Go", "py", NaN]
s.Trim("G")
s.TrimPrefix("G")
s.TrimSuffix(" ")
s.ReplaceAll("o", "0")  // substring replace; Series.Replace replaces values

s.Contains("o")         // [true, false, NaN]
s.StartsWith("G")
s.EndsWith(" ")
```

### Time accessors

Each accessor returns an Int series from a Time series (NaN stays NaN);
`Weekday` follows `time.Weekday` numbering (Sunday = 0). Calling them on a
non-Time series sets Err.

```go
t := series.New(
    []interface{}{time.Date(2024, 3, 5, 14, 30, 45, 0, time.UTC), nil},
    series.Time, "ts",
)

t.Year()    // [2024, NaN]
t.Month()   // [3, NaN]
t.Day()     // [5, NaN]
t.Hour()    // [14, NaN]
t.Minute()
t.Second()
t.Weekday() // [2, NaN]  (Tuesday)
```

---

### Additional DataFrame APIs (v1.2.1)

#### Shift

```go
df.Shift(1)           // shift all columns down by 1 row (NaN at top)
df.Shift(-2, "price") // shift "price" up by 2 rows (NaN at bottom)
```

#### Assign

```go
df2 := df.Assign("profit", func(d dataframe.DataFrame) series.Series {
    rev := d.Col("revenue").Float()
    cost := d.Col("cost").Float()
    out := make([]float64, len(rev))
    for i := range rev { out[i] = rev[i] - cost[i] }
    return series.Floats(out)
})
```

#### Explode

```go
// "tags" column: "go,python" → two rows
df2 := df.Explode("tags")
```

#### Query

```go
df.Query("age > 18")
df.Query("status == active")
df.Query("age >= 18 AND age <= 65")
df.Query("country in US,UK,CA")
df.Query("score > 0.5 OR label == good")
df.Query("active == true AND (score > 0.5 OR label == good)")
df.Query(`label in "A AND B","x,y"`) // quoted values preserve spaces/commas
```

Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`.
Combine with `AND` / `OR` (case-insensitive). Column names containing
operator substrings (e.g. `income`, `bandwidth`) are handled correctly. `AND`
binds more tightly than `OR`; parentheses override precedence. Single- or
double-quoted values may contain logical words and commas.

#### Stack / Unstack

```go
// wide → long (alias for Melt)
long := df.Stack([]string{"id"}, []string{"q1","q2","q3"}, "quarter", "value")

// long → wide
wide := df.Unstack([]string{"id"}, "quarter", "value")
```

#### Resample

```go
rg := df.Resample("date", dataframe.ResampleMonthly) // D/W/M/Y/H
monthly := rg.Aggregation(
    []dataframe.AggregationType{dataframe.Aggregation_SUM},
    []string{"revenue"},
)
// result has "period" column + aggregated columns
```

#### Parallel operations

```go
df.CapplyParallel(f)                                    // parallel column-wise apply
df.RapplyParallel(f)                                    // parallel row-wise apply
groups.AggregationParallel(typs, colnames)              // parallel GroupBy aggregation
```

---

### Additional I/O APIs (v1.2.1)

#### JSON Lines (NDJSON)

```go
// Read
df := dataframe.ReadNDJSON(r)

// Write (NaN → null)
err := df.WriteNDJSON(w)
```

#### Excel — sheet selection

```go
df := dataframe.ReadXLSXFile("data.xlsx", dataframe.WithSheet("Sheet2"))
```

#### SQL — named placeholders

```go
// PostgreSQL ($1, $2, …)
err := df.WriteSQL(pgDB, "users",
    dataframe.WithPlaceholderStyle(dataframe.SQLPlaceholderDollar))

// SQL Server (@p1, @p2, …)
err := df.WriteSQL(msDB, "users",
    dataframe.WithPlaceholderStyle(dataframe.SQLPlaceholderAt))
```

#### CSV streaming

```go
err := dataframe.ScanCSV(f, 1000, func(batch dataframe.DataFrame) error {
    // process 1000-row batch
    return nil
}, dataframe.DetectDelimiter(true))
```

---

## License

Apache-2.0 — see [LICENSE.md](LICENSE.md)
