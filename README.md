Gota: DataFrames, Series and Data Wrangling for Go
==================================================

`github.com/dreamsxin/gota` — Go 1.24+

A comprehensive implementation of DataFrames, Series and data wrangling
methods for Go, inspired by pandas. The API is still evolving so
*use at your own risk*.

## Table of Contents

- [Installation](#installation)
- [DataFrame](#dataframe)
  - [Loading data](#loading-data)
  - [Get row data](#get-row-data)
  - [Subsetting & Slicing](#subsetting--slicing)
  - [Column selection](#column-selection)
  - [Updating values](#updating-values)
  - [Filtering](#filtering)
  - [GroupBy, Aggregation, Apply & Transform](#groupby-aggregation-apply--transform)
  - [Pivot](#pivot)
  - [Arrange](#arrange)
  - [Mutate](#mutate)
  - [Joins](#joins)
  - [Function application](#function-application)
  - [Cumulative statistics](#cumulative-statistics-dataframe)
  - [Diff & PctChange](#diff--pctchange-dataframe)
  - [FillNA with strategy and limit](#fillna-with-strategy-and-limit)
  - [Correlation & Covariance](#correlation--covariance-dataframe)
  - [Melt (wide → long)](#melt-wide--long)
  - [Excel I/O](#excel-io)
  - [SQL I/O](#sql-io)
  - [Index & MultiIndex](#index--multiindex)
  - [Chaining operations](#chaining-operations)
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
  - [Categorical](#categorical)
- [New DataFrame APIs (v1.3+)](#new-dataframe-apis-v13)
  - [Shift](#shift)
  - [Assign](#assign)
  - [Explode](#explode)
  - [Query](#query)
  - [Stack / Unstack](#stack--unstack)
  - [Resample](#resample)
  - [Parallel operations](#parallel-operations)
- [New I/O APIs (v1.5+)](#new-io-apis-v15)
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

Requires Go 1.24+. Key dependencies:

| Package | Purpose |
|---|---|
| `gonum.org/v1/gonum` | Numeric operations |
| `github.com/xuri/excelize/v2` | Excel I/O (no CGO) |
| `modernc.org/sqlite` | SQL tests (pure Go SQLite) |
| `github.com/olekukonko/tablewriter` | Table formatting |

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

// Write
err := df.WriteXLSX(w)
err := df.WriteXLSXFile("output.xlsx")
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

### Chaining operations

All methods return a new DataFrame and propagate errors — once an error
occurs, subsequent operations become no-ops:

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
```

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

`Categorical` is a memory-efficient column type for low-cardinality string data
(country codes, status labels, enum-like columns). It uses dictionary encoding:
a sorted slice of unique strings plus a `[]int32` code array.

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

---

### New DataFrame APIs (v1.3+)

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
```

Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`.
Combine with `AND` / `OR` (case-insensitive). Column names containing
operator substrings (e.g. `income`, `bandwidth`) are handled correctly.

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

### New I/O APIs (v1.5+)

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
})
```

---

## License

MIT — see [LICENSE.md](LICENSE.md)
