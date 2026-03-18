Gota: DataFrames, Series and Data Wrangling for Go
==================================================

This is an implementation of DataFrames, Series and data wrangling
methods for the Go programming language. The API is still in flux so
*use at your own risk*.

## Table of Contents

- [DataFrame](#dataframe)
  - [Loading data](#loading-data)
  - [Get row data](#get-row-data)
  - [Subsetting](#subsetting)
  - [Column selection](#column-selection)
  - [Updating values](#updating-values)
  - [Filtering](#filtering)
  - [GroupBy, Aggregation, Apply & Transform](#groupby--aggregation)
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
  - **[New] Data Exploration](#data-exploration)**
  - **[New] Missing Data Handling](#missing-data-handling)**
  - **[New] Value Operations](#value-operations)**
- [Series](#series)
  - [FillNaN](#fillnan)
  - [FillNaN with limit](#fillnan-with-forward--backward-limit)
  - [Rolling Window](#rolling-window)
  - [EWM (Exponentially Weighted Moving)](#ewm-exponentially-weighted-moving)
  - [Cumulative statistics](#cumulative-statistics-series)
  - [Diff & PctChange](#diff--pctchange-series)
  - [Correlation & Covariance](#correlation--covariance-series)
  - **[New] Type Conversion](#type-conversion)**
- [License](#license)

---

DataFrame
---------

The term DataFrame typically refers to a tabular dataset that can be
viewed as a two dimensional table. Often the columns of this dataset
refers to a list of features, while the rows represent a number of
measurements. As the data on the real world is not perfect, DataFrame
supports non measurements or NaN elements.

Common examples of DataFrames can be found on Excel sheets, CSV files
or SQL database tables, but this data can come on a variety of other
formats, like a collection of JSON objects or XML files.

The utility of DataFrames resides on the ability to subset them, merge
them, summarize the data for individual features or apply functions to
entire rows or columns, all while keeping column type integrity.

### Usage
#### Loading data

DataFrames can be constructed passing Series to the dataframe.New constructor
function:

```go
df := dataframe.New(
	series.New([]string{"b", "a"}, series.String, "COL.1"),
	series.New([]int{1, 2}, series.Int, "COL.2"),
	series.New([]float64{3.0, 4.0}, series.Float, "COL.3"),
)
```

You can also load the data directly from other formats. 
The base loading function takes some records in the
form `[][]string` and returns a new DataFrame from there:

```go
df := dataframe.LoadRecords(
    [][]string{
        []string{"A", "B", "C", "D"},
        []string{"a", "4", "5.1", "true"},
        []string{"k", "5", "7.0", "true"},
        []string{"k", "4", "6.0", "true"},
        []string{"a", "2", "7.1", "false"},
    },
)
```

Now you can also create DataFrames by loading an slice of arbitrary structs:

```go
type User struct {
	Name     string
	Age      int
	Accuracy float64
    ignored  bool // ignored since unexported
}
users := []User{
	{"Aram", 17, 0.2, true},
	{"Juan", 18, 0.8, true},
	{"Ana", 22, 0.5, true},
}
df := dataframe.LoadStructs(users)
```

By default, the column types will be auto detected but this can be
configured. For example, if we wish the default type to be `Float` but
columns `A` and `D` are `String` and `Bool` respectively:

```go
df := dataframe.LoadRecords(
    [][]string{
        []string{"A", "B", "C", "D"},
        []string{"a", "4", "5.1", "true"},
        []string{"k", "5", "7.0", "true"},
        []string{"k", "4", "6.0", "true"},
        []string{"a", "2", "7.1", "false"},
    },
    dataframe.DetectTypes(false),
    dataframe.DefaultType(series.Float),
    dataframe.WithTypes(map[string]series.Type{
        "A": series.String,
        "D": series.Bool,
    }),
)
```

Similarly, you can load the data stored on a `[]map[string]interface{}`:

```go
df := dataframe.LoadMaps(
    []map[string]interface{}{
        map[string]interface{}{
            "A": "a",
            "B": 1,
            "C": true,
            "D": 0,
        },
        map[string]interface{}{
            "A": "b",
            "B": 2,
            "C": true,
            "D": 0.5,
        },
    },
)
```

You can also pass an `io.Reader` to the functions `ReadCSV`/`ReadJSON`
and it will work as expected given that the data is correct:

```go
csvStr := `
Country,Date,Age,Amount,Id
"United States",2012-02-01,50,112.1,01234
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,17,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,NA,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United States",2012-02-01,32,321.31,54320
Spain,2012-02-01,66,555.42,00241
`
df := dataframe.ReadCSV(strings.NewReader(csvStr))
```

```go
jsonStr := `[{"COL.2":1,"COL.3":3},{"COL.1":5,"COL.2":2,"COL.3":2},{"COL.1":6,"COL.2":3,"COL.3":1}]`
df := dataframe.ReadJSON(strings.NewReader(jsonStr))
```

#### Get row data


```go
row := df.GetRow(0)
```

#### Subsetting

We can subset our DataFrames with the Subset method. For example if we
want the first and third rows we can do the following:

```go
sub := df.Subset([]int{0, 2})
```

#### Column selection

If instead of subsetting the rows we want to select specific columns,
by an index or column name:

```go
sel1 := df.Select([]int{0, 2})
sel2 := df.Select([]string{"A", "C"})
```

#### Updating values

In order to update the values of a DataFrame we can use the Set
method:

```go
df2 := df.Set(
    []int{0, 2},
    dataframe.LoadRecords(
        [][]string{
            []string{"A", "B", "C", "D"},
            []string{"b", "4", "6.0", "true"},
            []string{"c", "3", "6.0", "false"},
        },
    ),
)
```

#### Filtering

For more complex row subsetting we can use the Filter method. For
example, if we want the rows where the column "A" is equal to "a" or
column "B" is greater than 4:

```go
fil := df.Filter(
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"B", series.Greater, 4},
)

filAlt := df.FilterAggregation(
    dataframe.Or,
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"B", series.Greater, 4},
) 
```

Filters inside Filter are combined as OR operations, alternatively we can use `df.FilterAggragation` with `dataframe.Or`.

If we want to combine filters with AND operations, we can use `df.FilterAggregation` with `dataframe.And`.

```go
fil := df.FilterAggregation(
    dataframe.And, 
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"D", series.Eq, true},
)
```

To combine AND and OR operations, we can use chaining of filters.

```go
// combine filters with OR
fil := df.Filter(
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"B", series.Greater, 4},
)
// apply AND for fil and fil2
fil2 := fil.Filter(
    dataframe.F{"D", series.Eq, true},
)
```

Filtering is based on predefined comparison operators: 
* `series.Eq`
* `series.Neq`
* `series.Greater`
* `series.GreaterEq`
* `series.Less`
* `series.LessEq`
* `series.In`
* `series.Out`

However, if these filter operations are not sufficient, we can use user-defined comparators.
We use `series.CompFunc` and a user-defined function with the signature `func(series.Element) bool` to provide user-defined filters to `df.Filter` and `df.FilterAggregation`.

```go
hasPrefix := func(prefix string) func(el series.Element) bool {
        return func (el series.Element) bool {
            if el.Type() == String {
                if val, ok := el.Val().(string); ok {
                    return strings.HasPrefix(val, prefix)
                }
            }
            return false
        }
    }

fil := df.Filter(
    dataframe.F{"A", series.CompFunc, hasPrefix("aa")},
)
```

This example filters rows based on whether they have a cell value starting with `"aa"` in column `"A"`.

#### GroupBy & Aggregation

GroupBy groups rows by one or more key columns and supports aggregation,
custom apply functions, and column-wise transforms.

```go
groups := df.GroupBy("key1", "key2") // group by "key1" and "key2"
aggre  := groups.Aggregation(
    []AggregationType{Aggregation_MAX, Aggregation_MIN},
    []string{"values", "values2"}, // max of "values", min of "values2"
)
```

**GroupBy.Apply** — apply an arbitrary function to each group and
concatenate the results, similar to `pandas groupby().apply()`:

```go
result := df.GroupBy("category").Apply(func(g dataframe.DataFrame) dataframe.DataFrame {
    // return a transformed or summarised DataFrame for this group
    return g.Capply(func(s series.Series) series.Series {
        return series.Floats(s.Mean())
    })
})
```

**GroupBy.Transform** — apply a function to a single column within each
group and return the results aligned to the original row order, similar
to `pandas groupby().transform()`:

```go
groups := df.GroupBy("category")
transformed, err := groups.Transform("value", func(s series.Series) series.Series {
    mean := s.Mean()
    // subtract group mean (group-wise de-mean)
    vals := s.Float()
    out := make([]float64, len(vals))
    for i, v := range vals {
        out[i] = v - mean
    }
    return series.Floats(out...)
})
```

#### Pivot

```go
pivot := df.Pivot(
    []string{"A", "B"}, // rows
    []string{"C", "D"}, // columns
    []PivotValue{       // values
      {Colname: "E", AggregationType: Aggregation_SUM},
      {Colname: "F", AggregationType: Aggregation_COUNT},
})
```

#### Arrange

With Arrange a DataFrame can be sorted by the given column names:

```go
sorted := df.Arrange(
    dataframe.Sort("A"),    // Sort in ascending order
    dataframe.RevSort("B"), // Sort in descending order
)
```

#### Mutate

If we want to modify a column or add one based on a given Series at
the end we can use the Mutate method:

```go
// Change column C with a new one
mut := df.Mutate(
    series.New([]string{"a", "b", "c", "d"}, series.String, "C"),
)
// Add a new column E
mut2 := df.Mutate(
    series.New([]string{"a", "b", "c", "d"}, series.String, "E"),
)
```

#### Joins

Different Join operations are supported (`InnerJoin`, `LeftJoin`,
`RightJoin`, `CrossJoin`). In order to use these methods you have to
specify which are the keys to be used for joining the DataFrames:

```go
df := dataframe.LoadRecords(
    [][]string{
        []string{"A", "B", "C", "D"},
        []string{"a", "4", "5.1", "true"},
        []string{"k", "5", "7.0", "true"},
        []string{"k", "4", "6.0", "true"},
        []string{"a", "2", "7.1", "false"},
    },
)
df2 := dataframe.LoadRecords(
    [][]string{
        []string{"A", "F", "D"},
        []string{"1", "1", "true"},
        []string{"4", "2", "false"},
        []string{"2", "8", "false"},
        []string{"5", "9", "false"},
    },
)
join := df.InnerJoin(df2, "D")
```

#### Function application

Functions can be applied to the rows or columns of a DataFrame,
casting the types as necessary:

```go
mean := func(s series.Series) series.Series {
    floats := s.Float()
    sum := 0.0
    for _, f := range floats {
        sum += f
    }
    return series.Floats(sum / float64(len(floats)))
}
df.Capply(mean)
df.Rapply(mean)
```

#### Cumulative statistics (DataFrame)

`CumSum`, `CumProd` compute running totals on all numeric columns (or
a named subset).  Non-numeric columns are passed through unchanged.

```go
// cumulative sum of all numeric columns
cumDF := df.CumSum()

// cumulative product of selected columns only
cumProd := df.CumProd("price", "qty")
```

#### Diff & PctChange (DataFrame)

```go
// first-order difference (row[i] - row[i-1]) for all numeric columns
diffDF := df.Diff(1)

// percentage change over 2 periods for selected columns
pct := df.PctChange(2, "close", "volume")
```

#### FillNA with strategy and limit

Fill NaN values using forward-fill or backward-fill with an optional
maximum fill count per gap:

```go
// forward-fill, fill at most 2 consecutive NaNs
filled := df.FillNAStrategyLimit(dataframe.FILLNA_FORWARD, 2)

// backward-fill with no limit (0 = unlimited)
filled := df.FillNAStrategyLimit(dataframe.FILLNA_BACKWARD, 0, "col1", "col2")
```

#### Correlation & Covariance (DataFrame)

`Corr` returns the Pearson correlation matrix; `Cov` returns the
sample covariance matrix. Both operate on all numeric columns and
return a square DataFrame whose row/column names are the original
column names.

```go
corrMatrix := df.Corr()
covMatrix  := df.Cov()
```

#### Melt (wide → long)

`Melt` unpivots a DataFrame from wide format to long format, similar
to `pandas.melt()`.

```go
// idVars: columns to keep as identifiers
// valueVars: columns to unpivot (empty = all other columns)
// varName: name for the new "variable" column
// valueName: name for the new "value" column
long := df.Melt([]string{"id", "date"}, []string{"open", "high", "low", "close"}, "field", "value")
```

#### Excel I/O

Read from or write to XLSX files using the `ReadXLSX`/`WriteXLSX`
family of functions. The implementation uses
[excelize](https://github.com/xuri/excelize) under the hood and
requires **no CGO**.

```go
// Read from an io.Reader (first sheet, first row = header)
f, _ := os.Open("data.xlsx")
df := dataframe.ReadXLSX(f)

// Convenience file-path wrapper
df := dataframe.ReadXLSXFile("data.xlsx")

// Write to an io.Writer
out, _ := os.Create("output.xlsx")
err := df.WriteXLSX(out)

// Convenience file-path wrapper
err := df.WriteXLSXFile("output.xlsx")
```

Load options (same as `LoadRecords`) can be passed to `ReadXLSX`:

```go
df := dataframe.ReadXLSXFile("data.xlsx",
    dataframe.HasHeader(true),
    dataframe.WithTypes(map[string]series.Type{"price": series.Float}),
)
```

#### SQL I/O

**FromSQL** — build a DataFrame from a `*sql.Rows` result set.
Column types are inferred from the SQL metadata; `NULL` values become
`NaN`.

```go
rows, err := db.Query("SELECT id, name, score FROM users WHERE active = 1")
if err != nil { log.Fatal(err) }
df := dataframe.FromSQL(rows)
```

**WriteSQL** — insert a DataFrame into a database table using batched
`INSERT` statements. Supports automatic table creation and truncation.

```go
err := df.WriteSQL(db, "users",
    dataframe.WithCreateTable(true),   // CREATE TABLE IF NOT EXISTS
    dataframe.WithTruncateFirst(true), // DELETE FROM users before inserting
    dataframe.WithBatchSize(200),      // rows per INSERT statement (default 500)
)
```

SQL type mapping:

| SQL type | Series type |
|---|---|
| INT / INTEGER / BIGINT … | `series.Int` |
| REAL / FLOAT / DOUBLE … | `series.Float` |
| BOOL / BOOLEAN | `series.Bool` |
| DATE / DATETIME / TIMESTAMP | `series.Time` |
| everything else | `series.String` |

#### Index & MultiIndex

Gota provides a lightweight label-based row index system analogous to
`pandas.Index` / `pandas.MultiIndex`.

**Single-level Index**

```go
// Attach an explicit index to a DataFrame
idx := dataframe.NewIndex([]string{"a", "b", "c", "d"})
idf, err := df.WithIndex(idx)

// Label-based row lookup (all rows with this label)
rows := idf.Loc("b")

// Inclusive label slice
rows := idf.LocSlice("a", "c") // rows for labels a, b, c

// Convert a column into the row index (drops that column from the frame)
idf, err := df.WithColumnIndex("id")

// Restore to a regular DataFrame (index labels become a new column)
plain := idf.ResetIndex("id")
```

**Multi-level Index**

```go
// Build a two-level MultiIndex
mi, err := dataframe.NewMultiIndex(
    []string{"2024", "2024", "2025", "2025"}, // level 0: year
    []string{"Q1",   "Q2",   "Q1",   "Q2"},   // level 1: quarter
)

midf, err := df.WithMultiIndex(mi)

// Full key lookup
rows := midf.Loc("2024", "Q1")

// Partial key lookup (all rows in 2024)
rows := midf.Loc("2024")
```

#### Chaining operations

DataFrames support a number of methods for wrangling the data,
filtering, subsetting, selecting columns, adding new columns or
modifying existing ones. All these methods can be chained one after
another and at the end of the procedure check if there has been any
errors by the DataFrame Err field. If any of the methods in the chain
returns an error, the remaining operations on the chain will become
a no-op.

```go
a = a.Rename("Origin", "Country").
    Filter(dataframe.F{"Age", "<", 50}).
    Filter(dataframe.F{"Origin", "==", "United States"}).
    Select("Id", "Origin", "Date").
    Subset([]int{1, 3})
if a.Err != nil {
    log.Fatal("Oh noes!")
}
```

#### Save a dataframe to file

With `WriteCSV` you can write a dataframe to a CSV file.

```go
file, err := os.Create("output.csv")
defer file.Close()
if err != nil {
    log.Fatal(err)
}
df.WriteCSV(file)
```

#### Print to console

```go
fmt.Println(flights)

> [336776x20] DataFrame
> 
>     X0    year  month day   dep_time sched_dep_time dep_delay arr_time ...
>  0: 1     2013  1     1     517      515            2         830      ...
>  1: 2     2013  1     1     533      529            4         850      ...
>  2: 3     2013  1     1     542      540            2         923      ...
>  3: 4     2013  1     1     544      545            -1        1004     ...
>  4: 5     2013  1     1     554      600            -6        812      ...
>  5: 6     2013  1     1     554      558            -4        740      ...
>  6: 7     2013  1     1     555      600            -5        913      ...
>  7: 8     2013  1     1     557      600            -3        709      ...
>  8: 9     2013  1     1     557      600            -3        838      ...
>  9: 10    2013  1     1     558      600            -2        753      ...
>     ...   ...   ...   ...   ...      ...            ...       ...      ...
>     <int> <int> <int> <int> <int>    <int>          <int>     <int>    ...
> 
> Not Showing: sched_arr_time <int>, arr_delay <int>, carrier <string>, flight <int>,
> tailnum <string>, origin <string>, dest <string>, air_time <int>, distance <int>, hour <int>,
> minute <int>, time_hour <string>
```

#### Interfacing with gonum

A `gonum/mat.Matrix` or any object that implements the `dataframe.Matrix`
interface can be loaded as a `DataFrame` by using the `LoadMatrix()` method. If
one wants to convert a `DataFrame` to a `mat.Matrix` it is necessary to create
the necessary structs and method implementations. Since a `DataFrame` already
implements the `Dims() (r, c int)` method, only implementations for the `At` and
`T` methods are necessary:

```go
type matrix struct {
	dataframe.DataFrame
}

func (m matrix) At(i, j int) float64 {
	return m.Elem(i, j).Float()
}

func (m matrix) T() mat.Matrix {
	return mat.Transpose{m}
}
```

---

### Data Exploration

#### Head & Tail - View First/Last Rows

Get the first or last n rows of a DataFrame, similar to pandas `head()` and `tail()`:

```go
// First 5 rows
df.Head(5)

// Last 10 rows
df.Tail(10)

// Chain with other operations
df.Head(100).Select([]string{"name", "age", "salary"})
```

#### Info - DataFrame Summary

Print a concise summary including dimensions, column types, and memory usage:

```go
import "os"

df.Info(os.Stdout)
```

Output:
```
<class 'dataframe.DataFrame'>
Index: 1000 entries, 0 to 999
Data columns (total 5 columns):
   Name      1000 non-null   string
   Age       950 non-null    int
   Salary    900 non-null    float
   City      980 non-null    string
   Active    1000 non-null   bool
memory usage: 32.0+ KB
```

#### Value Counts - Frequency Analysis

Count unique values in a column:

```go
// Count frequencies
vc := df.ValueCounts("category", false, false)
// Returns DataFrame with columns: category, count

// Get proportions instead of counts
vc := df.ValueCounts("category", true, false)

// Sort ascending
vc := df.ValueCounts("category", false, true)
```

#### Top N Selection

Find rows with largest or smallest values:

```go
// Top 10 by revenue
top10 := df.NLargest(10, "revenue")

// Bottom 5 by price
bottom5 := df.NSmallest(5, "price")
```

#### Random Sampling

Sample rows randomly with or without replacement:

```go
// Sample 100 rows (fixed seed for reproducibility)
sample := df.Sample(100, -1, false, 42)

// Sample 10% of rows
sample := df.Sample(-1, 0.1, false, 42)

// Sample with replacement
sample := df.Sample(1000, -1, true, 42)
```

---

### Missing Data Handling

#### Detect Missing Values

Check for NaN/null values:

```go
// Boolean mask for missing values
mask := df.IsNull()  // or df.IsNA()

// Boolean mask for non-missing values
mask := df.NotNull() // or df.NotNA()

// Count missing values per column
missing := df.IsNull()
for i, col := range missing.Names() {
    count := 0
    for j := 0; j < missing.Nrow(); j++ {
        if val, _ := missing.Elem(j, i).Bool(); val {
            count++
        }
    }
    fmt.Printf("%s: %d missing\n", col, count)
}
```

#### Filter by Missing Values

```go
// Rows where age is missing
dfMissing := df.Subset(df.IsNull().Col("age"))

// Rows where age is NOT missing
dfComplete := df.Subset(df.NotNull().Col("age"))
```

---

### Value Operations

#### Clip Values to Range

Restrict values to a specified range:

```go
// Clip all numeric columns to [0, 100]
lower := 0.0
upper := 100.0
df2 := df.Clip(&lower, &upper)

// Clip specific column
df2 := df.ClipColumn("discount", &lower, &upper)
```

#### Replace Values

Replace specific values throughout the DataFrame:

```go
// Replace "N/A" strings with NaN
df2 := df.Replace("N/A", nil)

// Replace in specific column
df2 := df.ReplaceInColumn("status", "unknown", nil)

// Chain multiple replacements
df2 := df.Replace("NA", nil).
          Replace("", nil).
          Replace("null", nil)
```

#### Type Conversion

Convert column types efficiently:

```go
df2 := df.Astype(map[string]series.Type{
    "price":  series.Float,
    "qty":    series.Int,
    "date":   series.Time,
    "active": series.Bool,
})
```

#### Condition Checks

Check if values are in a range or set:

```go
// Check if values are between 18 and 65
mask := df.Between("age", 18, 65, "both")
// inclusive options: "both", "neither", "left", "right"

// Check if values are in a set
mask := df.IsIn("country", []interface{}{"US", "UK", "CA"})

// Filter using IsIn
df2 := df.FilterIsIn("country", []interface{}{"US", "UK", "CA"})
```

---

### Complete Example: Data Cleaning Pipeline

Here's a complete example showing how to use the new methods together:

```go
package main

import (
    "fmt"
    "os"

    "github.com/dreamsxin/gota/dataframe"
    "github.com/dreamsxin/gota/series"
)

func main() {
    // Load data
    df := dataframe.ReadCSV("sales_data.csv")
    
    // 1. Explore the data
    fmt.Println("=== Data Summary ===")
    df.Info(os.Stdout)
    
    fmt.Println("\n=== First 5 Rows ===")
    fmt.Println(df.Head(5))
    
    // 2. Check for missing values
    fmt.Println("\n=== Missing Values ===")
    missing := df.IsNull()
    for i, col := range df.Names() {
        count := 0
        for j := 0; j < missing.Nrow(); j++ {
            if val, _ := missing.Elem(j, i).Bool(); val {
                count++
            }
        }
        if count > 0 {
            fmt.Printf("%s: %d missing (%.1f%%)\n", col, count, 
                float64(count)/float64(df.Nrow())*100)
        }
    }
    
    // 3. Clean the data
    fmt.Println("\n=== Cleaning Data ===")
    
    // Replace "N/A" and empty strings with NaN
    df = df.Replace("N/A", nil).
           Replace("", nil)
    
    // Convert types
    df = df.Astype(map[string]series.Type{
        "revenue": series.Float,
        "qty":     series.Int,
        "region":  series.String,
    })
    
    // Clip outliers
    zero := 0.0
    maxDiscount := 1.0
    df = df.ClipColumn("discount", &zero, &maxDiscount)
    
    // 4. Filter data
    fmt.Println("\n=== Filtering ===")
    
    // Keep only complete cases for key columns
    df = df.Subset(df.NotNull().Col("revenue"))
    
    // Filter specific regions
    df = df.FilterIsIn("region", []interface{}{"North", "South", "East", "West"})
    
    // 5. Analyze
    fmt.Println("\n=== Top 10 by Revenue ===")
    fmt.Println(df.NLargest(10, "revenue"))
    
    fmt.Println("\n=== Region Distribution ===")
    fmt.Println(df.ValueCounts("region", false, false))
    
    // 6. Sample for quick analysis
    fmt.Println("\n=== Random Sample (10 rows) ===")
    sample := df.Sample(10, -1, false, 42)
    fmt.Println(sample)
    
    // 7. Save cleaned data
    fmt.Println("\n=== Saving Cleaned Data ===")
    file, _ := os.Create("sales_data_cleaned.csv")
    defer file.Close()
    df.WriteCSV(file)
    fmt.Println("Saved to sales_data_cleaned.csv")
}
```

---

---

Series
------

Series are essentially vectors of elements of the same type with
support for missing values. Series are the building blocks for
DataFrame columns.

Four types are currently supported:

```go
Int
Float
String
Bool
```

### Usage

#### FillNaN

```go
s := series.New([]interface{}{"a", "b", nil}, series.String, "COL.1")
s.FillNaN(series.Strings("c"))
```

#### FillNaN with forward / backward limit

Forward-fill and backward-fill support an optional `limit` parameter
that caps how many consecutive NaN values are filled:

```go
s := series.New([]interface{}{1.0, nil, nil, nil, 5.0}, series.Float, "x")

// fill at most 1 NaN gap forward
s.FillNaNForwardLimit(1)   // → [1, 1, NaN, NaN, 5]

// fill all NaN gaps backward (limit 0 = unlimited)
s.FillNaNBackwardLimit(0)  // → [1, 5, 5, 5, 5]
```

#### Rolling Window

```go
s := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "x")

// 3-period rolling mean; NaN for the first 2 positions
s.Rolling(3).Mean()

// Allow results where at least 1 observation is present
s.Rolling(3).MinPeriods(1).Mean()

// Other aggregations: Sum, Min, Max, StdDev, Apply
s.Rolling(3).Sum()
s.Rolling(3).StdDev()
s.Rolling(3).Apply(func(w []float64) float64 {
    // custom aggregation
    return w[len(w)-1] - w[0]
})
```

#### EWM (Exponentially Weighted Moving)

EWM mirrors the `pandas.ewm()` interface. The most common entry point
is `series.EWM(span)` where `alpha = 2 / (span + 1)`.

```go
s := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "price")

// Exponentially weighted moving average (adjusted mode, pandas-compatible)
s.EWM(3).Mean()

// Use alpha directly instead of span
s.EWMAlpha(0.5).Mean()

// Non-adjusted (recursive) mode
s.EWM(3).Adjust(false).Mean()

// Require at least 2 observations before emitting a result
s.EWM(3).MinPeriods(2).Mean()

// Variance and standard deviation
s.EWM(3).Var()
s.EWM(3).Std()
```

The `adjust` parameter (default `true`) determines the weighting scheme:

| Mode | Formula for position i |
|---|---|
| `Adjust(true)` | `Σ (1-α)^k · x[i-k] / Σ (1-α)^k` (pandas default) |
| `Adjust(false)` | `y[i] = α·x[i] + (1-α)·y[i-1]` (recursive) |

#### Cumulative statistics (Series)

```go
s := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "x")

s.CumSum()  // [1, 3, 6, 10, 15]
s.CumProd() // [1, 2, 6, 24, 120]
s.CumMax()  // [1, 2, 3, 4, 5]
s.CumMin()  // [1, 1, 1, 1, 1]
```

NaN values propagate: once a NaN appears in the input the corresponding
output element and all subsequent elements will also be NaN.

#### Diff & PctChange (Series)

```go
s := series.New([]float64{10, 12, 15, 11}, series.Float, "close")

// First-order difference: [NaN, 2, 3, -4]
s.Diff(1)

// Difference over 2 periods: [NaN, NaN, 5, -1]
s.Diff(2)

// Percentage change: [NaN, 0.20, 0.25, -0.267]
s.PctChange(1)
```

#### Correlation & Covariance (Series)

Compute Pearson correlation coefficient or sample covariance between
two Series.  Pairs where either element is NaN are excluded.  Returns
`NaN` if fewer than 2 valid pairs exist.

```go
x := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "x")
y := series.New([]float64{2, 4, 6, 8, 10}, series.Float, "y")

corr := x.Corr(y) // 1.0  (perfect positive correlation)
cov  := x.Cov(y)  // 5.0  (sample covariance)
```

#### Type Conversion

Efficiently convert Series types using batch operations:

```go
// Convert []int to Float Series (optimized)
ints := []int{1, 2, 3, 4, 5}
s := series.BatchConvertInts(ints, series.Float, "values")

// Convert []float64 to String Series
floats := []float64{1.5, 2.5, 3.5}
s := series.BatchConvertFloats(floats, series.String, "values")

// Convert []string to Int Series (handles invalid values as NaN)
strings := []string{"1", "2", "3", "invalid"}
s := series.BatchConvertStrings(strings, series.Int, "values")

// Generic version for any type
data := getData() // []T
s := series.BatchConvert(data, series.Float, "column")
```

**Performance tip**: `BatchConvert` methods are significantly faster than
using `series.New()` for type conversion, especially for numeric types.
For 100,000 elements, `BatchConvertInts` is **11x faster** with **99.99% less memory**.

---

For more information about the API, make sure to check:

- [dataframe godoc][3]
- [series godoc][4]

License
-------
Copyright 2016 Alejandro Sanchez Brotons

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You may
obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

[1]: https://github.com/gonum
[2]: https://github.com/dreamsxin/gota
[3]: https://godoc.org/github.com/dreamsxin/gota/dataframe
[4]: https://godoc.org/github.com/dreamsxin/gota/series
