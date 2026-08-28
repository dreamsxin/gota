# Migration Guide: v1.x → v2.0.0

v2.0.0 is the columnar kernel release. It is a clean break: the module path,
the storage model, and parts of the API change. This guide lists every
breaking change and its replacement. See `CHANGELOG.md` for the full
release notes and `docs/rfc-columnar-kernel.md` for the design rationale.

## 1. Module path

```go
// v1.x
import (
    "github.com/dreamsxin/gota/dataframe"
    "github.com/dreamsxin/gota/series"
)

// v2
import (
    "github.com/dreamsxin/gota/v2/dataframe"
    "github.com/dreamsxin/gota/v2/series"
)
```

In `go.mod`: `require github.com/dreamsxin/gota/v2 v2.0.0`.

## 2. Adapters move to submodules

Excel, Parquet, and SQL I/O leave the core module (which no longer pulls in
excelize, parquet-go, or sqlite). Adapter methods become functions that take
the DataFrame as the first argument:

| v1.x | v2 |
|---|---|
| `dataframe.ReadXLSX(r, dataframe.WithSheet("S"))` | `excel.ReadXLSX(r, excel.WithSheet("S"))` |
| `dataframe.ReadXLSXFile(path, opts...)` | `excel.ReadXLSXFile(path, opts...)` |
| `dataframe.ReadXLSXSheets(r)` | `excel.ReadXLSXSheets(r)` |
| `df.WriteXLSX(w, dataframe.WithSheetName("S"))` | `excel.WriteXLSX(df, w, excel.WithSheetName("S"))` |
| `df.WriteXLSXFile(path)` | `excel.WriteXLSXFile(df, path)` |
| `df.WriteXLSXSheet(f, "S")` | `excel.WriteXLSXSheet(f, "S", df)` |
| `dataframe.WriteXLSXMultiSheet(w, dataframe.SheetData{...})` | `excel.WriteXLSXMultiSheet(w, excel.SheetData{...})` |
| `dataframe.ReadParquet(r, size)` | `parquet.ReadParquet(r, size)` |
| `dataframe.ReadParquetFile(path)` | `parquet.ReadParquetFile(path)` |
| `df.WriteParquet(w)` | `parquet.WriteParquet(df, w)` |
| `df.WriteParquetFile(path)` | `parquet.WriteParquetFile(df, path)` |
| `dataframe.FromSQL(rows)` | `sql.FromSQL(rows)` |
| `df.WriteSQL(db, "t", opts...)` | `sql.WriteSQL(df, db, "t", opts...)` |

Core load options (`HasHeader`, `Names`, `WithTypes`, `NaNValues`, ...) pass
through the excel module via `excel.WithLoadOptions(...)`. Excel style
options are renamed: `WithXLSXBoldHeader` → `excel.WithBoldHeader`,
`WithXLSXColumnWidths` → `excel.WithColumnWidths`, `WithXLSXNumberFormats` →
`excel.WithNumberFormats`. SQL placeholder styles keep their names under the
`sql` package (`sql.SQLPlaceholderDollar`, `sql.WithPlaceholderStyle`, ...).

Package name collision: the SQL adapter package is `sql`, so import it with
an alias when you also use `database/sql`:

```go
import (
    dbsql "database/sql"
    gotasql "github.com/dreamsxin/gota/v2/sql"
)
```

## 3. Element API removed → typed accessors

The `Element`/`Elements` interfaces, `Series.Elem(i)`, `Series.Map`, and the
`ElementValue` type are removed. Per-row access uses typed accessors that
read the column buffers directly:

| v1.x | v2 |
|---|---|
| `s.Elem(i).IsNA()` | `s.IsNA(i)` |
| `s.Elem(i).Val()` | `s.Val(i)` (nil when missing) |
| `s.Elem(i).String()` | `s.Record(i)` ("NaN" when missing) |
| `s.Elem(i).Float()` | `s.FloatAt(i)` (NaN when missing) |
| `s.Elem(i).Int()` / `.Int64()` | `s.IntAt(i)` / `s.Int64At(i)` (error when missing) |
| `s.Elem(i).Bool()` | `s.BoolAt(i)` |
| `s.Elem(i).Time()` | `s.TimeAt(i)` |
| `df.Elem(r, c)` | `df.CellVal(r, c)` / `df.CellNA(r, c)` |
| `s.Map(f)` | `series.MapFloat64` / `series.MapInt64` masked loops |

Mutation via `Elem(i).Set(v)` is replaced by `Series.Set(indexes, values)`.

## 4. Row-wise apply removed

`DataFrame.Rapply` and `DataFrame.RapplyParallel` are removed (RFC §9.4).
Row-oriented logic cannot vectorize over column buffers. Replacements:

- Express the logic column-wise (`Capply`, `CapplyParallel`, `Mutate`).
- Register a `dataframe.BatchTransform` and call `df.ApplyBatch(t)`.
- Write scalar row logic as a masked-loop kernel:
  `series.MapFloat64(s, func(v float64) float64 { ... })` or
  `series.MapInt64(s, func(v int64) int64 { ... })`.

## 5. Missing-value semantics

Missing values are tracked in validity bitmaps instead of sentinel values:

- Int columns distinguish the value `0` from missing. Code that relied on
  `0` meaning "unset" must use `IsNA`.
- Comparisons never match a missing operand (unchanged from v1), but
  equality against real values no longer accidentally hits sentinel-encoded
  missing rows.
- `Schema.Field.Nullable` reports observed nullability (a column without
  missing values is non-nullable) instead of always true; `Schema.Equal`
  now compares DTypes too.

## 6. Things that stay the same

- DataFrame/Series constructors (`New`, `NewNoCopy`, `LoadRecords`,
  `ReadCSV`, `ReadJSON`, `ScanCSV`, `ReadNDJSON`, `ReadHTML`).
- Filter/Query syntax, GroupBy/Aggregation, joins, Arrange, Mutate, Pivot,
  reshape, rolling/EWM, sticky error propagation, sentinel errors via
  `errors.Is`.
- Categorical (plus the new dictionary-backed Series view via
  `ToDictionarySeries`).

## 7. Optional: chain-local interning

Attach an execution context to canonicalize repeated GroupBy key strings
across a transformation chain (RFC §9.2). The pool is lock-free by contract
(one context per chain) and drops in O(1):

```go
ctx := series.NewExecutionContext()
defer ctx.Release()
out := df.WithExecutionContext(ctx).GroupBy("k1", "k2", "k3").Aggregation(...)
```
