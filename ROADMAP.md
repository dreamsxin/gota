# ROADMAP

This document tracks the current state of the project and planned work.
Status markers: ✅ done · 🔧 partial / needs polish · ❌ not started

---

## Completed

### Core DataFrame & Series (≤ v0.12.0)
- ✅ `New`, `LoadRecords`, `LoadMaps`, `LoadStructs`, `LoadMatrix`
- ✅ `ReadCSV` / `WriteCSV`, `ReadJSON` / `WriteJSON`, `ReadHTML`
- ✅ `Subset`, `SliceRow`, `Select`, `Drop`, `Set`, `Mutate`, `Rename`
- ✅ `Filter`, `FilterAggregation` (AND / OR), `CompFunc` user-defined comparators
- ✅ `Arrange` (multi-column sort, stable)
- ✅ `CBind`, `RBind`, `Concat`
- ✅ `InnerJoin`, `LeftJoin`, `RightJoin`, `OuterJoin`, `CrossJoin` (hash-join O(n+m))
- ✅ `GroupBy` + `Aggregation` (MAX/MIN/MEAN/MEDIAN/STD/SUM/COUNT)
- ✅ `GroupBy.Apply`, `GroupBy.Transform`, `GroupBy.GetGroups`
- ✅ `Capply`, `Rapply`
- ✅ `Pivot` (spreadsheet-style pivot table)
- ✅ `Describe`, `Duplicated`, `DropDuplicates`
- ✅ `FillNaN`, `DropNA` (any/all, subset)
- ✅ `FillNAStrategy` / `FillNAStrategyLimit` (ffill / bfill with limit)
- ✅ `CumSum`, `CumProd`, `Diff`, `PctChange`
- ✅ `Corr`, `Cov` (Pearson correlation / sample covariance matrix)
- ✅ `Melt` (wide → long)
- ✅ `Head`, `Tail`, `Info`, `ValueCounts`, `NLargest`, `NSmallest`, `Sample`
- ✅ `IsNull` / `IsNA`, `NotNull` / `NotNA`
- ✅ `Clip`, `ClipColumn`, `Replace`, `ReplaceInColumn`, `Astype`
- ✅ `Between`, `IsIn`, `FilterIsIn`
- ✅ `Pipe`, `PipeWithArgs`, `ApplyMap`
- ✅ `GetRow`, `Records`, `Maps`, `Elem`, `Col`, `ColIndex`, `Names`, `Types`
- ✅ `gonum/mat.Matrix` interface (`At`, `T`, `Dims`, `LoadMatrix`)

### Series
- ✅ Types: `Int` (int64), `Float` (float64), `String`, `Bool`, `Time`
- ✅ `FillNaNForward` / `FillNaNBackward`
- ✅ `FillNaNForwardLimit` / `FillNaNBackwardLimit`
- ✅ `Rolling` window: `Mean`, `Sum`, `Min`, `Max`, `StdDev`, `Apply`
- ✅ `EWM`: `Mean`, `Var`, `Std` (adjusted & non-adjusted, `MinPeriods`, `IgnoreNA`)
- ✅ `CumSum`, `CumProd`, `CumMax`, `CumMin`
- ✅ `Diff`, `PctChange`
- ✅ `Corr`, `Cov`
- ✅ `Unique`, `NUnique`, `ValueCounts`
- ✅ `BatchConvert[T]` generics + typed helpers (`BatchConvertInts` etc.)
- ✅ `Order` (stable sort, NaN pushed to end)
- ✅ `Map`, `Slice`, `Subset`, `Concat`, `Append`
- ✅ `Mean`, `StdDev`, `Median`, `Min`, `Max`, `Sum`, `Quantile`

### I/O
- ✅ Excel read/write (`ReadXLSX`, `WriteXLSX`, `ReadXLSXFile`, `WriteXLSXFile`) via excelize, no CGO
- ✅ SQL read/write (`FromSQL`, `WriteSQL`) with batched INSERT, CREATE TABLE, TRUNCATE options

### Index
- ✅ `Index` (single-level label index): `Loc`, `LocSlice`, `WithIndex`, `WithColumnIndex`, `ResetIndex`
- ✅ `MultiIndex` (hierarchical): `Loc` with partial key lookup, `WithMultiIndex`

### Infrastructure
- ✅ Go 1.24 module, Go generics (1.18+) used in `BatchConvert`
- ✅ `sync.Pool` for Series and element slice reuse (`pool.go`)
- ✅ `NewNoCopy` constructor for zero-copy DataFrame creation
- ✅ Hash-join replacing O(n²) nested-loop joins
- ✅ O(n) sliding-window algorithms for `Rolling.Min` / `Rolling.Max` (monotonic deque)
- ✅ Structured error type (`dataframe.Error`) with Op / Col / Row context

---

## In Progress / Needs Polish

### GroupBy
- ✅ `Transform` now preserves original row order via hidden `__groupby_row_idx__` column
- ✅ `GroupBy` key builder uses `fmt.Sprintf("%v", ...)` — supports all types including `Time`

### Rolling / EWM
- ✅ `Rolling.StdDev` rewritten with Welford's online sliding-window algorithm (O(n))
- 🔧 `EWM.Var` Bessel correction approximation (`den - w`) is not exactly equivalent to pandas — needs verification against reference values.

### Series
- ✅ `Subset` / `Copy` / `Append` / `Fill` for `Time` type fixed and tested
- ✅ `pool.go` element pool helpers made unexported; only `GetSeries`/`PutSeries` remain public

### DataFrame
- ✅ `Describe` shows min/max RFC3339 timestamps for `Time` columns
- 🔧 `Info` memory estimate is a rough heuristic (fixed bytes per type); does not account for string heap allocation.
- 🔧 `Sample` without replacement sorts the result indexes, which changes the relative row order. Should preserve the sampled order.

---

## Planned

### v1.2 — Correctness & Stability
- ✅ Fix `GroupBy.Transform` row-order preservation (attach `__groupby_row_idx__` before grouping, re-sort after)
- ✅ Add `Time` type support to `GroupBy` key builder
- ✅ Comprehensive `Time` Series tests (`Copy`, `Append`, `Subset`, `FillNaN*`, `Concat`, `Order`, element conversions)
- ✅ Make pool element helpers (`GetXxxElements` etc.) unexported
- ✅ `Rolling.StdDev` — Welford's online algorithm, O(n)
- ❌ Verify `EWM.Var` / `EWM.Std` against pandas reference values; add table-driven tests
- ✅ `Describe` — show `min` / `max` for `Time` columns as RFC3339 strings

### v1.3 — Missing pandas-equivalent APIs
- ✅ `DataFrame.Shift(periods int, subset ...string)` — shift column values by n rows
- ❌ `DataFrame.Resample` — time-based grouping (requires `Time` index)
- ❌ `DataFrame.Stack` / `Unstack` — reshape between wide and long with MultiIndex
- ✅ `Series.Clip` — element-wise clip
- ✅ `Series.Replace` — element-wise value replacement
- ✅ `Series.Between` — element-wise range check returning Bool Series
- ✅ `Series.IsIn` — membership test returning Bool Series
- ✅ `DataFrame.Explode(col)` — expand comma-separated column into rows
- ✅ `DataFrame.Assign(name, fn)` — add computed column via function
- ❌ `DataFrame.Query(expr)` — string-based filter expression (low priority)

### v1.4 — Performance
- ❌ Parallel `Capply` / `Rapply` using `GOMAXPROCS` worker pool
- ❌ Parallel `GroupBy` aggregation
- ❌ Arrow / columnar memory layout option for numeric columns (zero-copy interop with Apache Arrow)
- ❌ Lazy evaluation / query plan for chained operations (avoids intermediate copies)
- ❌ SIMD-friendly float operations via `gonum/blas` where applicable

### v1.5 — I/O & Interop
- ❌ Parquet read/write (`github.com/parquet-go/parquet-go` or `xitongsys/parquet-go`)
- ❌ `ReadXLSX` sheet selection option (`WithSheet(name)`) — stub exists but not wired
- ❌ `WriteSQL` support for named placeholders (`$1`, `@name`) for PostgreSQL / SQL Server
- ❌ `ReadCSV` streaming mode for files larger than memory
- ❌ JSON Lines (`ndjson`) read/write

### v1.6 — Type System
- ❌ Nullable typed columns (`Int64?`, `Float64?`) without boxing every element
- ❌ `Categorical` / `Enum` type for low-cardinality string columns (dictionary encoding)
- ❌ `Decimal` type for exact fixed-point arithmetic (financial use cases)
- ❌ Reduce `interface{}` usage — migrate to typed generics where Go version allows

### Long-term / Research
- ❌ Distributed DataFrame (chunked across goroutines or nodes)
- ❌ GPU-accelerated numeric operations
- ❌ Python interop via gRPC / shared memory for pandas round-trip
