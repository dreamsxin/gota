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

### Rolling / EWM
- ✅ `Rolling.StdDev` rewritten with Welford's online sliding-window algorithm (O(n))
- ✅ `EWM.Var` / `EWM.Std` rewritten with pandas-compatible weighted Bessel correction; verified against reference values

### DataFrame
- ✅ `Describe` shows min/max RFC3339 timestamps for `Time` columns
- ✅ `Info` memory estimate uses actual string lengths for String columns
- ✅ `Sample` without replacement preserves sampled row order (removed spurious `sort.Ints`)

---

## Planned

### v1.2 — Correctness & Stability
- ✅ Fix `GroupBy.Transform` row-order preservation
- ✅ Add `Time` type support to `GroupBy` key builder
- ✅ Comprehensive `Time` Series tests
- ✅ Make pool element helpers unexported
- ✅ `Rolling.StdDev` — Welford's online algorithm, O(n)
- ✅ Verify `EWM.Var` / `EWM.Std` against pandas reference values; rewritten with correct formula
- ✅ `Describe` — show `min` / `max` for `Time` columns as RFC3339 strings

### v1.3 — Missing pandas-equivalent APIs
- ✅ `DataFrame.Shift(periods int, subset ...string)`
- ✅ `DataFrame.Resample(colname, freq)` — time-based grouping with `Aggregation`
- ✅ `DataFrame.Stack` / `Unstack` — wide↔long reshape
- ✅ `Series.Clip`, `Series.Replace`, `Series.Between`, `Series.IsIn`
- ✅ `DataFrame.Explode(col)` — expand comma-separated column into rows
- ✅ `DataFrame.Assign(name, fn)` — add computed column via function
- ✅ `DataFrame.Query(expr)` — expression-based row filter (col op val, AND/OR, in/not in)

### v1.4 — Performance
- ✅ Parallel `Capply` via `CapplyParallel` using `GOMAXPROCS` worker pool
- ✅ Parallel `Rapply` via `RapplyParallel` — row order preserved
- ✅ Parallel `GroupBy` aggregation via `AggregationParallel`
- ❌ Arrow / columnar memory layout option for numeric columns
- ❌ Lazy evaluation / query plan for chained operations
- ❌ SIMD-friendly float operations via `gonum/blas`

### v1.5 — I/O & Interop
- ❌ Parquet read/write
- ✅ `ReadXLSX` sheet selection via `WithSheet(name)` option — fully wired
- ✅ `WriteSQL` named placeholders — `SQLPlaceholderDollar` ($1) for PostgreSQL, `SQLPlaceholderAt` (@p1) for SQL Server
- ✅ `ReadCSV` streaming mode — `ScanCSV(r, batchSize, fn)` for large files
- ✅ JSON Lines (`ndjson`) — `ReadNDJSON` / `WriteNDJSON`

### v1.6 — Type System
- ❌ Nullable typed columns (`Int64?`, `Float64?`) without boxing every element
- ❌ `Categorical` / `Enum` type for low-cardinality string columns (dictionary encoding)
- ❌ `Decimal` type for exact fixed-point arithmetic (financial use cases)
- ❌ Reduce `interface{}` usage — migrate to typed generics where Go version allows

### Long-term / Research
- ❌ Distributed DataFrame (chunked across goroutines or nodes)
- ❌ GPU-accelerated numeric operations
- ❌ Python interop via gRPC / shared memory for pandas round-trip
