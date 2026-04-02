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
- 🔧 `Transform` does not preserve original row order — groups are iterated over a Go map (non-deterministic). Needs an explicit row-index tracking mechanism.
- 🔧 `GroupBy` key generation uses `fmt.Sprintf` with type-switched format strings; `time.Time` values are not handled and return an error. Should use a type-safe key builder.

### Rolling / EWM
- 🔧 `Rolling.StdDev` uses O(n·w) naive per-window computation instead of Welford's online algorithm. Acceptable for small windows but degrades on large ones.
- 🔧 `EWM.Var` Bessel correction approximation (`den - w`) is not exactly equivalent to pandas — needs verification against reference values.

### Series
- 🔧 `Subset` / `Copy` / `Append` / `Fill` for `Time` type were missing and have been fixed, but `Time` Series still lacks dedicated tests for these paths.
- 🔧 `pool.go` exposes `GetXxxElements` / `PutXxxElements` as public API but `BatchConvert` no longer uses them (fixed use-after-free). The pool API should either be made internal or documented as unsafe for direct use.

### DataFrame
- 🔧 `Describe` returns `"-"` for `Time` columns — could show min/max timestamps instead.
- 🔧 `Info` memory estimate is a rough heuristic (fixed bytes per type); does not account for string heap allocation.
- 🔧 `Sample` without replacement sorts the result indexes, which changes the relative row order. Should preserve the sampled order.

---

## Planned

### v1.2 — Correctness & Stability
- ❌ Fix `GroupBy.Transform` row-order preservation (attach `__row_idx__` before grouping, re-sort after)
- ❌ Add `Time` type support to `GroupBy` key builder
- ❌ Comprehensive `Time` Series tests (`Copy`, `Append`, `Subset`, `FillNaN*`, `Concat`)
- ❌ Make pool helpers (`GetXxxElements` etc.) unexported or add safety documentation
- ❌ `Rolling.StdDev` — switch to Welford's online algorithm for O(n) performance
- ❌ Verify `EWM.Var` / `EWM.Std` against pandas reference values; add table-driven tests
- ❌ `Describe` — show `min` / `max` for `Time` columns as RFC3339 strings

### v1.3 — Missing pandas-equivalent APIs
- ❌ `DataFrame.Shift(periods int)` — shift column values by n rows
- ❌ `DataFrame.Resample` — time-based grouping (requires `Time` index)
- ❌ `DataFrame.Stack` / `Unstack` — reshape between wide and long with MultiIndex
- ❌ `Series.Clip` — element-wise clip (currently only on DataFrame)
- ❌ `Series.Replace` — element-wise value replacement
- ❌ `Series.Between` — element-wise range check returning `[]bool`
- ❌ `Series.IsIn` — membership test returning `[]bool`
- ❌ `DataFrame.Explode(col)` — expand list-valued column into rows
- ❌ `DataFrame.Assign(name, fn)` — add computed column via function
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
