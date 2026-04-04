# ROADMAP

This document tracks the current state of the project and planned work.
Status markers: ✅ done · 🔧 partial / needs polish · ❌ not started

---

## Released: v2.0.0

### Core DataFrame & Series (≤ v0.12.0 baseline)
- ✅ `New`, `LoadRecords`, `LoadMaps`, `LoadStructs`, `LoadMatrix`
- ✅ `ReadCSV` / `WriteCSV`, `ReadJSON` / `WriteJSON`, `ReadHTML`
- ✅ `Subset`, `SliceRow`, `Select`, `Drop`, `Set`, `Mutate`, `Rename`
- ✅ `Filter`, `FilterAggregation` (AND / OR), `CompFunc` user-defined comparators
- ✅ `Arrange` (multi-column sort, stable)
- ✅ `CBind`, `RBind`, `Concat`
- ✅ `InnerJoin`, `LeftJoin`, `RightJoin`, `OuterJoin`, `CrossJoin` (hash-join O(n+m))
- ✅ `GroupBy` + `Aggregation` (MAX/MIN/MEAN/MEDIAN/STD/SUM/COUNT)
- ✅ `GroupBy.Apply`, `GroupBy.Transform` (row-order preserved), `GroupBy.GetGroups`
- ✅ `Capply`, `Rapply`, `CapplyParallel`, `RapplyParallel`, `AggregationParallel`
- ✅ `Pivot`, `Describe`, `Duplicated`, `DropDuplicates`
- ✅ `FillNaN`, `DropNA`, `FillNAStrategy` / `FillNAStrategyLimit`
- ✅ `CumSum`, `CumProd`, `Diff`, `PctChange`, `Shift`
- ✅ `Corr`, `Cov`, `Melt`, `Stack`, `Unstack`, `Resample`
- ✅ `Head`, `Tail`, `Info`, `ValueCounts`, `NLargest`, `NSmallest`, `Sample`
- ✅ `IsNull`/`IsNA`, `NotNull`/`NotNA`, `Clip`, `ClipColumn`, `Replace`, `ReplaceInColumn`
- ✅ `Between`, `IsIn`, `FilterIsIn`, `Astype`
- ✅ `Pipe`, `PipeWithArgs`, `ApplyMap`
- ✅ `Assign`, `Explode`, `ExplodeOn`, `Query`, `RenameAll`, `AsCategorical`
- ✅ `gonum/mat.Matrix` interface

### Series
- ✅ Types: `Int`, `Float`, `String`, `Bool`, `Time`
- ✅ `FillNaNForward/Backward`, `FillNaNForwardLimit/BackwardLimit`
- ✅ `Rolling`: `Mean`, `Sum`, `Min`, `Max`, `StdDev` (Welford O(n)), `Apply`
- ✅ `EWM`: `Mean`, `Var`, `Std` (pandas-compatible Bessel correction)
- ✅ `CumSum`, `CumProd`, `CumMax`, `CumMin`, `Diff`, `PctChange`
- ✅ `Corr`, `Cov`, `Unique`, `NUnique`, `ValueCounts`
- ✅ `Clip`, `Replace`, `Between`, `IsIn`
- ✅ `Abs`, `Round`, `Sign`, `Pow`, `Sqrt`, `Log`, `Log10`, `Exp`
- ✅ `BatchConvert[T]` generics, `Order`, `Map`, `Slice`, `Subset`, `Concat`, `Append`
- ✅ `Categorical` type (dictionary encoding, `NewCategorical`, `CategoricalFromSeries`, `ToSeries`)
- ✅ `Mean`, `StdDev`, `Median`, `Min`, `Max`, `Sum`, `Quantile`

### I/O
- ✅ Excel: `ReadXLSX`/`WriteXLSX` (no CGO), `WithSheet`, `WriteXLSXMultiSheet`
- ✅ SQL: `FromSQL`, `WriteSQL` (batched INSERT, named placeholders `?`/`$1`/`@p1`)
- ✅ CSV: `ReadCSV`, `WriteCSV`, `ScanCSV` (streaming, large-file safe)
- ✅ JSON: `ReadJSON`, `WriteJSON`, `ReadNDJSON`, `WriteNDJSON` (JSON Lines)
- ✅ HTML: `ReadHTML`

### Index
- ✅ `Index` (single-level): `Loc`, `LocSlice`, `WithIndex`, `WithColumnIndex`, `ResetIndex`
- ✅ `MultiIndex` (hierarchical): `Loc` with partial key lookup, `WithMultiIndex`

### Infrastructure
- ✅ Go 1.24 module, Go generics (1.18+)
- ✅ `sync.Pool` for Series reuse (`GetSeries`/`PutSeries`)
- ✅ `NewNoCopy` zero-copy constructor
- ✅ Hash-join O(n+m), monotonic deque O(n) for `Rolling.Min`/`Max`
- ✅ `numWorkers()` helper — GOMAXPROCS guard (min 1)
- ✅ Structured error type with Op/Col/Row context

---

## v2.1 — In Progress (current)

### Fixes applied since v2.0.0
- ✅ `Clip` / `ClipColumn`: NaN values are now preserved (not clipped to bounds)
- ✅ `Between`: returns error when `left > right` instead of silently returning all-false
- ✅ `Explode` → `ExplodeOn(col, sep)`: custom separator support; `Explode` is now a comma shortcut
- ✅ `Info`: non-null count and memory estimate merged into a single O(n) pass
- ✅ `CapplyParallel` / `RapplyParallel` / `AggregationParallel`: `numWorkers()` guard prevents zero-size semaphore when `GOMAXPROCS=0`
- ✅ `ScanCSV`: deep-copy each batch row slice before passing to `LoadRecords` (prevents cross-batch data corruption)
- ✅ Test coverage: `Head`, `Tail`, `Info`, `IsNull`/`NotNull`, `ValueCounts`, `NLargest`/`NSmallest`, `Sample`, `Pipe`/`PipeWithArgs`/`ApplyMap`, `Clip`/`ClipColumn`, `Replace`/`ReplaceInColumn`, `Astype`, `Between`, `IsIn`/`FilterIsIn`, `ExplodeOn`, `Resample` all frequencies, `WriteXLSXMultiSheet`, `Categorical.Rename`

### Still open in v2.1
- 🔧 `Query`: column names containing spaces are not supported (operator search uses space as word boundary)
- 🔧 `Unstack`: missing (rowKey, varVal) combinations filled with `"NaN"` string instead of typed NaN
- 🔧 `ApplyMap`: column type may change to String when function returns a different type — no type-preservation option
- 🔧 `Categorical`: no cache for `ValueCounts`; repeated calls re-scan all codes

---

## Planned

## v2.2 — Released 2026-04-04
- ✅ `Query`: support quoted column names for columns containing spaces (`"col name" > 5`)
- ✅ `Unstack`: fill missing cells with typed NaN (Float NaN / empty String / false Bool) instead of `"NaN"` string
- ✅ `ApplyMapTyped(f)`: type-preserving variant of `ApplyMap`
- ✅ `Categorical.ValueCounts`: lazy cache invalidated on `SetValue`/`AddCategory`
- ✅ `DataFrame.Describe`: added `count` and `nunique` rows
- ✅ `Series.Mode()` — most frequent value
- ✅ `Series.Skew()` / `Series.Kurt()` — skewness and kurtosis
- ✅ `DataFrame.Interpolate(method)` — linear / forward-fill interpolation for numeric columns
- ✅ `DataFrame.CrossTab(row, col)` — contingency table (frequency cross-tabulation)

### v2.3 — Performance
- ❌ `Query`: cache column string representations to avoid repeated `fmt.Sprintf` per row
- ❌ `Unstack`: pre-parse composite row keys instead of repeated `strings.Split`
- ❌ `ValueCounts` (DataFrame): use element hash instead of `String()` conversion as map key
- ❌ `RapplyParallel`: use `sync.Pool` to reuse per-row Series objects
- ❌ `Resample`: use `int64` Unix timestamp as map key instead of formatted string
- ❌ Parallel `Arrange` for large DataFrames (parallel merge-sort)

### v2.4 — I/O & Interop
- ❌ Parquet read/write (`github.com/parquet-go/parquet-go`)
- ❌ `WriteSQL`: support `ON CONFLICT` / `UPSERT` for PostgreSQL and SQLite
- ❌ `ReadCSV` / `ScanCSV`: auto-detect delimiter (comma, tab, semicolon, pipe)
- ❌ `WriteXLSX`: cell formatting options (number format, bold header, column width)
- ❌ `ReadXLSX`: read all sheets at once → `map[string]DataFrame`

### v2.5 — Type System
- ❌ Nullable typed columns (`NullableInt`, `NullableFloat`) without boxing every element
- ❌ `Decimal` type for exact fixed-point arithmetic (financial use cases)
- ❌ Reduce `interface{}` usage — migrate hot paths to typed generics
- ❌ `Enum` type — ordered categorical with defined value set and comparison operators

### Long-term / Research
- ❌ Distributed DataFrame (chunked across goroutines or nodes)
- ❌ GPU-accelerated numeric operations
- ❌ Python interop via gRPC / shared memory for pandas round-trip
- ❌ Arrow / columnar memory layout for zero-copy interop with Apache Arrow
- ❌ Lazy evaluation / query plan for chained operations (avoid intermediate copies)
