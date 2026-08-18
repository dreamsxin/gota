# ROADMAP

This roadmap describes product scope, release state, and dependency order. A
feature is complete only when its public semantics are documented, edge cases
are tested, and performance claims include their assumptions.

## Product Scope

Gota is an embeddable, single-process, in-memory DataFrame and Series library
for Go. The project prioritizes predictable type and missing-value semantics,
composable eager operations, and practical batch I/O.

Pandas API parity, distributed execution, and GPU execution are not current
product goals.

## v1.2.1 Release

### Correctness and Contracts

- [x] Collision-safe, type-aware composite keys for hash joins.
- [x] Hash join complexity documented as expected `O(n+m+k)`, where `k` is
  the number of output rows.
- [x] MultiIndex full and partial lookups honor level boundaries and arbitrary
  string labels.
- [x] Query implements `AND` precedence over `OR`, parentheses, and quoted
  values containing logical words or commas.
- [x] README documents mutating operations, copy behavior, and sticky errors.

### I/O

- [x] Parquet read/write for String, Int, Float, Bool, and Time columns.
- [x] Missing values round-trip as Parquet null for every supported type.
- [x] Parquet writes use bounded row batches instead of materializing a second
  full-table row map.
- [x] Excel sheet selection, multi-sheet I/O, and write formatting.
- [x] SQL batched inserts, placeholder styles, and SQLite/PostgreSQL upserts.
- [x] Streaming CSV, delimiter detection, and NDJSON I/O.

### Data Operations

- [x] Hash joins, grouping, aggregation, reshape, resample, rolling, and EWM.
- [x] Parallel column, row, grouped aggregation, and large-frame arrange paths.
- [x] Time Series support and standalone dictionary-encoded Categorical values.
- [x] Basic Index and MultiIndex wrappers for label-based lookup.

## Next: API and Schema Reliability

These items must remain backward-compatible within v1.x.

- [x] Define one ownership contract for `NewNoCopy`, mutating methods, and
  concurrent access; add aliasing and race tests for the documented contract
  (contract documented in README; aliasing and sticky-error tests run under
  the CI race job).
- [ ] Apply the structured `Error` type consistently across public operations;
  retain sticky errors for v1 compatibility. Core failure paths now wrap the
  exported sentinels for `errors.Is` with unchanged messages; remaining work
  is the long tail of minor `fmt.Errorf` sites and I/O adapter errors.
- [x] Define a public schema/DType model covering physical type, logical type,
  and nullability (see Design Decisions; `dataframe.Schema` ships the
  physical-type and nullability surface, logical types extend it with the
  columnar kernel).
- [x] Define whether Index and Categorical remain adapters or become native
  DataFrame state/types; do not expand either API before this decision
  (decision: adapters through v1.x, see Design Decisions).
- [x] Split optional Excel, Parquet, and database adapters from the core package
  if this can be done without an import cycle or v1 API break (decision: not
  feasible without a break, deferred to the v2 kernel line, see Design
  Decisions).
- [x] Add supported-Go-version CI, race tests, and benchmark regression
  reporting (GitHub Actions: gofmt/vet/test matrix on Go 1.24.9 and stable,
  race job, benchstat comparison against the cached master baseline).
- [ ] Add documentation checks and a release checklist job to CI.
- [x] Add fuzz/property tests for joins and indexes (Query parsing, CSV
  delimiter detection, Parquet round-trips, type conversion, join counts,
  and Index/MultiIndex lookups are all fuzzed in CI).

Exit criteria: documented contracts, compatibility tests, no known silent data
corruption, and a release checklist runnable from a clean clone.

## Design Decisions (v1.x)

These decisions close the open design questions above for the v1 line.

### Schema/DType model

`dataframe.Schema` is the public column-layout surface: an ordered list of
`Field{Name, Type, Nullable}` backed by `series.Type`. In v1.x every column is
nullable, so `Nullable` is always true; it exists so the columnar kernel can
introduce non-nullable buffers without another API change. Logical types
(Decimal, ordered Enum) are deferred to the kernel milestone and will extend
`Field`, not replace it. `Schema.Equal` is the supported way to check join and
concat compatibility; `FromSchema` builds conforming empty frames for output
buffers and streaming accumulators.

### Index and Categorical stay adapters

`IndexedDataFrame`, `MultiIndexedDataFrame`, and `series.Categorical` remain
wrappers through v1.x. Promoting them to native DataFrame state would change
the `DataFrame` struct and every transformation's propagation rules - a
breaking change with unclear semantics (which columns survive a Mutate?). They
become candidates for native state only as part of the v2 columnar kernel,
where validity and dictionary encoding are storage-level concerns. Until then
their APIs stay as-is; no expansion.

### Adapter split is deferred to v2

Excel (excelize), Parquet (parquet-go), and SQL adapters live in the core
`dataframe` package and directly import their heavy dependencies. Removing
those dependencies from the core module requires moving `WriteParquet`,
`ReadXLSX`, `WriteSQL`, and friends out of `package dataframe` - an API break.
Thin wrappers cannot help: a wrapper import keeps the dependency in the graph,
so there is no dependency-weight win without the break. The split lands with
the v2 module reorganization if the kernel work proceeds.

## Later: Columnar Kernel RFC

This is a design milestone, not a collection of independent features.

- [ ] Prototype contiguous typed buffers with a validity bitmap and batch
  kernels, behind the current Series API.
- [ ] Measure memory use and representative operations against the v1.2.1
  element-based implementation.
- [ ] Design Decimal and ordered Enum as logical DTypes on the same kernel.
- [ ] Add Arrow import/export only after buffer ownership and validity semantics
  can support a documented zero-copy path.
- [ ] Decide migration and compatibility policy before replacing v1 storage.

## Research, Not Committed

- Lazy query plans and local chunked execution, gated by workload benchmarks.
- Python interoperability through standard Arrow IPC after Arrow support exists.
- Distributed and GPU execution. These require separate execution, scheduling,
  memory, and fault-tolerance architectures and are outside the current scope.
