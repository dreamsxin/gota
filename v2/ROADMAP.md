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

## Completed in v1.3.0: API and Schema Reliability

All items landed in the v1.3.0 release (2026-08-18) and stay backward-
compatible within v1.x.

- [x] Define one ownership contract for `NewNoCopy`, mutating methods, and
  concurrent access; add aliasing and race tests for the documented contract
  (contract documented in README; aliasing and sticky-error tests run under
  the CI race job).
- [x] Apply the structured `Error` type consistently across public operations;
  retain sticky errors for v1 compatibility. Core operations and the I/O
  adapters (Query, Excel, SQL) wrap the exported sentinels for `errors.Is`
  with unchanged messages; tokenizer-level parse errors keep plain messages
  because they have no distinct matchable kinds.
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
- [x] Add documentation checks and a release checklist job to CI
  (internal/doccheck anchor tests run in the suite; CI verifies the latest
  tag has a dated CHANGELOG heading).
- [x] Add fuzz/property tests for joins and indexes (Query parsing, CSV
  delimiter detection, Parquet round-trips, type conversion, join counts,
  and Index/MultiIndex lookups are all fuzzed in CI).

Exit criteria: documented contracts, compatibility tests, no known silent data
corruption, and a release checklist runnable from a clean clone - all met as
of v1.3.0. The next milestone is the Columnar Kernel RFC below; until it
starts, v1.x work is limited to fixes and non-breaking additions.

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

Design document: [docs/rfc-columnar-kernel.md](docs/rfc-columnar-kernel.md)
(accepted 2026-08-18, revised 2026-08-28; sort snapshot views, chain-local
string interning, byte-budget batch chunking, and Rapply removal decided in
its §9). The measured v1 baseline it builds on, anchored by the committed
benchmarks in `series/benchmarks_test.go`: 16 B/element numeric columns,
4.9 ms + 8 MB alloc for a 1M-row Mean, 252 µs for a 100k-row Copy, and
4.4 ms for a 1M-element `Elem(i).Float()` walk (measured before `Elem` was
removed; the benchmark retired with it). This is a design milestone, not a
collection of independent features.

- [x] RFC reviewed and accepted (§9 decisions resolved: materialize + snapshot
  view with mandatory measurements, chain-local intern pool only with no
  global pool, 1.5 GiB byte-budget batch splitting with no exceptions and
  neither automatic threshold adjustment nor disk spill, Rapply deleted from
  the kernel path at v2; the Int-vs-NaN semantics change activates with the
  /v2 module bump, not earlier).
- [x] Prototype contiguous typed buffers with a validity bitmap and batch
  kernels (landed in the `v2/` directory as a clean break: buffers plus the
  aggregate/compare/fill kernels replaced the element storage directly; see
  Milestone 1 below).
- [x] Measure memory use and representative operations against the v1.2.1
  element-based implementation (Copy 100k Int: 252 µs / 1.6 MB → 90 µs /
  0.8 MB; Mean 1M Float: 4.9 ms / 8 MB → 0.13 ms / 0 allocs).
- [ ] Design Decimal and ordered Enum as logical DTypes on the same kernel.
- [ ] Add Arrow import/export only after buffer ownership and validity semantics
  can support a documented zero-copy path.
- [x] Decide migration and compatibility policy before replacing v1 storage
  (decision: no compatibility obligation - v2.0.0 ships as a clean break on
  the `/v2` module path; see RFC §7).

## Next: v2.0.0 Release

The columnar kernel ships as v2.0.0 under `github.com/dreamsxin/gota/v2`,
developed in the repository's `v2/` directory per Go's major-version
subdirectory layout (tags use the `v2/v2.0.0` form). Implementation follows
the RFC milestone order (buffers, kernels, DType, release); v1.x at the
repository root receives fixes only until the tag. Breaking changes ship
together in v2.0.0 and are enumerated in the CHANGELOG and a migration
guide.

- [x] Milestone 1: column buffers behind the Series API, memory ~2x down,
  benchmarks improved against the §1 baseline. Landed in the `v2/`
  directory with a scope decision: since compatibility is not an
  obligation, the `Element`/`Elem` API and the `Rapply` family were removed
  in the same step instead of Milestone 4, and the module path moved to
  `/v2` with the directory move.
- [x] Milestone 2: batch kernels landed - selection masks with word-wise
  AND/OR for Filter and Query, single-pass permutation arrange with the
  §9.1 measured snapshot view, typed single-key hash joins with batched
  output assembly, and the `BatchTransform`/`MapFloat64`/`MapInt64`
  registration API; golden-output tests and benchmark anchors committed.
- [x] Milestone 3: DType system landed - `DType` interface with physical
  singletons and the Dictionary logical type; dictionary-encoded columns
  (`dictionaryElements`) behave as String columns with DType identity;
  Schema exposes `Field.DType` and derives `Nullable` from the data;
  chain-local `ExecutionContext` intern pool feeds GroupBy keys (RFC §9.2,
  lock-free by contract); ScanCSV flushes on the 1.5 GiB byte budget
  (RFC §9.3).
- [x] Milestone 4: adapters split into the `v2/excel`, `v2/parquet`, and
  `v2/sql` submodules (core module dependency-light); Int columns
  distinguish 0 from missing (validity bitmaps, Milestone 1).
- [x] Publish the CHANGELOG entry (`## [2.0.0]`), MIGRATION.md, and tag
  `v2/v2.0.0` (2026-08-28).

## Research, Not Committed

- Lazy query plans and local chunked execution, gated by workload benchmarks.
- Python interoperability through standard Arrow IPC after Arrow support exists.
- Distributed and GPU execution. These require separate execution, scheduling,
  memory, and fault-tolerance architectures and are outside the current scope.
