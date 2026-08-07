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

- [ ] Define one ownership contract for `NewNoCopy`, mutating methods, and
  concurrent access; add aliasing and race tests for the documented contract.
- [ ] Apply the structured `Error` type consistently across public operations;
  retain sticky errors for v1 compatibility.
- [ ] Define a public schema/DType model covering physical type, logical type,
  and nullability.
- [ ] Define whether Index and Categorical remain adapters or become native
  DataFrame state/types; do not expand either API before this decision.
- [ ] Split optional Excel, Parquet, and database adapters from the core package
  if this can be done without an import cycle or v1 API break.
- [ ] Add fuzz/property tests for joins, indexes, Query parsing, type conversion,
  and I/O round-trips.
- [ ] Add supported-Go-version CI, race tests, documentation checks, and
  benchmark regression reporting.

Exit criteria: documented contracts, compatibility tests, no known silent data
corruption, and a release checklist runnable from a clean clone.

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
