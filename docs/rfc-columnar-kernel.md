# RFC: v2 Columnar Kernel

Status: accepted (open questions resolved 2026-08-18, see §9)
Date: 2026-08-18
Scope: Series storage layout, batch kernels, DType system, and the migration
path. This is a design document, not a feature list; every performance claim
below is measured on the v1 implementation unless marked as projected.

## 1. Motivation: measured v1 costs

All numbers from this repository's benchmarks (12th Gen i5-12400F, Go 1.25,
1M rows unless stated).

| Metric | v1 (measured) | Why it costs that |
|---|---|---|
| Int/Float per element | 16 B | `intElement{e int64; nan bool}` pads to 16 B; validity rides inline instead of a bitmap |
| String per element | 24 B | `stringElement{e string; nan bool}` |
| `Mean` on 1M Float | 5.2 ms + 8 MB alloc | `Float()` materializes a full `[]float64` copy before summing |
| `Copy` 100k Int | 273 µs | element-struct copy loop instead of `memcpy` |
| `Elem(i).Float()` over 1M | 1.16 ms | interface dispatch per element |

A contiguous typed buffer with a validity bitmap is ~2x smaller for numerics
and enables vectorized kernels with zero per-element dispatch. The kernel is
therefore about three things: memory, speed, and a semantics upgrade (fast
null handling) that the current layout cannot express.

## 2. Non-goals

- No pandas API parity beyond what already exists.
- No distributed execution, no GPU (unchanged from product scope).
- No Arrow interchange in this RFC; zero-copy Arrow import/export is a
  follow-up once buffer ownership is settled (see ROADMAP).
- No lazy query planning. Operations stay eager; chunking is a memory
  technique, not an execution model.

## 3. Physical layout

### 3.1 Column buffer

```go
type column struct {
    dtype    DType      // logical type (see §4)
    data     unsafe.Pointer // typed: []int64 / []float64 / []string / ...
    validity *bitmap    // nil when all-valid (fast common case)
    length   int
    // offset/count support zero-cost views over a parent buffer
    offset   int
}
```

- **Typed data arrays.** Each physical type stores one Go slice:
  `[]int64`, `[]float64`, `[]string` (string data stays inline; dictionary
  encoding is a DType concern, §4), `[]bool`, `[]time.Time`.
- **Validity bitmap.** One bit per row, `nil` means fully valid. This is the
  single source of truth for missing data; the v1 "NaN as a value" trick
  (NaN floats, "NaN" strings, nan flags) is retired. Int columns can finally
  distinguish 0 from missing.
- **Views.** `offset` + `length` give zero-copy slices of a parent buffer.
  `Subset`, `Head`, `Tail`, and `Filter` return views; a view pins its
  parent's memory alive (refcounted, §6).

### 3.2 What leaves the layout

The `Elements`/`Element` interface pair with per-element boxing. `Elem(i)`
remains as a compatibility adapter (§7) but is never used internally by
kernels.

## 4. DType system

Physical types stay as they are. Logical types become data:

```go
type DType interface {
    Physical() PhysicalType           // Int64, Float64, Utf8, Bool, Time, Dictionary
    Metadata() map[string]string      // e.g. decimal scale, enum ordering
}
```

v1 ships: physical DTypes mirroring `series.Type`, plus **Dictionary** as the
first logical DType (absorbing `series.Categorical`). Decimal and ordered
Enum arrive later on the same interface, without storage changes.

The v1.x `Schema` model (already released) maps directly: `Field.Type`
becomes `DType`; `Nullable` becomes real - a column may be declared
non-nullable, letting kernels skip validity work entirely.

## 5. Batch kernels

Kernels are typed functions over whole columns, never per-element
interfaces:

```go
func sumFloat64(data []float64, validity *bitmap, out *stat) // no allocs
func compareInt64(data []int64, validity *bitmap, op, operand, mask []bool) // writes a selection mask
func gather(data, validity, rows []int) column                // view materialization
```

Rules:

1. **Masks in, masks out.** Filter kernels consume and produce selection
   bitsets; combining AND/OR filters is bitwise, allocation-free.
2. **Validity composes.** Every kernel takes the validity bitmap as an
   operand. Missing-in means missing-out, matching today's NaN semantics so
   test expectations port directly.
3. **No materialization without need.** `Mean`/`Sum`/`Min`/`Max` walk the
   data array directly; the `Float()` materialize-then-reduce pattern is
   deleted from hot paths.
4. **Vectorization-ready.** Loops are written so the compiler's bounds-check
   elimination and SIMD-friendly patterns apply; explicit SIMD (avo/assembly)
   is deferred until the Go-generic baseline is measured.

## 6. Ownership and sharing

- `NewNoCopy` semantics become first-class: a Series is a view; mutation
  through a view writes through to the parent buffer (documented, tested -
  the v1.3.1 ownership tests port over).
- Copy-on-write refcounting guards the dangerous case: writing through a
  shared view detaches it first. The v1 "struct copy shares column storage"
  trap disappears because sharing is explicit and COW makes it safe.
- The DataFrame `Copy` semantic stays: `Copy` always materializes private
  buffers.

## 7. Migration and compatibility

This is the highest-risk part; the plan is deliberately boring:

1. **Phase 0 (this RFC).** Merge the design; measure the v1 baseline (done,
   §1) and keep it as the comparison reference.
2. **Phase 1 - buffer behind Series.** Replace `Elements` implementations
   with column buffers while keeping the `Series` API byte-compatible.
   Public behavior is unchanged; benchmarks must show no regression and
   memory should drop ~2x. `Elem(i)` adapters preserve the Element API.
3. **Phase 2 - kernels.** Port hot operations (aggregations, filters,
   sorting, joins) to batch kernels one by one, each with a v1
   golden-output test. Sorting follows §9.1 (materialize + snapshot view).
   `BatchTransform` registration lands here; `Rapply` grows the §9.4
   perf warning. Sticky errors and sentinel wrapping carry over
   unchanged.
3. **Phase 3 - DType.** Logical types land; `Categorical` becomes a DType;
   Schema exposes it. Int-vs-NaN semantics change here (introduced
   deliberately, called out in the release notes). The chain-local
   intern pool (§9.2) and 1.5 GiB chunking (§9.3) land with the
   Dictionary DType, where they are first needed.
4. **Phase 4 - v2 module.** Split adapters (Excel/Parquet/SQL) into
   submodules per the ROADMAP decision; bump module path to `/v2`.
   `Rapply` leaves the package here (§9.4).

Each phase is independently shippable and revertible. Phase 1-2 can ship
inside v1.x as pure performance work if the API holds.

## 8. Success criteria

- Numeric column memory within 1.1x of the ideal 8.125 B/row.
- `Mean` on 1M Float: <1 ms, zero allocations (from 5.2 ms / 8 MB).
- `Copy` 100k Int: <30 µs (from 273 µs).
- All v1 golden tests pass unchanged through Phase 2.
- The v1.3+ suite keeps passing: fuzz targets, ownership contract, sentinel
  errors, doc checks.

## 9. Decisions (resolved 2026-08-18)

The four open questions are decided. Where a ruling used database-engine
terms, it is mapped to Gota's single-process eager scope; the intent is kept.

### 9.1 Sort: materialize internally, expose a read-only snapshot view

`Arrange`/`Sort` materializes the sorted output buffers once, then returns a
read-only snapshot view - an index of `(buffer, rowOffset)` over the
materialized data, not a lazy permutation. Sorting is a global reorder whose
intermediate state must be frozen before downstream operators see it; a lazy
view would hand out pointers into unstable memory.

Views must carry measurement metadata: `num_rows`, `total_byte_size`, and
`spill_ratio` (0 while all in-memory). A sort view without measurements is a
constructor error, not a warning: exact post-sort cardinality is what memory
estimation and the future Arrow zero-copy exchange depend on.

Acceptance: view dereference costs < 5% of total sort time (benchmarked).

### 9.2 String interning: chain-local pools + thread-local hot cache; no global pool

- **No process-global pool, ever.** Fine-grained locking across concurrent
  users would thrash cache locality and fragment memory; proposals to promote
  any pool to process scope are rejected outright.
- **Chain-local pool (primary).** Gota is eager, so the query context maps to
  one transformation chain: an `ExecutionContext` owns the dictionary pool,
  interning Dictionary keys, constants, and GroupBy keys for the whole chain.
  It is released in O(1) with the context - no per-string teardown.
- **Thread-local hot cache (secondary).** Worker goroutines keep a
  read-only weak-reference map of at most 1024 hot constants that bypasses
  the pool's atomic refcounting; entries evict beyond the cap.
- **Hard rule:** no pointer reuse across chains.

Acceptance: under `CapplyParallel`-style concurrency, pool lock waiting stays
below 0.3% of CPU cycles.

### 9.3 Chunking: flush at 1.5 GiB, never at 2 GiB

Chunk boundaries are decided by cumulative column-buffer bytes, never by row
count. A RecordBatch under construction flushes when it passes **1.5 GiB of
net data**; the only exception is fixed-width numeric columns (Int64-class)
in final aggregate output, which may extend to **1.9 GiB** before converting
to an IPC streaming form.

The 500 MiB headroom exists because allocator success rates for large
contiguous virtual ranges degrade near 2 GiB, and serialization metadata
(dictionary blocks, compression headers) lands on top of net bytes. Every
flush emits a `ChunkedEvent` metric; five consecutive 1.5 GiB flushes
auto-lower the threshold to 1.2 GiB to stop thrashing.

Blocking operators (Sort, Join) whose single column would cross the
threshold spill to disk rather than panic on a failed allocation. Spilling
is a memory-pressure escape hatch inside the single-process scope, not a
distributed execution feature.

Acceptance: no single allocation above 1.6 GiB in large-frame workloads.

### 9.4 Rapply: removed from the kernel path, demoted to the compat layer

Row-oriented apply cannot vectorize and would poison escape analysis for
every kernel compiled near it; it is deleted from the kernel path.

- All UDFs register as `BatchTransform` (column batch in, column batch out).
- Scalar row logic must be written as a `ScalarFunc`, which the kernel wraps
  into a vectorized masked loop (AVX-512/NEON when available).
- The v1-compatible `Rapply` survives only in the compat layer for
  diagnostics, prints `[PERF WARNING] Row-level fallback engaged`, and
  reports accumulated slow-path time after the call.
- v1.x keeps `Rapply` with a doc warning; the symbol leaves the package at
  v2.0.0 and callers migrate to `BatchTransform`.

Acceptance: no kernel-path stack contains the row-wise implementation (CI
guard alongside the benchmark suite).

| Decision | Action | Acceptance |
|---|---|---|
| Sort | materialize + snapshot view + mandatory measurements | view deref < 5% of sort time |
| String pool | chain-local pool + 1024-entry hot cache, no global | lock wait < 0.3% CPU cycles |
| Chunking | 1.5 GiB flush (1.9 GiB fixed-width exception) | no allocation > 1.6 GiB |
| Rapply | kernel deletion; compat layer with warning | no row-wise frames in kernel stacks |

