# RFC: v2 Columnar Kernel

Status: accepted (open questions resolved 2026-08-18, see §9)
Date: 2026-08-18
Revised: 2026-08-28 - §9.2 thread-local hot cache dropped; §9.3 reduced to
byte-budget batch splitting; §9.4 wording aligned with §5 rule 4; the
Int-vs-NaN semantics change is bound to the /v2 module bump.
Revised: 2026-08-28 (release plan) - §1 re-anchored to the committed
benchmark suite; compatibility is dropped (§2, §7): v2.0.0 ships as a clean
break with no `Elem` adapter, no `Rapply` compat layer, and no in-v1.x
staging; §3.2, §6, §8, §9.4 updated to match.
Revised: 2026-08-28 (Milestone 1 landed) - the `v2` branch now stores every
Series in `column[T]` buffers with validity bitmaps; `Element`/`Elem` and
`Rapply` were removed in the same step; measured results: Mean 1M Float
4.9 ms / 8 MB → 0.13 ms / 0 allocs, Copy 100k Int 252 µs / 1.6 MB →
90 µs / 0.8 MB.
Scope: Series storage layout, batch kernels, DType system, and the v2.0.0
release path. This is a design document, not a feature list; every
performance claim below is measured on the v1 implementation by the
committed benchmarks in `series/benchmarks_test.go` unless marked as
projected.

## 1. Motivation: measured v1 costs

All numbers are produced by the committed benchmark suite
(`series/benchmarks_test.go`) on a 12th Gen i5-12400F; §1 and §8 acceptance
figures reference the same benchmarks, so every claim is reproducible with
`go test -bench=. -benchmem ./series/`.

| Metric | v1 (measured) | Benchmark | Why it costs that |
|---|---|---|---|
| Int/Float per element | 16 B | `BenchmarkSeries_Copy -benchmem` | `intElement{e int64; nan bool}` pads to 16 B; the 100k-Int copy allocates 1.6 MB; validity rides inline instead of a bitmap |
| String per element | 24 B | `BenchmarkSeries_Copy -benchmem` | `stringElement{e string; nan bool}`; 100k elements allocate 2.4 MB |
| `Mean` on 1M Float | 4.9 ms + 8 MB alloc | `BenchmarkSeries_Mean` | `Float()` materializes a full `[]float64` copy before summing |
| `Copy` 100k Int | 252 µs | `BenchmarkSeries_Copy` | element-struct copy loop instead of `memcpy` |
| `Elem(i).Float()` over 1M | 4.4 ms | `BenchmarkSeries_Elem` | interface dispatch per element |

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
- No backward compatibility with the v1 API or semantics. v2.0.0 is a clean
  break (§7): no compat adapters, no deprecation cycles, no in-v1.x staging.

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

The `Elements`/`Element` interface pair and per-element boxing leave the
package entirely. `Elem(i)` is removed at v2.0.0 - kernels never use
per-element interfaces, and without a compatibility obligation there is no
adapter worth maintaining.

## 4. DType system

Physical types stay as they are. Logical types become data:

```go
type DType interface {
    Physical() PhysicalType           // Int64, Float64, Utf8, Bool, Time, Dictionary
    Metadata() map[string]string      // e.g. decimal scale, enum ordering
}
```

v2 ships: physical DTypes mirroring `series.Type`, plus **Dictionary** as the
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
func compareInt64(data []int64, validity *bitmap, op, operand int64, mask *bitmap) // writes a selection mask
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
  the ownership contract tests carry over).
- Copy-on-write detach guards the dangerous case: writing through a shared
  view detaches it first. Sharing needs no manual refcounting: a view holds
  the parent's slice header, so the garbage collector keeps the parent
  buffer alive. The v1 "struct copy shares column storage" trap disappears
  because sharing is explicit and COW makes it safe.
- The DataFrame `Copy` semantic stays: `Copy` always materializes private
  buffers.

## 7. Release plan: v2.0.0 as a clean break

Compatibility with v1 is not a constraint (§2). The kernel lands on a v2
development branch and ships as v2.0.0 under the module path
`github.com/dreamsxin/gota/v2`; v1.x receives fixes only until that tag.
All breaking changes ship together in that one release and are listed in the
CHANGELOG plus a migration guide:

- The `Element`/`Elements` interfaces and `Elem(i)` are removed (§3.2).
- `Rapply`/`RapplyParallel` are removed; UDFs use `BatchTransform` and
  `ScalarFunc` (§9.4).
- Int-vs-NaN semantics change: Int columns distinguish 0 from missing via
  the validity bitmap.
- Adapters (Excel/Parquet/SQL) split into submodules per the ROADMAP
  decision.

The milestones below are the implementation order on the v2 branch; they are
reviewable units, not independently shipped releases:

1. **Milestone 1 - buffers (landed on the v2 branch).** Replace `Elements`
   implementations with column buffers. Because compatibility is not an
   obligation, the `Element`/`Elem` surface and `Rapply` were removed in the
   same step; typed Series accessors (`Val`, `IsNA`, `Record`, `FloatAt`,
   `IntAt`, `BoolAt`, `TimeAt`) replace them. Benchmarks improved against
   the §1 baseline and memory dropped ~2x.
2. **Milestone 2 - kernels.** Port the remaining hot operations (filters,
   sorting, joins) to batch kernels one by one, each with a golden-output
   test derived from v1 expectations except where semantics change
   deliberately. Sorting follows §9.1 (materialize + snapshot view).
   `BatchTransform` registration lands here. Sticky errors and sentinel
   wrapping carry over unchanged.
3. **Milestone 3 - DType.** Logical types land; `Categorical` becomes a
   DType; Schema exposes it. The chain-local intern pool (§9.2) and
   byte-budget chunking (§9.3) land with the Dictionary DType, where they
   are first needed.
4. **Milestone 4 - release.** Bump the module path to `/v2`, split adapters
   into submodules, publish the CHANGELOG entry and migration guide, tag
   v2.0.0.

## 8. Success criteria

- Numeric column memory within 1.1x of the ideal 8.125 B/row.
- `BenchmarkSeries_Mean` on 1M Float: <1 ms, zero allocations (from 4.9 ms /
  8 MB).
- `BenchmarkSeries_Copy` 100k Int: <30 µs (from 252 µs).
- The per-element dispatch cost measured by `BenchmarkSeries_Elem` (4.4 ms)
  disappears from hot paths entirely: kernels never touch the Element
  interface, and the benchmark retires with `Elem`.
- The v1.3+ suite is ported and keeps passing on v2: fuzz targets,
  ownership contract, sentinel errors, doc checks - with expectations
  updated only where semantics change deliberately.

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

### 9.2 String interning: chain-local pool only; no global pool

- **No process-global pool, ever.** Fine-grained locking across concurrent
  users would thrash cache locality and fragment memory; proposals to promote
  any pool to process scope are rejected outright.
- **Chain-local pool.** Gota is eager, so the query context maps to one
  transformation chain: an `ExecutionContext` owns the dictionary pool,
  interning Dictionary keys, constants, and GroupBy keys for the whole chain.
  It is released in O(1) with the context - no per-string teardown.
- **Hard rule:** no pointer reuse across chains.

A per-goroutine hot cache was proposed and rejected: Go provides no
goroutine-local storage, and goroutines migrate between OS threads, so such a
cache would need synchronization anyway, negating its purpose. It may be
revisited only if profiling shows pool contention is a measured cost.

Acceptance: under `CapplyParallel`-style concurrency, pool lock waiting stays
below 0.3% of CPU cycles.

### 9.3 Chunking: split batches at a byte budget

Chunk boundaries are decided by cumulative column-buffer bytes, never by row
count: a batch under construction splits when it passes **1.5 GiB of net
data**. The budget exists so no single allocation grows unbounded, and
serialization metadata (dictionary blocks, compression headers) always lands
on top of a known, capped base.

Chunking stays a memory technique for bounded allocations inside the
single-process scope (§2): no row-count limits, no automatic threshold
adjustment, no format conversion, and no disk spill in this RFC. Spilling
for blocking operators (Sort, Join) under memory pressure is future work,
gated on a measured need.

Acceptance: no single column-buffer allocation above 1.5 GiB in large-frame
workloads.

### 9.4 Rapply: removed at v2.0.0

Row-oriented apply boxes every value through the per-row Series interface
and forces scalar, branch-heavy loops that the compiler cannot vectorize; it
cannot share the batch kernel code paths. It is removed at v2.0.0 with no
compat fallback.

- All UDFs register as `BatchTransform` (column batch in, column batch out).
- Scalar row logic must be written as a `ScalarFunc`, which the kernel wraps
  into a masked loop over typed buffers. SIMD acceleration follows §5 rule 4:
  explicit SIMD is deferred until the plain-Go baseline is measured.
- The migration guide maps `Rapply`/`RapplyParallel` call sites to
  `BatchTransform` and `CapplyParallel` equivalents.

Acceptance: no kernel-path stack contains the row-wise implementation (CI
guard alongside the benchmark suite).

| Decision | Action | Acceptance |
|---|---|---|
| Sort | materialize + snapshot view + mandatory measurements | view deref < 5% of sort time |
| String pool | chain-local pool only, no global pool | lock wait < 0.3% CPU cycles |
| Chunking | 1.5 GiB byte-budget batch split, no exceptions | no allocation > 1.5 GiB |
| Rapply | removed at v2.0.0, no compat layer | no row-wise frames in kernel stacks |

