# RFC: v2 Columnar Kernel

Status: draft for review
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
   golden-output test. Sticky errors and sentinel wrapping carry over
   unchanged.
3. **Phase 3 - DType.** Logical types land; `Categorical` becomes a DType;
   Schema exposes it. Int-vs-NaN semantics change here (introduced
   deliberately, called out in the release notes).
4. **Phase 4 - v2 module.** Split adapters (Excel/Parquet/SQL) into
   submodules per the ROADMAP decision; bump module path to `/v2`.

Each phase is independently shippable and revertible. Phase 1-2 can ship
inside v1.x as pure performance work if the API holds.

## 8. Success criteria

- Numeric column memory within 1.1x of the ideal 8.125 B/row.
- `Mean` on 1M Float: <1 ms, zero allocations (from 5.2 ms / 8 MB).
- `Copy` 100k Int: <30 µs (from 273 µs).
- All v1 golden tests pass unchanged through Phase 2.
- The v1.3+ suite keeps passing: fuzz targets, ownership contract, sentinel
  errors, doc checks.

## 9. Open questions

1. Should `Sort`/`Arrange` produce views (permutation arrays) or
   materialized copies? Views save memory; permutation indirection costs on
   every later gather. Proposal: views, measure, revisit.
2. String interning for dictionary DType: per-column or per-DataFrame pool?
3. Chunked columns for >2 GB frames: needed before or after Arrow IPC?
4. Does `Rapply` survive? It is inherently row-oriented; a view-based
   kernel world makes it the slow path by definition. Keep with a doc
   warning, or deprecate at v2.
