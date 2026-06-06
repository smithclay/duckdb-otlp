# Review B — FFI / Arrow C Data Interface boundary

Reviewer B. Charter: the FFI / Arrow C Data Interface boundary between the Rust core
(`external/otlp2records`) and the C++ extension. Read-only review; baseline (build clean,
250 assertions green) taken as given.

## Summary

The FFI boundary is in good shape and noticeably more defensive than typical first-pass FFI
code. Every `#[no_mangle] extern "C"` entry is wrapped in `catch_unwind` and converts a caught
unwind into `OtlpStatus::Internal`, so a Rust panic cannot unwind into C++ (no UB); the project
also never sets `panic = "abort"`, so the catch path is live rather than dead. Arrow
`ArrowSchema`/`ArrowArray` ownership is documented and, on the **happy path and on all the
error paths I could reach**, released exactly once: the bind caches the schema (freed in the
dtor), the logs/traces scan releases the per-call schema and keeps the array, and the metrics
scan keeps one shape's array, releases its schema, and releases the other present batches via a
helper that nulls `present` so the chosen batch can't be double-freed. The ingest callers
(`BufferSignal`/`BufferMetrics`) wrap the append in try/catch and release on the throwing path.
`#[repr(C)]` layouts match the hand-written C header, and `FFI_ArrowArray`/`FFI_ArrowSchema`
are arrow-rs's documented C-Data-Interface-compatible structs so the by-value embedding in
`OtlpArrowBatch` is sound. The findings below are mostly latent/low: a narrow leak window if a
panic lands *between* the two `ptr::write`s inside `export_record_batch` (FFI-001), a
second hand-maintained C header (the cbindgen output `otlp2records_ffi.h`) that is generated but
never compiled and has drifted from the real ABI (FFI-002), `OtlpStatus::OutOfMemory`/
`InvalidFormat` being declared but never returned (FFI-005), and several pieces of
boundary-defense that guard states the current Rust core cannot produce (noted as nits, not
removal recommendations). No blocker. One medium (FFI-002, the drifted unused header is an ABI
foot-gun for future contributors).

---

## FFI-001 — Panic between the two `ptr::write`s in `export_record_batch` leaks the first-written struct

- **Location:** `external/otlp2records/src/bindings/ffi.rs:171-188` (`export_record_batch`),
  reached from `otlp_transform` (`:250`) and via `export_optional_metric_batch` (`:190-204`)
  from `otlp_transform_metrics_all` (`:315-328`). C++ consumers: `read_otlp.cpp:276-298`,
  `otlp_server.cpp:568-571,615-618`.
- **Category:** ownership / release-exactly-once (leak window), panic interaction
- **Severity:** medium · **Confidence:** medium

`export_record_batch` does `ptr::write(out_schema, ffi_schema)` then `ptr::write(out_array,
ffi_array)` (`:184-185`). Both `FFI_ArrowSchema::try_from` and `FFI_ArrowArray::new` already ran
before either write, so the structs are fully constructed and the two `ptr::write`s are
effectively memcpys with no obvious panic site between them. *If* one did panic there (or in any
future code inserted between the writes), `catch_unwind` returns `OtlpStatus::Internal`. The C++
callers treat any non-`OK` status as "no batch present / nothing to release" — `read_otlp.cpp:295-298`
comments "On failure no batch is present; nothing to release"; `BufferSignal` only releases inside
the `try` after a successful `otlp_transform` — so the already-written `out_schema` would leak its
Arrow private_data. The contract ("OK ⇒ both written; non-OK ⇒ nothing to release") is honored as
long as the two writes are atomic-in-practice, which they currently are.

Note the *metrics* path is already hardened against the analogous hazard: `export_optional_metric_batch`
sets `present = 0` first (`:194`), writes, then sets `present = 1` last (`:201`), so a panic mid-export
leaves `present = 0` and `release_all`/`release_batch` correctly skip it (`otlp_server.cpp:596-607`,
`read_otlp.cpp:318-329`). The single-shape `out_array`/`out_schema` pair has no such "commit flag", which
is why this window exists only there.

- **Minimal action:** none strictly required today. If hardening: write the array first and the
  schema second, or have `export_record_batch` release the already-written struct if the second
  write's surrounding scope can fail — but the cheapest correct guarantee is the doc-comment note
  that the two writes must remain panic-free and adjacent.

---

## FFI-002 — Generated `otlp2records_ffi.h` is unused, undefines its own Arrow types, and has drifted from the real ABI

- **Location:** `external/otlp2records/include/otlp2records_ffi.h` (whole file), produced by
  `external/otlp2records/build.rs:6-61`; compare to the hand-written `otlp2records.h` that C++
  actually includes (`src/function/read_otlp.cpp:17`, `src/include/otlp_arrow.hpp:6`,
  `src/include/otlp_server.hpp:8`).
- **Category:** ABI stability / maintenance foot-gun
- **Severity:** medium · **Confidence:** high

There are two C headers and only one is real. C++ includes **only** `otlp2records.h` (the
hand-written header that defines `struct ArrowArray`/`struct ArrowSchema` by value and embeds them
by value in `OtlpArrowBatch`). The cbindgen-generated `otlp2records_ffi.h` is written into the
crate on every `ffi`-feature build (`build.rs:48-55`) but is included by **no** C++ source
(`grep` for `otlp2records_ffi.h` / `FFI_Arrow` in `src/` returns nothing). Worse, that generated
header is not even self-consistent: it is emitted with `no_includes: true` (`build.rs:24`) plus only
`stdint.h`/`stddef.h`, yet its structs reference `FFI_ArrowArray`/`FFI_ArrowSchema`
(`otlp2records_ffi.h:108-109`) which it never defines — so the file would not compile on its own.
It also drifts in *kind*: the generated enums are `OtlpInputFormat { Auto, Protobuf, ... }`
(Rust variant names) whereas the consumed header uses `OTLP_FORMAT_AUTO` etc. A future contributor
who "fixes the build" by switching the `#include` to the generated header, or who treats it as the
source of truth, would silently desync the ABI. The two-header split is the most likely future ABI
break on this boundary.

- **Minimal action:** either (a) delete the generated `otlp2records_ffi.h` and drop the cbindgen
  step from `build.rs` (the hand-written header is canonical), or (b) keep generation but emit to a
  build-output dir (`OUT_DIR`), not into `include/`, and add a one-line comment in `otlp2records.h`
  marking it as the canonical, hand-maintained ABI. Surgical: option (a) removes the foot-gun
  outright.

---

## FFI-003 — `repr(C)` layouts match the C header; Arrow-by-value embedding is sound (verification, no defect)

- **Location:** `external/otlp2records/src/bindings/ffi.rs:34-121` vs `external/otlp2records/include/otlp2records.h:61-132`.
- **Category:** ABI stability · **Severity:** nit (verification) · **Confidence:** high

Field-by-field the Rust `#[repr(C)]` types match the hand-written header: `OtlpSignalType`/
`OtlpInputFormat`/`OtlpStatus` are field-order- and value-identical (0..5 / 0..3 / 0..5). A C `enum`
is `int`-sized and a `#[repr(C)]` fieldless enum is also `int`-sized, so they agree. `OtlpArrowBatch`
is `{ array, schema, present }` in both, and `OtlpMetricsArrowBatches` is the four batches followed
by four `u64`/`uint64_t` skip counters in both (`ffi.rs:104-121`, header `:114-132`). The only
representational subtlety is that Rust embeds `FFI_ArrowArray`/`FFI_ArrowSchema` by value while the C
header embeds `struct ArrowArray`/`struct ArrowSchema` by value: arrow-rs documents `FFI_ArrowArray`/
`FFI_ArrowSchema` as `#[repr(C)]` structs that are layout-identical to the Arrow C Data Interface
`ArrowArray`/`ArrowSchema`, so the by-value embedding is ABI-compatible. `present` is `c_int` in Rust
(`ffi.rs:107`) and `int` in the header (`:117`) — same type.

One coupling worth recording: the layout match is unenforced — nothing in CI compares the hand-written
header to the Rust structs (FFI-002 is the related risk). The arrow-rs major version (`arrow-* = "58"`,
`Cargo.toml:29-33`) is the implicit contract for the Arrow C Data structs; a major bump that altered
`FFI_ArrowArray` layout would silently break the by-value embedding, but the C Data Interface struct
shape is a stable cross-language ABI, so this is very low risk.

- **Minimal action:** none. Optionally add a `static_assert(sizeof/offsetof)` in one C++ TU, or a
  Rust build-time `const _: () = assert!(...)` on the enum discriminants, to make the contract
  self-checking.

---

## FFI-004 — Panic barrier is complete and the `AssertUnwindSafe` usage is sound

- **Location:** `external/otlp2records/src/bindings/ffi.rs:146-165, 230-270, 306-340, 348-368`;
  panic strategy `external/otlp2records/Cargo.toml:73-77` (no `panic` key) and no `.cargo/config`
  override.
- **Category:** panic barrier · **Severity:** nit (verification) · **Confidence:** high

Every `#[no_mangle] extern "C"` data entry (`otlp_get_schema`, `otlp_transform`,
`otlp_transform_metrics_all`) wraps its body in `catch_unwind(AssertUnwindSafe(...))` and maps a
caught unwind to `OtlpStatus::Internal` via `.unwrap_or(OtlpStatus::Internal)`, so no panic can
unwind across the FFI boundary. `otlp_status_message` (`:348-368`) is the one entry *without*
`catch_unwind`, which is correct: it only indexes static byte slices and `as_ptr()`, none of which
can panic, and it returns a raw pointer (no `Result` to fold a status into). The profile sets `lto`,
`opt-level=3`, `codegen-units=1`, `strip` but **not** `panic = "abort"`, and there is no
`.cargo/config[.toml]`, so the default `panic = "unwind"` applies and `catch_unwind` actually
catches — the catch path is live, not dead code (the WASM bindings in `wasm.rs` are a separate
surface that returns `Result<_, JsError>` and don't use this path).

On `AssertUnwindSafe` soundness: the asserted-safe state is the set of out-params written through raw
pointers. A panic returns `Internal` (never `Ok`), and the C++ side treats non-`Ok` as
"nothing present" — so a half-written success can't be *observed as success*. The metrics path writes
`present` last (FFI-001 discussion), and the skip counters are written only after all four exports
succeed (`ffi.rs:333-336`), so neither a partially-present batch nor partial skip counters are ever
observed on the `Ok` path. The one residual is the schema-leak window in FFI-001, which is a leak,
not unsoundness.

- **Minimal action:** none.

---

## FFI-005 — `OtlpStatus::OutOfMemory` and `OtlpStatus::InvalidFormat` are declared but never returned

- **Location:** `external/otlp2records/src/bindings/ffi.rs:90-95` (variants), returns at
  `:161,245,251,267,311,330` only ever produce `InvalidArgument`/`ParseFailed`/`Internal`/`Ok`.
- **Category:** error returns / contract surface · **Severity:** nit · **Confidence:** high

The status enum advertises six codes; the FFI functions only ever return four of them
(`Ok`, `InvalidArgument`, `ParseFailed`, `Internal`). `OutOfMemory` (4) and `InvalidFormat` (3) are
never produced — an allocation failure in arrow-rs/prost would abort or unwind (→ `Internal`), not
surface as `OutOfMemory`, and format-detection failures collapse into `ParseFailed` (`:267,311`).
This is harmless (the C++ side maps any non-`OK` to an `IOException`/`OtlpHttpError` via
`otlp_status_message`, `read_otlp.cpp:277,297`, `otlp_server.cpp:570,617`), but the contract over-
promises. `otlp_status_message` does handle all six (`:358-365`), so there's no missing-arm risk.

- **Minimal action:** none required; optionally document these two as reserved/unused, or drop them
  to keep the ABI honest (dropping changes the enum — do it pre-1.0 if at all).

---

## FFI-006 — Boundary defenses that guard states the current Rust core cannot produce (cruft assessment)

- **Location:** `read_otlp.cpp:216-218` (`current_batch.length < 0`), `:356-361`
  (`array.length < 0` after metrics), `:338-341` (`array = {}` for an absent chosen shape);
  `otlp_arrow.cpp` invalid-offset / negative-length `IOException`s (`:218-220,244-245,330-331,
  410-411,426-427`) and the buffer-count guards (`:199-201,251-253,...`).
- **Category:** defensive-cruft assessment (requested) · **Severity:** nit · **Confidence:** medium

The boundary is defended against malformed Arrow that arrow-rs's exporter does not currently emit:
negative `length`, negative/decreasing string & list offsets, missing buffers, null children. These
are not currently reachable through `otlp2records` (its schemas are fixed and arrow-rs produces
well-formed arrays), so strictly they guard impossible states. I would **keep** them: this is a
genuine trust boundary (the C++ struct is a raw C ABI; a future schema with a real list/struct, a
different arrow-rs version, or a third-party producer of these structs could violate the invariants),
the checks are O(1) per column or O(rows) only on the path that already iterates, and converting a
malformed buffer into an `IOException` instead of a segfault is exactly the right posture for a
pre-1.0 ingest server. The `array = {}` synthesis for an absent metric shape (`:338-341`) is
correctly inert: zeroing makes `release` null and `length` 0, so the subsequent `length < 0` /
`release && length > 0` / `release` checks (`:356-368`) all no-op without touching uninitialized
memory.

- **Minimal action:** none. (Flagged only because the charter asked whether the defense is warranted —
  it is.)

---

## FFI-007 — `out_array`/`out_schema` left uninitialized on early-return error, but never read (verification)

- **Location:** `read_otlp.cpp:265-266` (`ArrowArray array; ArrowSchema schema;` — uninitialized
  locals), `otlp_server.cpp:243` / `:564-565` (uninitialized locals), `ReadOTLPRustBindData::arrow_schema`
  (`read_otlp.cpp:31`, member with no initializer); Rust early returns
  `ffi.rs:142-144,226-228,291-293`.
- **Category:** ownership / use-of-uninitialized · **Severity:** nit · **Confidence:** high

The C++ callers declare `ArrowArray`/`ArrowSchema` as plain (uninitialized) locals/members and rely on
the FFI to fill them. On the FFI's null-pointer early returns (`InvalidArgument`) the out-params are
not written — but those returns require the caller to have passed a null pointer, which none of the
callers do (`&array`, `&schema`, `&result->arrow_schema`, `&batches` are all non-null). On a caught
panic the out-params for the single-shape functions may be unwritten, but the caller checks `status`
first and throws before reading `.release`/`.length` (`read_otlp.cpp:276,295`, `otlp_server.cpp:568,
615`). The metrics struct is `= {}`-zeroed by the caller before the call (`read_otlp.cpp:293`,
`otlp_server.cpp:592`) and additionally zeroed by the FFI up front (`ffi.rs:297-304`), so it is always
safely inspectable. So there is no actual use-of-uninitialized; recording it because the safety
depends on the status-first discipline being maintained at every call site.

- **Minimal action:** none required; value-initializing the locals (`ArrowArray array {};`) would make
  the safety local rather than contract-dependent, at zero cost.
