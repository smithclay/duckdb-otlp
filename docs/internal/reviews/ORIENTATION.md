# Phase 1 — Orientation

## Baseline (clean branch `hardening-pass`, commit ad725fcb)

- `GEN=ninja make` — clean (exit 0). Artifact `build/release/extension/otlp/otlp.duckdb_extension` (30M).
- `make test` — green, **250 assertions in 16 test cases**.
- `make format-check` — **fails for an environmental reason only**: the repo's format venv has no `black` on PATH (`format.py` aborts before checking anything). Not a code finding. A fresh venv (black 24.10.0 / clang-format 11.0.1 / clang-tidy / cmakelang) is being staged for Phase 4 fixes.
- `GEN=ninja make tidy-check` — same environmental block: `run-clang-tidy.py` aborts with "Unable to run clang-tidy" (no `clang-tidy` on PATH). Not a code finding.

## Module map

The extension has three faces sharing one Rust parsing core:

1. **File-read table functions** (`read_otlp_*`). `src/storage/otlp_extension.cpp` registers six readers plus `read_otlp_metrics`/`read_otlp_metrics_summary` placeholders, `otlp_serve/otlp_stop/otlp_server_list/otlp_seal_list/otlp_flush`, and `otlp_uri_parser`. `src/function/read_otlp.cpp` is the FFI bridge: bind caches the Arrow schema from `otlp_get_schema`, scan reads a whole file (≤100 MB) into `file_buffer`, calls `otlp_transform` (logs/traces) or `otlp_transform_metrics_all` (metrics, keep one shape, release the rest), then `src/otlp_arrow.cpp` converts the Arrow C array → DuckDB `DataChunk` `STANDARD_VECTOR_SIZE` rows at a time with projection.

2. **Live ingest server** (`src/otlp_server.cpp` + `src/otlp_server_http.cpp` + `src/otlp_storage.cpp`). httplib worker pool parses+converts+buffers per request into per-signal `ColumnDataCollection`s (each behind its own mutex); a single background sealer thread group-commits on size (`seal_target_bytes`, default 128 MiB admitted bytes) or age (`seal_max_age_ms`, default 5 s), one transaction per seal. Two durable backends: a catalog (`Appender` into a table, transactional) or `parquet_export_path` (per-signal `COPY ... TO` partitioned dataset, at-least-once, no local table). Admission backpressure is an atomic byte counter (`max_buffered_bytes`, default 512 MiB) reserving `max(body,1024)` of *input* bytes. Optional post-seal DuckLake `CHECKPOINT` maintenance. `otlp_storage.cpp` holds the per-DatabaseInstance server registry.

3. **Native daemon** (`src/server/main.cpp` + `server_config.cpp`). Embeds DuckDB, loads the static extension, generates mode setup SQL (7 modes: local/aws/r2-local/r2-neon DuckLake, parquet, r2-data-catalog Iceberg, s3-tables), starts `otlp_serve` + optional `quack_serve`, sets the mode catalog as instance default, waits for readiness, handles SIGTERM/SIGINT, seals on stop.

**Rust core** `external/otlp2records`: `bindings/ffi.rs` is the FFI surface (`catch_unwind` on each entry, `repr(C)` enums/structs, Arrow C Data Interface export). `schema/arrow.rs` builds the six canonical Arrow schemas; `schema/defs.rs` is a *second*, parallel field-list definition for "schema inspection". `decode/` + `batch/` do the parse→Arrow work.

## Data path

`POST /v1/{logs,traces,metrics}` → auth → admission reserve → `otlp_transform[_metrics_all]` (Rust, Arrow out) → `CopyArrowStructToDataChunk` → `ColumnDataCollection::Append` under per-signal lock → 202. Sealer wakes on trigger → swap each buffer for a fresh empty collection under all locks → append swapped collections to catalog/Parquet in one transaction → release admission. `read_otlp_*` is the offline mirror: file → Rust transform → projected Arrow→DataChunk.

## Where the bodies are likely buried (leads to verify, not conclusions)

- **Two schema sources of truth.** `schema/arrow.rs` (the real schema) and `schema/defs.rs` (`SchemaField{required}`) are independent. `service_name` nullability and timestamp nullability already disagree in spirit between them; check whether `defs.rs` is even reachable from the C++ surface, and whether it can silently drift.
- **`status_status_message`** (traces) — a doubled-word column name baked into both `arrow.rs` and `schemas.md`. A 1.0 freeze will inherit it. `duration_time_unix_nano` is a Duration mislabeled as a unix-nano timestamp.
- **Doc/contract drift.** `AGENTS.md` advertises column counts 25/15/16/18/22/27; the real counts (and `schemas.md`/`api.md`) are **24/18/17/19/22/27**. `api.md` says the size trigger is "~64 MiB" but the default is 128 MiB. `api.md` omits `otlp_seal_list` entirely though it is a registered public function.
- **Seal failure semantics.** Catalog path rolls back and re-buffers; Parquet path is at-least-once and counts exported rows into `committed_rows_total` even on `success=false`. The implicit DB-close path (`~OtlpStorageExtensionInfo` → `ShutdownIngest` with `db_ptr` already expired) **drops buffered rows** — confirm this is surfaced honestly and that 202 callers can't be misled.
- **Admission vs heap.** `max_buffered_bytes` bounds *input* bytes, not decoded columnar heap; a pathological expansion ratio could OOM under backpressure. Worth quantifying.
- **FFI lifetime.** The metrics path hand-rolls release of 4 batches + chosen-schema; verify exactly-once release on every partial-failure branch. `catch_unwind(AssertUnwindSafe)` is present on all entries — confirm no `&mut`/unwind-unsafe state can be observed post-panic.
- **Hot path.** Whole-file materialization (acknowledged prototype), `CopyArrowStructToDataChunk` rebuilds identity `column_ids` per chunk, ingest `AppendArrowBatch` re-inits a `DataChunk` per APPEND_CHUNK_SIZE slice.
