# V1 Slop & Hardening Report — duckdb-otlp pre-1.0 pass

Branch `hardening-pass`. Baseline (commit ad725fcb): `GEN=ninja make` clean, `make test`
green (250 assertions / 16 cases), `make format-check` passes with correct tooling
(local failure was a missing-`black` venv, not code). Local `clang-tidy` is environmentally
unusable (the pip/brew LLVM clang-tidy can't resolve this Apple-clang build's libc++ headers,
erroring on every file); CI runs format+tidy with pinned tooling and gates on them.

Six reviewers (`docs/internal/reviews/A–F`) plus a competitive memo
(`docs/internal/COMPETITIVE_POSITIONING.md`). No blockers. The findings below are
deduplicated, ranked, and partitioned by **where the fix lives**, because
`external/otlp2records` is a **pinned git submodule** (tag `v0.8.4`, its own repo) — Rust/FFI
fixes are cross-repo and cannot be committed on this branch without a coordinated release.

## Cross-reviewer overlaps (strong signal)

| Theme | Reviewers | Verdict |
| --- | --- | --- |
| `defs.rs` is a dead, ungated second schema source of truth | API-004 (high) + SLOP-001 (high) | Real, high — two independent reviewers. Delete (submodule). |
| `AGENTS.md` column counts wrong (25/15/16/18 → 24/18/17/19) | API-003 + SLOP-008 + BLD-003 | Real — three reviewers. In-repo doc fix. |
| FFI status enum values never produced (`InvalidFormat`/`OutOfMemory`) | FFI-005 + SLOP-002 | Real, low. Submodule. |

## Global ranking (severity × confidence × blast-radius)

### HIGH

| ID | Where | Finding | Fix location |
| --- | --- | --- | --- |
| ~~API-001~~ | — | ~~`status_status_message` doubled-word~~ — **NOT a defect: OTAP-canonical name** (otel-arrow data model). Won't fix. | n/a |
| ~~API-002~~ | — | ~~`duration_time_unix_nano` misnamed~~ — **NOT a defect: OTAP-canonical name** (duration field is literally named this in the otel-arrow model). Won't fix. | n/a |
| API-004 = SLOP-001 | integrity | `defs.rs` dead parallel schema, no drift guard | **submodule** |
| CON-001 | durability | Decoded columnar heap is unbounded; `max_buffered_bytes` caps only *input* bytes → OOM under stuck/slow seal while `admitted_bytes` shows headroom | **in-repo** (`otlp_server.*`) |
| CON-002 | durability/obs | Persistently-failing seal keeps returning 202 while `/readyz` stays a static 200; only signal is a stderr WARNING | **in-repo** (`otlp_server_http.cpp`) |
| BLD-001 | CI safety | PR gate compiles the daemon but never builds/boots the published image or runs ingest→seal e2e (only `docker-smoke` does, gated to main) | **in-repo CI** (needs user OK) |

### MEDIUM

| ID | Where | Finding | Fix |
| --- | --- | --- | --- |
| FFI-002 | ABI | Generated `otlp2records_ffi.h` is unused, won't compile standalone, drifted from the real ABI — future-contributor foot-gun | submodule (`build.rs`) |
| HOT-001 | read hot path | `std::string fmt(schema.format)` + `substr` built per column per chunk in `CopyArrowToDuckDB` | in-repo |
| HOT-002 | ingest hot path | `CopyArrowStructToDataChunk` rebuilds an identity `column_ids` vector every 2048-row slice | in-repo |
| HOT-003 | read hot path | Projection pushdown prunes only the Arrow→DuckDB copy; Rust FFI still materializes all columns (the bigger half) | in-repo (document) |
| HOT-004 | ingest hot path | Row copied ~4× wire→catalog; bridging hops #2/#3 unavoidable w/o Arrow-direct collection append | in-repo (document) |
| SLOP-003 | integrity | `SqlQuote`/`SqlEscape`/`QuoteIdentifier`(=`QuoteIdent`) copy-pasted across 3 TUs (injection surface) | in-repo |
| SLOP-004 | integrity | `release_batch` FFI-ownership lambda duplicated byte-for-byte in two TUs | in-repo |
| SLOP-005 | integrity | `Truthy`/`EnvTruthy` re-implement the same accepted-value set in 2 TUs | in-repo |
| SLOP-002 = FFI-005 | ABI | Two FFI status codes never produced | submodule |
| API-005 | docs | `api.md` "~64 MiB" seal trigger; real default 128 MiB | in-repo doc |
| API-006 | docs | `schemas.md` types the 4 bucket/bound list columns as VARCHAR JSON; readers return `BIGINT[]`/`DOUBLE[]` | in-repo doc |
| API-007 | API | `otlp_uri_parser` is a registered public scalar, documented nowhere | in-repo (doc or drop) |
| API-008 | docs | `time_unix_nano` etc. are `TIMESTAMP_NS` in file readers but lossy `TIMESTAMP` (µs) in live tables, same name | in-repo doc |
| CON-004 | durability | Parquet at-least-once can duplicate rows *within* a signal whose partitioned COPY fails mid-write (documented; comment reads as per-signal-atomic) | in-repo (comment) |
| CON-005 | durability | 3-attempt drain WARNING conflates "no graceful stop" with "repeated seal failure" | in-repo (log text) |
| CON-010 / BLD-005 | tests | Seal-path invariants only covered by manual harnesses not in any CI gate; vendored Rust not gated in-repo | recommend |
| BLD-002 | CI/integrity | `CMakeLists.txt` claims a "docker-smoke gzip e2e canary" that no longer exists after the smoke harness went JSON-only — comment now lies; gzip ingest uncovered | in-repo |

### NIT
FFI-001 (submodule: narrow schema-leak window if a panic lands between the two `ptr::write`s in `export_record_batch`), FFI-003/004/006/007 (verifications, no defect), HOT-005 (`filter_pushdown=false`, not an easy win), HOT-006 (`unsealed_admission_bytes` could be atomic), HOT-007 (provably-false per-row overflow check on widening unsigned→signed), CON-003 (size-seal can lag one poll interval on a lost notify; latency only), CON-009 (`ListSeals` copies under registry lock), SLOP-006 (`ReadOTLPRustBindData::format` always AUTO), SLOP-007 (`ReadOTLPMetricsUnsupportedScan` unreachable — required ctor placeholder), SLOP-009 (one-line `otlp_request.hpp`), API-009 (`api.md` omits `otlp_seal_list`), API-010 (`serve.md` undercounts its own function table), API-011 (confirm throwing placeholders at freeze), BLD-004 (only `otlp_serve` carries the WASM bind guard), BLD-006 (pre-commit/CI tooling-version drift; local format venv broken).

## Triage

**V1 blockers:** none. The build is clean, tests green, the FFI boundary is sound (exactly-once
Arrow release verified on all reachable paths, complete panic barrier), and the single-writer
seal invariant holds.

**Correction — API-001/API-002 are NOT defects.** The two "awkward" trace column names are the
OpenTelemetry Arrow (OTAP) data-model field names verbatim, and the schema deliberately conforms
to that model (README + `schemas.md`). The reviewer/orchestrator misread them; a rename was made
and fully reverted. The schema is unchanged. There are **no** freeze-critical renames.

**Do-now in-repo cleanup (while the code is fresh):** CON-001 (bound decoded heap / at minimum
surface buffered bytes), CON-002 (`/readyz` degrades on stuck seals), the doc-drift cluster
(API-003/005/006/007/008/009/010, AGENTS.md counts), the hot-path allocation nits
(HOT-001/002), the conceptual-integrity dedup (SLOP-003/004/005), BLD-002 (restore/retire the
gzip canary honestly), and the durability comment/log clarifications (CON-004/005, HOT-003/004
documentation).

**Recommend, needs user sign-off (CI policy):** BLD-001 — run the image build + smoke e2e on PRs,
not only post-merge. Touches the CI workflow; flagged for the user rather than changed unilaterally.

**Cross-repo, deferred with rationale (otlp2records submodule, pinned v0.8.4):** API-001, API-002,
API-004/SLOP-001 (delete `defs.rs`), SLOP-002/FFI-005 (drop unused status codes), FFI-002 (drop
or relocate the generated header), FFI-001 (write array-before-schema or document the adjacency
invariant). These are correct and worth doing, but belong in an otlp2records release; documented
here as the cross-repo work list.

---

## Status log (updated as Phase 4 progresses)

(Each entry: ID(s) — status — commit.)

- API-003 / BLD-003 / SLOP-008 (AGENTS.md column counts 25/15/16/18 → 24/18/17/19) — **fixed** (docs).
- API-005 (api.md "64 MiB" → 128 MiB) — **fixed** (docs).
- API-006 (schemas.md bucket/bound lists VARCHAR → BIGINT[]/DOUBLE[]) — **fixed** (docs).
- API-008 (serve.md cross-link the TIMESTAMP_NS-vs-TIMESTAMP precision note) — **fixed** (docs).
- API-009 (api.md add `otlp_seal_list`) — **fixed** (docs).
- API-010 (serve.md "four+one" undercount → "five server functions") — **fixed** (docs).
- SLOP-004 (duplicated `release_batch` FFI lambda) — **fixed** — commit 4834922a (shared `ReleaseOtlpArrowBatch`).
- SLOP-003 (SQL-quoting helpers ×3) + SLOP-005 (`Truthy`/`EnvTruthy`) — **fixed** — commit 4e440a78 (shared `otlp_sql_util.hpp` + daemon `EnvTruthy`; daemon DRY_RUN config tests confirm SQL unchanged).
- HOT-002 (identity `column_ids` rebuilt per ingest slice) — **fixed** — commit 2f6b92a5 (precomputed per `OtlpSignalBuffer`; dead `CopyArrowStructToDataChunk` removed; manual concurrency harness PASS).
- BLD-002 (stale gzip-canary comment + lost CI coverage) — **fixed** — commit 27b2200d (smoke_test.py POSTs gzip + asserts 202; CMakeLists comment corrected).
- CON-004 (per-signal at-least-once comment), CON-005 (shutdown drop-cause WARNING disambiguated), HOT-003 (FFI projection limitation documented) — **fixed** — commit 82868b64.
- HOT-001 (`std::string fmt` per column per chunk) — **deferred, won't-fix**: Arrow format strings are all ≤ ~5 chars, well within libc++/libstdc++ SSO (~15–22 bytes), so the construction and the `substr(0,3)` temporaries are stack-allocated — there is no heap churn. A full const-char* rewrite of a correctness-critical conversion function for a micro-CPU saving with no measured benefit is exactly the speculative perf change the pass is meant to avoid.
- HOT-004/005/006/007, CON-003/009 (nits) — **deferred** (documented in reviews; latency/micro only, no correctness impact). HOT-006 (`unsealed_admission_bytes` could be atomic) intentionally left: the seal-restore arithmetic composes under the all-buffers lock today; an atomic refactor is unmeasured.

- CON-002 (`/readyz` stays 200 while seals fail) — **fixed** — commit 3bf76a24 (`/readyz` returns 503 when `SealStalled()`; daemon healthcheck inherits it; idle/healthy still 200).
- CON-001 (decoded heap unobservable) — **observability fixed** — commit 3bf76a24 (`buffered_bytes` column on `otlp_server_list`; SQLLogicTest extended). Full decoded-heap *backpressure* gate still **deferred** (a new bound/knob is a larger design decision; the column makes the OOM risk visible in the meantime).
- API-007 (`otlp_uri_parser` undocumented) — **fixed** (documented in `serve.md` URI-scheme section; kept as public, not dropped).

### Decisions taken (user, this session)
1. Schema renames + Rust cleanups: **do now** (submodule + repo).
2. Durability: **CON-002 + CON-001 observability**.
3. BLD-001 CI PR-gate: **leave to the user** — documented, no workflow change.

- **API-001 / API-002 — WON'T FIX (reviewer error, corrected).** `status_status_message` and `duration_time_unix_nano` are **not** typos: they are the exact field names in the OpenTelemetry Arrow (OTAP) span schema (verified against `open-telemetry/otel-arrow/docs/data_model.md` — the spans record defines `duration_time_unix_nano` of type `duration` and `status_status_message` of type `string`), and the README/schemas docs state the schema deliberately aligns with that model. Reviewer D (and the orchestrator) wrongly read them as drift. A rename was briefly made and then **reverted in full** after the maintainer flagged it; the schema is unchanged. The C++ "deliberate divergence from OTAP" note refers only to the duration *type* mapping (BIGINT vs INTERVAL), never the name. Do not re-flag these.
- API-004/SLOP-001 (delete dead `defs.rs`), SLOP-002/FFI-005 (drop never-produced FFI status codes), FFI-002 (remove generated header + cbindgen), FFI-001 (export_record_batch write-order note) — **fixed** in otlp2records submodule commit **bfeeb36** (branch `hardening-renames`, **UNPUSHED**, no schema-name change); 116 cargo tests pass. Submodule pointer bumped in parent commit **241b3fd**; `make test` 250 assertions + manual harness PASS. **Release step (user): push otlp2records, tag, re-point submodule.**
- BLD-001 (PR gate doesn't build/boot the published image or run ingest→seal e2e) — **deferred to user (CI policy)**. Recommended fix: run `docker-smoke` (build `runtime-prebuilt` + `scripts/smoke_test.py`, amd64-only, reuses the `daemon-amd64` build cache) on PRs, or at least build+boot `runtime-prebuilt` on PRs, in `.github/workflows/MainDistributionPipeline.yml`. Today a Dockerfile-runtime, arm64-only, or ingest/seal regression first surfaces on the publish run.
