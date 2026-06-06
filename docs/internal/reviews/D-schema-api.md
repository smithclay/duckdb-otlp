# Reviewer D — Schema Integrity & Public API Surface (1.0 freeze)

## Summary

The schema and API surface is in better shape than the leads suggested, but it carries
two **frozen-forever naming mistakes** and a cluster of doc drift. The canonical schemas
live in `external/otlp2records/src/schema/arrow.rs` (reached via the only schema FFI entry,
`otlp_get_schema`); the parallel `defs.rs` is **dead code** — re-exported from the Rust
crate but never called by any FFI entry, C++ caller, daemon, or test, and never compared
against `arrow.rs`. Contrary to the orientation lead, `arrow.rs` and `defs.rs` currently
**agree on every column name, order, and nullability** (a full field-by-field diff finds
zero disagreements once you map `required = !nullable`); the risk is not present drift but
*silent future* drift, since nothing pins them together. Real, measured column counts are
**24 / 18 / 17 / 19 / 22 / 27** (traces / logs / gauge / sum / histogram / exp_histogram);
`schemas.md` and `api.md` match, but `AGENTS.md` is wrong in two places, and `api.md` still
quotes a stale "~64 MiB" seal trigger (real default 128 MiB). The biggest 1.0 traps are
two column names baked into *both* the file-read and live-ingest surfaces:
`status_status_message` (doubled word) and `duration_time_unix_nano` (a BIGINT ns Duration
named like a unix-nano timestamp). Renaming either is cheap now and impossible after 1.0.
The `read_otlp_metrics` / `read_otlp_metrics_summary` placeholders fail cleanly on bind; no
misleading summary stub is reachable from the shipped surface. One scalar function
(`otlp_uri_parser`) is registered but undocumented; one type asymmetry (`TIMESTAMP_NS` in
file readers vs `TIMESTAMP` in live tables for the *same* column names) is real and only
half-explained.

### Severity counts
- **blocker:** 0
- **high:** 4 (API-001, API-002, API-003, API-004)
- **medium:** 4 (API-005, API-006, API-007, API-008)
- **nit:** 3 (API-009, API-010, API-011)

### Headline IDs
- **API-001** (high) — `status_status_message`: doubled-word column name frozen into both surfaces. Rename now or never.
- **API-002** (high) — `duration_time_unix_nano`: a Duration→BIGINT(ns) named like a unix-nano timestamp. Misleading name frozen into both surfaces.
- **API-003** (high) — `AGENTS.md` advertises wrong column counts (25/15/16/18 for traces/logs/gauge/sum; real 24/18/17/19).
- **API-004** (high) — `defs.rs` is an unreachable second schema source of truth with no drift guard; either delete it or gate it with a test.
- **API-005** (medium) — `api.md` says seal size trigger is "~64 MiB"; real default `seal_target_bytes` = 128 MiB.
- **API-006** (medium) — `schemas.md` types `bucket_counts`/`explicit_bounds`/`*_bucket_counts` as `VARCHAR (JSON array)`, but file readers return typed `BIGINT[]` / `DOUBLE[]`.
- **API-007** (medium) — `otlp_uri_parser` is a registered public scalar function, undocumented anywhere.
- **API-008** (medium) — Same column name, two types across surfaces: file readers expose ns timestamps as `TIMESTAMP_NS`; live-ingest tables store them as `TIMESTAMP` (µs, lossy). Documented once in `schemas.md` but not in `api.md`/`serve.md`.
- **API-009** (nit) — `api.md` omits `otlp_seal_list` (it is registered and `serve.md` documents it).
- **API-010** (nit) — `serve.md` "registers four lifecycle functions and one diagnostic function" undercounts the surface (also ignores `otlp_uri_parser`).
- **API-011** (nit) — `read_otlp_metrics` empty/union vs shape-specific naming asymmetry worth a final decision before freeze.

---

## Enumerated public API surface (ground truth from code)

All registrations: `src/storage/otlp_extension.cpp:22-38` + `src/function/read_otlp.cpp:378-429`.

### File-read table functions (single positional `path` VARCHAR; no named params)
| Function | Bind | Columns | Notes |
|---|---|---|---|
| `read_otlp_logs(path)` | `read_otlp.cpp:121` | 18 | `arrow.rs:8` |
| `read_otlp_traces(path)` | `read_otlp.cpp:126` | 24 | `arrow.rs:31` |
| `read_otlp_metrics_gauge(path)` | `read_otlp.cpp:131` | 17 | `arrow.rs:60` |
| `read_otlp_metrics_sum(path)` | `read_otlp.cpp:136` | 19 | `arrow.rs:82` |
| `read_otlp_metrics_histogram(path)` | `read_otlp.cpp:159` | 22 | `arrow.rs:106` |
| `read_otlp_metrics_exp_histogram(path)` | `read_otlp.cpp:165` | 27 | `arrow.rs:133` |
| `read_otlp_metrics(path)` | `read_otlp.cpp:150` | — | placeholder; bind throws `NotImplementedException` (`read_otlp.cpp:154`) |
| `read_otlp_metrics_summary(path)` | `read_otlp.cpp:172` | — | placeholder; bind throws `NotImplementedException` (`read_otlp.cpp:145`) |

`projection_pushdown = true`, `filter_pushdown = false` on all six real readers
(`read_otlp.cpp:382-423`). Glob support via DuckDB FS (`read_otlp.cpp:94`). Hard 100 MB
per-file cap (`read_otlp.cpp:252`).

### Lifecycle / diagnostic table functions
| Function | Args | Return cols | Source |
|---|---|---|---|
| `otlp_serve([uri], <named...>)` | optional VARCHAR + 13 named params | 11 | `otlp_start_stop.cpp:197` (TableFunctionSet w/ 1-arg + 0-arg overloads, lines 213/215) |
| `otlp_stop(uri)` | VARCHAR | 1 (`status`) | `otlp_start_stop.cpp:247` |
| `otlp_flush(uri)` | VARCHAR | 4 | `otlp_start_stop.cpp:467` |
| `otlp_server_list()` | none | 25 | `otlp_start_stop.cpp:360` |
| `otlp_seal_list()` | none | 14 | `otlp_start_stop.cpp:415` |

### Scalar function
| Function | Args | Return | Source |
|---|---|---|---|
| `otlp_uri_parser(uri)` | VARCHAR | STRUCT(host VARCHAR, port USMALLINT, ipv6 BOOLEAN, url VARCHAR) | `otlp_uri.cpp:85` |

### `otlp_serve` named parameters (type → default; validation)
Registration `otlp_start_stop.cpp:200-212`; parsing/validation `:66-143`; defaults
`src/include/otlp_server.hpp:25-68`; daemon env mapping `src/server/server_config.cpp:657-664`.

| Param | Type | Default | Validation / notes |
|---|---|---|---|
| `token` | VARCHAR | random 32-hex | `ValidateToken` len ≥ 16 (`server.hpp:135`) |
| `catalog` | VARCHAR | "" (default catalog) | empty = connection default |
| `schema` | VARCHAR | `main` (`server.hpp:30`) | must be non-empty |
| `parquet_export_path` | VARCHAR | "" | non-empty; mutually exclusive with `catalog` |
| `create_tables` | BOOLEAN | `true` | |
| `allow_other_hostname` | BOOLEAN | `false` | non-localhost rejected otherwise |
| **`max_body_bytes`** | UBIGINT | 16 MiB (`server.hpp:38`) | > 0; **not in the audit's option list — present in code & serve.md** |
| `http_threads` | UBIGINT | 0 (host default) | > 0 when set |
| `max_buffered_bytes` | UBIGINT | 512 MiB | > 0 |
| `seal_target_bytes` | UBIGINT | **128 MiB** (`server.hpp:47`) | > 0 |
| `seal_max_age_ms` | BIGINT | 5000 | > 0 |
| `target_file_size` | UBIGINT | 256 MiB | > 0 (DuckLake CHECKPOINT) |
| `maintenance_retention_ms` | BIGINT | 900000 (15 min) | > 0 (DuckLake CHECKPOINT) |

All thirteen are documented in `serve.md:34-49` with correct types and defaults. The audit
brief's list omitted `max_body_bytes`; it is a real public param. No code/doc param drift on
`otlp_serve` other than API-005 below.

### User-reachable error strings (1.0 contract)
- Bind/input: `"No files found that match the pattern \"%s\""` (`read_otlp.cpp:96`);
  `"File %s is too large ... Maximum supported size is 100MB."` (`read_otlp.cpp:254`);
  `"Invalid OTLP listen URI specified"` (`otlp_start_stop.cpp:52,224,431`);
  `"Only localhost is allowed ... set allow_other_hostname=true"` (`:62`);
  `"<param> must be greater than zero"` / `"must not be empty"` family (`:81-142`);
  `"parquet_export_path cannot be combined with a catalog target..."` (`:93`).
- URI parse: `"Invalid OTLP listen URI, needs to start with 'otlp:'"` (`otlp_uri.cpp:33`);
  `"Invalid OTLP listen port"` (`:11,17`); IPv6 `"missing ']'"` / `"Missing IPv6 address"` (`:41,47`).
- Placeholders: `"Summary metrics not yet supported. Use read_otlp_metrics_gauge()..."`
  (`read_otlp.cpp:145`); `"read_otlp_metrics() is not supported yet ..."` (`read_otlp.cpp:154`).
- HTTP (server contract): 413 oversize (`otlp_server.cpp:506`), 415 content-type/encoding
  (`:220,511`), 503 admission full (`:523`). Matches `serve.md:246-253`.

---

## Findings

### API-001 — `status_status_message` doubled-word column (traces)
- **Location:** `external/otlp2records/src/schema/arrow.rs:45`; mirrored `defs.rs:98`,
  `schemas.md:36`; also lands in the live-ingest `otlp_traces` table (`otlp_server.cpp:242-251`
  derives table columns from the same `otlp_get_schema`).
- **Category:** schema naming / freeze risk.
- **Severity:** high (high-if-done-now / impossible-later). **Confidence:** high.
- **What & why:** The OTLP field is `Status.message`; the column is named
  `status_status_message` — the word "status" is doubled. It is queryable as-is and not a
  bug, but it is an obvious wart that 1.0 will inherit on *both* the file-read schema and the
  server table schema. Renaming a frozen column is a breaking change after 1.0.
- **Action:** Rename to `status_message` in `arrow.rs:45` (+ `defs.rs:98`), `schemas.md:36`,
  any test referencing it. One-line change now; do it before the freeze.

### API-002 — `duration_time_unix_nano` is a Duration mislabeled as a timestamp
- **Location:** `arrow.rs:34` (`duration_ns("duration_time_unix_nano", false)`), type mapped
  to BIGINT at `src/otlp_arrow.cpp:31-39`; mirrored `defs.rs:87`, `schemas.md:25`. Also in the
  live `otlp_traces` table.
- **Category:** schema naming / semantics / freeze risk.
- **Severity:** high (impossible-later). **Confidence:** high.
- **What & why:** The column is a span *duration* in nanoseconds (Arrow `Duration(ns)` →
  DuckDB `BIGINT`, deliberately, per the well-documented note at `otlp_arrow.cpp:31-39`). The
  *name* `duration_time_unix_nano` reads like an absolute unix-nano *timestamp*, matching the
  `*_time_unix_nano` timestamp columns (`start_time_unix_nano`, `time_unix_nano`) that really
  are timestamps. This will mislead every consumer and is frozen at 1.0. The BIGINT *type*
  choice is sound; only the name is wrong.
- **Action:** Rename to `duration_nano` (or `duration_ns`) in `arrow.rs:34`, `defs.rs:87`,
  `schemas.md:25`, `schema_bridge.test:71`. Keep the BIGINT type and the existing rationale
  comment. Cheap now; breaking later.

### API-003 — `AGENTS.md` column counts are wrong
- **Location:** `AGENTS.md:118-123` and `AGENTS.md:155-158`.
- **Category:** doc drift. **Severity:** high (it is the agent-facing source of truth).
  **Confidence:** high (counts measured directly from `arrow.rs`).
- **What & why:** AGENTS.md advertises traces 25, logs 15, gauge 16, sum 18. Real counts
  (field-counted in `arrow.rs`): traces **24**, logs **18**, gauge **17**, sum **19**.
  Histogram 22 and exp_histogram 27 are correct. `schemas.md`/`api.md` already carry the
  correct numbers, so AGENTS.md is the lone disagreement — and it is the file agents read
  first.
- **Action:** Fix both blocks in `AGENTS.md` to 24/18/17/19/22/27 (and the "Schema Design"
  prose at `:155-158`).

### API-004 — `defs.rs` is an unreachable second schema source of truth, ungated
- **Location:** `external/otlp2records/src/schema/defs.rs` (whole file);
  re-exported `schema/mod.rs:14`, `lib.rs:67-68`.
- **Category:** schema integrity / maintainability. **Severity:** high. **Confidence:** high.
- **What & why:** `defs.rs` defines all six schemas a *second* time (`SchemaField{required}`
  + `FieldType`). The only consumers of `schema_def`/`schema_defs` are the `pub use`
  re-exports — **no FFI entry, no C++ caller, no daemon path, no test** uses them (verified by
  grep across `src/`, `bindings/ffi.rs`, `bindings/wasm.rs`, all tests). The shipped surface
  gets schemas exclusively from `arrow.rs` via `otlp_get_schema` (`ffi.rs:138-165`). I did a
  full field-by-field diff: the two definitions currently **agree on every name, order, and
  nullability** (mapping `required = !nullable`) — so the orientation lead that
  `service_name`/timestamp nullability "disagree in spirit" is **not borne out**. The risk is
  that nothing keeps them in sync: a future edit to `arrow.rs` will silently leave `defs.rs`
  stale, and any downstream Rust user of `schema_defs()` (it is `pub`) would get a divergent
  answer. There is no test asserting `arrow == defs`. `defs.rs` also lacks any `#[cfg(test)]`.
- **Action:** Pick one. Either (a) delete `defs.rs` + its `pub use` (preferred — it is dead on
  the shipped surface), or (b) if some external "schema inspection" consumer needs it, add a
  Rust unit test that asserts each `schema_def(name)` matches the corresponding `arrow.rs`
  schema field-for-field, so drift becomes a build failure.

### API-005 — `api.md` quotes a stale "~64 MiB" seal trigger
- **Location:** `api.md:52` ("...when admitted request-body bytes reach about 64 MiB").
- **Category:** doc drift. **Severity:** medium. **Confidence:** high.
- **What & why:** The default `seal_target_bytes` is **128 MiB** (`otlp_server.hpp:47`;
  daemon default `server_config.cpp:660`). `serve.md` already says 128 MiB in two places
  (`serve.md:46`, `serve.md:264`); only `api.md` carries the old 64 MiB number.
- **Action:** Change `api.md:52` "about 64 MiB" → "about 128 MiB".

### API-006 — `schemas.md` types histogram bucket lists as VARCHAR; readers return typed lists
- **Location:** `schemas.md:128-129` (`bucket_counts`/`explicit_bounds` as `VARCHAR`,
  "JSON array") and `schemas.md:161,163` (`positive_bucket_counts`/`negative_bucket_counts`
  as `VARCHAR`). Real types: `arrow.rs:117-118` `u64_list`/`f64_list`, mapped to DuckDB
  `LIST(...)` at `otlp_arrow.cpp:96-101`.
- **Category:** schema doc drift (type). **Severity:** medium. **Confidence:** high
  (pinned by test `read_otlp_metrics_histogram.test:39-60`: `typeof(bucket_counts)` = `BIGINT[]`,
  `explicit_bounds` renders as `[5.0, 10.0, ...]`).
- **What & why:** The file readers return these as DuckDB native arrays (`BIGINT[]` /
  `DOUBLE[]`), not JSON-string VARCHAR. A user reading `schemas.md` would write
  `json_extract(bucket_counts, ...)` and fail; the correct usage is array indexing /
  `unnest`. (Note: the *other* JSON-shaped columns — `resource_attributes`, `events_json`,
  `exemplars_json`, etc. — genuinely are VARCHAR JSON strings; only the four bucket/bound
  list columns are mistyped in the doc.)
- **Action:** In `schemas.md`, change the four list columns to type `BIGINT[]` (bucket counts)
  / `DOUBLE[]` (explicit bounds) and update the descriptions to "array of integers/floats"
  rather than "JSON array". Optionally note the live-ingest tables keep the same list types.

### API-007 — `otlp_uri_parser` registered but undocumented
- **Location:** registered `otlp_uri.cpp:85`, `otlp_extension.cpp:30`. Grep of `site/`,
  `AGENTS.md`, `README.md` finds zero references.
- **Category:** API surface / doc gap. **Severity:** medium. **Confidence:** high.
- **What & why:** This is a user-callable public scalar returning
  `STRUCT(host, port, ipv6, url)`. If it ships at 1.0 it is a frozen function with a frozen
  return struct, yet it appears in no reference doc — so its existence and shape are
  undocumented contract. (It is the same parser `otlp_serve` uses, so it has real utility for
  validating URIs.)
- **Action:** Either document it (a short row in `api.md`/`serve.md` URI-scheme section with
  its return struct) or, if it is meant to be internal-only, drop the registration before
  1.0. Decide explicitly; do not freeze it undocumented.

### API-008 — Same column name, two types: `TIMESTAMP_NS` (files) vs `TIMESTAMP` (live tables)
- **Location:** file readers use default `OtlpArrowSchemaOptions` (`timestamp_ns_as_timestamp
  = false`, `otlp_arrow.hpp:11`) → `TIMESTAMP_NS` (`otlp_arrow.cpp:24-29`). Live-ingest tables
  set `timestamp_ns_as_timestamp = true` (`otlp_server.cpp:249-251`) → `TIMESTAMP` (µs).
- **Category:** schema semantics / cross-surface consistency. **Severity:** medium.
  **Confidence:** high.
- **What & why:** Columns like `time_unix_nano` / `start_time_unix_nano` are `TIMESTAMP_NS`
  (full nanosecond) when read from a file, but `TIMESTAMP` (microsecond, **lossy** — ns
  truncated, see the `/1000` at `otlp_arrow.cpp:183`) when queried from a live-ingest table,
  under the *same column name*. This is a deliberate catalog-compatibility choice and is
  documented in one spot (`schemas.md:180`), but it is a genuine cross-surface footgun at
  1.0: a query/JOIN that mixes file reads and live tables sees two types and loses ns
  precision on the live side. `api.md`/`serve.md` do not mention the precision loss.
- **Action:** No code change required (the trade-off is sound). Strengthen the docs: cross-link
  the `schemas.md:180` note from `serve.md` (target-table section) and `api.md`, and state
  explicitly that the live path truncates ns→µs. Confirm this is the intended 1.0 contract.

### API-009 — `api.md` omits `otlp_seal_list`
- **Location:** `api.md:54-57` lists `otlp_serve`/`otlp_flush`/`otlp_stop`/`otlp_server_list`
  but not `otlp_seal_list`. It IS registered (`otlp_extension.cpp:28`,
  `otlp_start_stop.cpp:415`) and IS documented in `serve.md:21,154`.
- **Category:** doc drift. **Severity:** nit. **Confidence:** high.
- **Action:** Add an `otlp_seal_list()` bullet to `api.md`'s Live Ingest list (one line,
  link to `serve.md`).

### API-010 — `serve.md` undercounts the function surface
- **Location:** `serve.md:13` "registers four lifecycle functions and one diagnostic
  function".
- **Category:** doc nit. **Severity:** nit. **Confidence:** high.
- **What & why:** The table immediately below lists five (`serve`, `flush`, `stop`,
  `server_list`, `seal_list`), and the extension also ships the `otlp_uri_parser` scalar. The
  prose count is internally inconsistent with its own table.
- **Action:** Reword to "five server functions" (and reference `otlp_uri_parser` if API-007 is
  resolved by documenting it).

### API-011 — Union/summary placeholder naming asymmetry (decision before freeze)
- **Location:** `read_otlp.cpp:407` (`read_otlp_metrics`), `:426` (`read_otlp_metrics_summary`).
- **Category:** API design / freeze decision. **Severity:** nit. **Confidence:** medium.
- **What & why:** Both placeholders correctly throw `NotImplementedException` on bind with
  clear, actionable messages (verified by `read_otlp_basic.test:78-87`), and there is **no**
  reachable summary stub in the shipped core — the OTAP encoder in
  `batch/otap.rs` (`SummaryDataPointBuilders`, `otap_summary_data_points_schema_arc`,
  `METRIC_SUMMARY`) is NOT wired to `otlp_transform`/`otlp_transform_metrics_all`
  (`ffi.rs:285`); the standard metrics path explicitly drops summaries as
  `skipped.summaries` (`batch/metrics.rs:289-290`). So failure is clean. The only open
  question is whether registering two functions that *always throw* is the shape you want to
  freeze at 1.0 (vs not registering them and letting "function not found" surface). Registered
  placeholders give a better error, so this is likely fine — but it is a deliberate 1.0
  decision worth recording.
- **Action:** Confirm intent to keep both as throwing placeholders at 1.0; if kept, ensure the
  `schemas.md:16` / `api.md:48` "registered placeholders" note stays. No code change needed.

---

## What I verified clean (coverage, not just spot checks)
- **arrow.rs vs defs.rs vs schemas.md:** full field-by-field diff — names, order, nullability
  all agree between the two Rust defs; `schemas.md` column names/order match `arrow.rs`
  exactly for all six schemas.
- **No always-null phantom columns:** `event_name`/`flags` (logs), `trace_state`/`flags`
  (traces), `flags`/`exemplars_json` (all metrics) are genuinely populated from OTLP fields
  (`batch/logs.rs:288-291`, `batch/traces.rs:299-352`, `batch/metrics.rs:673-676,802-804`).
- **snake_case:** every column in all six schemas is snake_case; the only naming defects are
  API-001 (doubled word) and API-002 (misleading-but-still-snake_case). No camelCase leaks.
- **`otlp_serve` params:** all 13 named params parse, validate (`> 0` / non-empty), and are
  documented in `serve.md` with correct types and defaults matching `otlp_server.hpp` and the
  daemon env defaults in `server_config.cpp`. No UBIGINT/BIGINT inconsistency beyond the
  intentional signed-age vs unsigned-bytes split (age knobs BIGINT, byte knobs UBIGINT).
- **Placeholders fail on bind**, not mysteriously, with shape-specific guidance.
- **HTTP status contract** (413/415/503) in `otlp_server.cpp` matches `serve.md`/`api.md`.
