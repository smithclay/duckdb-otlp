# Reviewer A — Ingest correctness, durability, concurrency (the seal/group-commit path)

Charter: the buffered group-commit ("seal") ingest path — `src/otlp_server.cpp`,
`src/otlp_server_http.cpp`, `src/otlp_storage.cpp`, `src/otlp_start_stop.cpp`,
`src/server/main.cpp`, the two manual harnesses, and `test/sql/otlp_serve.test`.

**Summary.** The seal path is well built and the single-writer invariant holds: only the
sealer thread and `FlushNow` (serialized by `writer_mutex`) ever commit, lock ordering is
consistent (`buf.mutex` → `admission_mutex`; seal takes all `buf.mutex` then
`admission_mutex`), the buffer-swap-under-all-locks is correct, and the re-buffer-on-failure
ordering preserves rows and does not double-release admission. The durability hazard ("a
plain DB/connection close drops buffered rows") is surfaced honestly in `ShutdownIngest`'s
comment + WARNING log + `reference/serve.md`. The two issues I'd hold for 1.0 are both
*resource-bound* rather than correctness defects: there is **no bound at all on the decoded
columnar heap** (admission only counts input bytes), so a stuck/slow seal under sustained
ingest can OOM the process while `otlp_server_list` still shows admission headroom (CON-001);
and a persistently-failing seal keeps returning `202 Accepted` and accumulating until the
*input-byte* admission cap trips, at which point the only operator signal is
`seal_failures_total` polled by the daemon every ~1 s and logged to stderr — `/readyz` stays
green (CON-002). Everything else is medium/nit: a size-trigger seal can be delayed up to the
poll interval on a lost `notify_one` (CON-003, latency only), the parquet at-least-once path
can duplicate rows *within* a single signal whose `COPY` fails mid-write (CON-004, documented
but worth a sharper code comment), and several smaller observations below.

No blockers. Two highs (CON-001, CON-002).

---

## CON-001 — Decoded columnar heap is unbounded; admission only caps input bytes

- **Location:** `src/otlp_server.cpp:402` (`TryReserveAdmission`), `:670` (`BufferAppend`),
  `:273`/`:732` (`make_uniq<ColumnDataCollection>(allocator, ...)`); config field
  `src/include/otlp_server.hpp:63-67` (`max_buffered_bytes`).
- **Category:** durability / resource exhaustion
- **Severity:** high
- **Confidence:** high

`max_buffered_bytes` bounds `admitted_bytes`, which reserves `MaxValue<idx_t>(body.size(),
1024)` of *input* bytes per request (`:519`) and is only released at seal (`ReleaseAdmission`,
`:889`/`:915`/`:1013`). The actual buffered rows live in per-signal `ColumnDataCollection`s
allocated from `Allocator::Get(*db)` (`:273`, `:659`, `:732`) — the raw DuckDB allocator, not
the buffer-managed pool, so even DuckDB's `memory_limit` does not bound it. Nothing anywhere
counts decoded heap or buffered-row size against any cap (confirmed: the only size signal in
the file is `buffered_rows`, used purely for telemetry/age, never for backpressure).

I confirmed via `httplib.hpp:7899` that a *single* request's body — compressed or
decompressed — is bounded to `max_body_bytes` (the receiver rejects with 413 once
`req.body.size() + n > payload_max_length_`), so this is **not** an unbounded single-request
compression bomb. The real vector is the OTLP→Arrow expansion ratio multiplied by the number
of admitted-but-unsealed requests: a 16 MiB protobuf dense in tiny attribute maps decodes to
substantially more columnar heap, and under a stuck or merely slow seal (large
`seal_target_bytes`, slow object store) up to `max_buffered_bytes` / `body_size` such requests
can be resident at once. The process can OOM while `otlp_server_list.admitted_bytes` still
shows headroom — i.e. the operator's only memory gauge is measuring the wrong quantity. The
header comment at `:63-67` is honest that this is "an admission/throughput proxy, not a precise
memory bound," but there is no second bound to fall back on.

- **Proposed minimal action:** add a second admission gate on a *decoded-bytes* counter.
  `ColumnDataCollection::SizeInBytes()` (or `AllocationSize()`) is available; accumulate the
  appended chunk's allocation size in `BufferAppend` under `buf.mutex` into a new atomic, check
  it against a new `max_buffered_decoded_bytes` (or reuse `max_buffered_bytes` as a heap cap and
  rename the input-byte counter), and reset it at seal alongside `sealed_admission_bytes`. If a
  full second counter is too much for 1.0, at minimum surface a `buffered_bytes`/heap estimate
  column in `otlp_server_list` so the OOM risk is observable.

---

## CON-002 — Failing seal silently keeps accepting 202s; only signal is a stderr WARNING + a counter

- **Location:** `src/otlp_server.cpp:1137` (`SealerLoop`, 250 ms back-off on throw),
  `:855`/`:950` (re-buffer on failure), `src/otlp_server_http.cpp:115-117` (`/readyz` static
  200), `src/server/main.cpp:138-175` (`WaitForShutdownOrListenerFailure`).
- **Category:** durability / observability
- **Severity:** high
- **Confidence:** high

When a seal fails, both backends re-buffer the un-committed rows (catalog: full rollback +
restore, `:950-1010`; parquet: restore only the un-exported signals, `:855-913`) and the sealer
backs off 250 ms and retries on the next age/size trigger. Ingest is unaffected: POSTs keep
returning `202 {"status":"buffered"}` and `admitted_bytes` keeps climbing because the failed
seal does **not** release admission for the rebuffered share. So a misconfigured credential or a
down object store produces a server that accepts data indefinitely, never commits it, and only
stops accepting once cumulative *input* bytes hit `max_buffered_bytes` and start returning 503
(and, per CON-001, may OOM on decoded heap first). This is correct *as designed* (transient
outages should not crash the daemon), and the re-buffer/rollback logic itself is sound — I
checked the admission accounting on both failure paths and it neither leaks nor double-releases
(`restored_admission` split at `:879-887`, only the exported share released at `:889`).

The honesty gap is in surfacing. `/readyz` (`otlp_server_http.cpp:115`) returns a static 200
regardless of seal health, so a load balancer / orchestrator sees the daemon as healthy while
nothing is durable. The daemon's `WaitForShutdownOrListenerFailure` (`main.cpp:164-172`) is the
only active surfacing: it polls `seal_failures_total` every ~1 s (`ticks % 4`) and prints
`WARNING: buffered rows are not committing ...` to **stderr** — easily lost in a container, and
it does not change the process's health/exit state. `last_seal_age_ms` correctly keeps growing
on a stuck catalog seal (the throw at `:1010` precedes the `last_seal_unix_ms.store` at `:1016`),
so the data to build a real readiness signal exists; it just isn't wired into `/readyz`.

- **Proposed minimal action:** make `/readyz` degrade when seals are stuck — e.g. return 503
  when `seal_failures_total` has advanced and `LastSealAgeMs()` exceeds a multiple of
  `seal_max_age_ms` (a healthy idle server with nothing buffered must still pass). That single
  change converts a silent stderr line into an actionable orchestrator signal. Keep `/healthz`
  liveness-only.

---

## CON-003 — Size-triggered seal can be delayed up to the poll interval on a lost notify

- **Location:** `src/otlp_server.cpp:692-695` (`RequestSeal`), `:1137-1145` (`SealerLoop` wait).
- **Category:** concurrency (lost wakeup)
- **Severity:** nit
- **Confidence:** high

`RequestSeal` sets `flush_requested.store(true)` and calls `sealer_cv.notify_one()` **without**
holding `sealer_mutex`. The classic lost-wakeup window exists: if the store+notify lands after
the sealer evaluates the `wait_for` predicate but before it blocks, the notification is lost.
This is *not* a lost seal — `flush_requested` is checked by the predicate
(`:1145`), so the very next `wait_for` re-entry sees it, and in the worst case the age trigger
(`SealAgeDue`, `:1165`) catches it regardless. The only consequence is a size-triggered seal
delayed by up to `poll_ms` (floored at 50 ms, capped at min(`seal_max_age_ms`, 1000)). Worth
noting only because it makes the size trigger's latency bound `poll_ms`, not immediate.

- **Proposed minimal action:** none required for correctness. If you want the size trigger to be
  prompt, take `sealer_mutex` around the `flush_requested` store in `RequestSeal` (move the
  store under the lock, keep `notify_one` after release), which closes the window. Optional.

---

## CON-004 — Parquet at-least-once can duplicate rows *within* a signal whose COPY fails mid-write

- **Location:** `src/otlp_server.cpp:771-853` (parquet seal loop), comment at `:776-779`.
- **Category:** durability semantics
- **Severity:** medium
- **Confidence:** high

The parquet path is correct *across* signals — an already-exported signal is marked
`exported[i] = true` (`:835`) and never re-buffered (`:866`), so no cross-signal duplication, and
`committed_rows_total` is advanced by `exported_rows` even on overall failure (`:900`), which
matches the on-disk truth and is documented (`reference/serve.md:156`,
`otlp_server.hpp:84-101`). The remaining hole is *within* a single signal: `BuildParquetExportSql`
emits a `COPY ... (FORMAT PARQUET, PARTITION_BY (...), APPEND, FILENAME_PATTERN
'seal_{uuidv7}')` (`:113-130`). A partitioned COPY can write several partition files; if it
fails after writing some of them, that signal is *not* marked exported, so its entire collection
is re-buffered (`:866-875`) and re-COPY'd on the next seal — re-writing the rows whose partition
files already landed. Result: duplicate rows for that one signal. The code comment at `:776-779`
("a single signal whose COPY fails mid-write can still be re-exported on retry, so downstream
readers must tolerate duplicate rows") acknowledges this, and `reference/serve.md:179` tells
users to dedupe. So this is a documented property, not a hidden defect. I flag it because the
`exported[]` bookkeeping reads as if it gives per-signal atomicity, which it does not — a future
edit could easily assume exactly-once per signal.

- **Proposed minimal action:** none functional (it is an accepted at-least-once contract). Tighten
  the comment at `:835` to state explicitly that `exported[i]=true` means "COPY returned success
  for this signal," and that a COPY that throws may already have written partition files, so the
  re-buffer + retry is at-least-once at signal granularity, not exactly-once.

---

## CON-005 — `ShutdownIngest` 3-attempt final drain is correct; verified, with one caveat

- **Location:** `src/otlp_server.cpp:1190-1224` (`ShutdownIngest`), `:710-717` (`SealOnce` db-lock
  guard), `src/otlp_server_http.cpp:207-220` (`Close`).
- **Category:** durability / shutdown
- **Severity:** medium
- **Confidence:** high

The drain loop `for (int attempt = 0; attempt < 3 && BufferedRows() > 0; attempt++)` (`:1205`) is
correct for the intended cases. Trace verified:

1. `otlp_stop` → `StopServer` (erase under `servers_mutex`, then `Close()`) → `Close` joins the
   listener, then `ShutdownIngest` while the DB is still alive → `SealOnce` runs and commits. The
   3-attempt loop only re-runs on a `SealOnce` *throw*; on success `BufferedRows()` is 0 and the
   loop exits after one pass. Good.
2. `otlp_flush` → `FlushServer` (holds a `shared_ptr` ref across the seal with `servers_mutex`
   released, `otlp_storage.cpp:63-87`) → `FlushNow` → `SealOnce(false)`. Serialized against the
   sealer and against `ShutdownIngest` by `writer_mutex` (`:711`). A `FlushNow` racing a
   concurrent `otlp_stop` is safe: if `ShutdownIngest` has already reset `writer_con`, `SealOnce`
   sees `!writer_con` and returns an empty result (`:712`). Confirmed.
3. Implicit DB teardown: `~OtlpStorageExtensionInfo` → `StopAllServers` → `Close` →
   `ShutdownIngest`, but by then `db_ptr.lock()` returns null, so `SealOnce` no-ops at `:715-717`,
   `BufferedRows() > 0` stays true for all 3 attempts, and the WARNING at `:1213-1218` ("dropping
   N buffered rows on shutdown ...") fires. This is the honestly-surfaced data-loss path.

**Caveat worth noting (not a bug):** the 3-attempt cap means that if a *real* seal keeps failing
(e.g. object store still down) during a graceful `otlp_stop`, the drain gives up after 3 throws
and drops the rows with the same WARNING, even though the DB is alive. That is a deliberate
bounded-shutdown choice, but the WARNING text ("database closed without graceful otlp_stop, or
repeated seal failure") conflates the two causes. An operator who *did* call `otlp_stop` and lost
rows to a backend outage will be told they didn't call `otlp_stop`.

- **Proposed minimal action:** none required. Optionally, when `db_ptr` is still live but rows
  remain after 3 attempts, log the seal error (from `seal_last_error`) rather than the generic
  "closed without otlp_stop" text, so the two loss causes are distinguishable.

---

## CON-006 — Single-writer / second-writer invariant holds across all catalog touch points

- **Location:** `src/otlp_server.cpp:437-456` (`EnsureTargetTables`), `:846-849` (lazy parquet
  view), `:1031-1104` (`MaybeRunCatalogMaintenance`), `:1106-1135`
  (`ConfigureCatalogMaintenanceOptions`), constructor `otlp_server_http.cpp:77-94`.
- **Category:** concurrency (write serialization)
- **Severity:** medium (confirmation — no defect found)
- **Confidence:** high

I verified the "exactly one concurrent catalog writer" invariant that avoids DuckLake
optimistic-concurrency conflicts and tiny-file churn:

- `EnsureTargetTables` (`:437`) runs a *throwaway* `Connection con(*db)` (`:451`) — but it runs in
  the constructor, *before* `writer_con` is created (`otlp_server_http.cpp:88`) and *before*
  `StartSealer()` (`:94`), single-threaded. No overlap with the sealer.
- The lazy parquet inspection view (`CREATE VIEW IF NOT EXISTS`, `:848`) and the staging temp
  tables (`:809-812`) all run on `*writer_con`, inside `SealOnce` which holds `writer_mutex` from
  `:711`. Good.
- `MaybeRunCatalogMaintenance`'s `CHECKPOINT` (`:1069`) is called only from `SealOnce` (`:1026`)
  under the same `writer_mutex`, and on `*writer_con`. Good.
- `ConfigureCatalogMaintenanceOptions`' `CALL ...set_option(...)` (`:1120-1124`) runs on
  `*writer_con` in the constructor, single-threaded, before the sealer starts. Good.

No second concurrent writer exists on any path. The only cross-connection sequence is
construction-time CREATE TABLE (throwaway connection) followed by sealer Appends (`writer_con`),
which are strictly sequential, not concurrent. Recorded as a positive confirmation.

---

## CON-007 — Rescan guards on the side-effecting table functions are correct

- **Location:** `src/otlp_start_stop.cpp:171-195` (`OtlpServe`), `:232-245` (`OtlpStop`),
  `:445-465` (`OtlpFlush`), `:312-358`/`:385-413` (list functions).
- **Category:** correctness (rescan safety)
- **Severity:** nit (confirmation)
- **Confidence:** high

All three side-effecting functions guard with `if (bind_data.finished) return;` and set
`finished = true` at the right point:

- `OtlpServe` sets `finished = true` *immediately after* `CreateServer` succeeds (`:181`), before
  any `SetValue`, with an explicit comment that a `SetValue` throw must not re-create the running
  server. Correct. `CreateServer` (`otlp_storage.cpp:26-42`) constructs the `OtlpServer` *before*
  `servers.emplace` (`:37` then `:39`), so a bind failure in the constructor throws before
  registration — no orphan in the map, and the constructor's own catch
  (`otlp_server_http.cpp:191-194`) stops the just-started sealer. Clean.
- `OtlpStop` and `OtlpFlush` set `finished = true` after the side effect + `SetValue`. A `SetValue`
  throw there would allow a re-scan to re-stop / re-flush, but both are idempotent (`StopServer`
  returns false → "no server found"; `FlushNow` just seals whatever is buffered). Acceptable.
- `OtlpServerList` / `OtlpSealList` are read-only with `initialized`/`offset` paging — safe under
  rescan, and snapshot-once semantics avoid mid-scan registry mutation.

No defect.

---

## CON-008 — `RecordSealEvent` increment-then-snapshot order matches the header invariant

- **Location:** `src/otlp_server.cpp:325-348` (`RecordSealEvent`), success/failure call sites
  `:910`/`:925`/`:1009`/`:1023`; invariant doc `src/include/otlp_server.hpp:94-101`.
- **Category:** observability correctness
- **Severity:** nit (confirmation)
- **Confidence:** high

`OtlpSealEvent` denormalizes the running totals (`seals_total`, `seal_failures_total`,
`committed_rows_total`) and the header warns the increment-then-snapshot order in `SealOnce` must
be preserved or the per-seal cumulative tallies are off by one. Verified all four call sites
increment the relevant atomics *before* calling `RecordSealEvent`: parquet failure increments
`seal_failures_total` (`:895`) and `committed_rows_total` (`:900`) before `:910`; parquet success
increments `seals_total`/`committed_rows_total` (`:916-917`) before `:925`; catalog failure
increments `seal_failures_total` (`:1003`) before `:1009`; catalog success increments
(`:1014-1015`) before `:1023`. Consistent. No defect; flagged so the invariant stays on the
review record.

---

## CON-009 — `seal_history` / `ListSeals` are bounded but copy all events under the registry lock

- **Location:** `src/otlp_server.cpp:320-323` (`SealHistory`), `:342-347` (deque cap),
  `src/otlp_storage.cpp:146-162` (`ListSeals`).
- **Category:** observability / lock-hold time
- **Severity:** nit
- **Confidence:** med

`seal_history` is correctly bounded to `MAX_SEAL_HISTORY = 4096` per server with a `pop_front`
when full (`:343-346`). `ListSeals` (`otlp_storage.cpp:146`) holds `servers_mutex` while calling
`SealHistory()` on every server, which copies up to 4096 events per server (`SealHistory` itself
takes `seal_history_mutex` and returns a full vector copy). For many servers this is a copy of
N×4096 `OtlpSealEvent`s with the global registry lock held, briefly blocking
`otlp_serve`/`otlp_stop`/`otlp_flush`/`otlp_server_list`. Not a correctness issue and unlikely to
matter at realistic server counts (the daemon runs one server), but `ListServers`
(`otlp_storage.cpp:105`) deliberately copies snapshots cheaply under the lock whereas `ListSeals`
does the heavy copy under it.

- **Proposed minimal action:** optional — collect the `shared_ptr`s under `servers_mutex`, release
  it, then gather `SealHistory()` per server outside the registry lock (mirrors the pattern
  `FlushServer` already uses). Low priority.

---

## CON-010 — `otlp_flush`/`otlp_stop` final-seal contract is verified by manual harnesses, not `make test`

- **Location:** `test/manual/otlp_serve_concurrency.py` (stop-under-load `:348`, flush/stop race
  `:392`, backpressure `:269`), `test/manual/otlp_serve_parquet.py` (failure recovery `:175`),
  `test/sql/otlp_serve.test:5-12`.
- **Category:** test coverage
- **Severity:** medium
- **Confidence:** high

The seal/durability invariants this charter cares about are exercised *only* by the two manual
harnesses, which are explicitly outside `make test` (SQLLogicTest cannot drive HTTP). The
harnesses are good: `scenario_stop_under_load` and `scenario_flush_stop_race` assert
`table_rows == accepted_rows` (no loss across a stop racing in-flight POSTs and a concurrent
flush/stop), `scenario_backpressure` asserts both 202s and 503s occur and the committed count
matches accepted, and `otlp_serve_parquet.py:scenario_failure_recovery` asserts a COPY failure
re-buffers (no loss) and drains on recovery. What is **not** covered by any automated gate: these
harnesses do not run in CI (per the docker-smoke memory note the publish gate is the minimal
`scripts/smoke_test.py`), so a regression in the seal/re-buffer path would not be caught by
`make test` or the PR gate. The cross-signal no-duplication property is explicitly noted as
requiring in-code fault injection that the harness cannot do (`otlp_serve_parquet.py:21-23`).

- **Proposed minimal action:** none code-level. Recommend (for the hardening milestone, not 1.0
  blocker) wiring `otlp_serve_concurrency.py` into a manual/opt-in CI job — ideally under
  TSan/ASan as `test/sql/otlp_serve.test:12` already suggests — so the seal path has *some*
  automated regression signal beyond the smoke test. Flagged as a coverage gap, not a code defect.
