# Competitive Positioning — duckdb-otlp before 1.0

*Lens: where the single-binary, agent-first "ingest + store + serve + query in one process"
thesis is and is not competitive, measured against the decoupled pipelined-object-store
design typified by OpenData's "Ingesting 1Gbps of logs into ClickHouse for $180/month"
([opendata.dev](https://www.opendata.dev/blog/ingesting-1gbps-logs-to-clickhouse/)).*

This memo is grounded in the actual ingest path: `src/otlp_server.cpp`
(`SealOnce`, `SealerLoop`, `TryReserveAdmission`, `MaybeRunCatalogMaintenance`),
`src/include/otlp_server.hpp` (`OtlpServerConfig`), and `src/otlp_server_http.cpp` (the
`202 Accepted` / `{"status":"buffered"}` contract). The foil's numbers (1.1 Gbps
compressed, p50 ~2.8 s / p99 ~10 s, ~$180/mo S3, ~1.3 16 MB batches per CAS, at-least-once
with a `(source, sink, low_sequence, high_sequence, schema_version)` batch identity) are
taken as given for reference only — the point is to locate *us*, not to grade them.

## 1. Where the single-binary thesis genuinely wins

The strongest, least-arguable win is **operational surface area**. The foil's 1.1 Gbps
result is a *two-node, two-program* system: an OTel-gateway-side producer (accumulator +
encode/compress pool + upload pool + a CAS-committing thread) and a separate
ClickHouse-ingestor node (fetch pool + decode pool + sink writer + an ack coordinator that
advances offsets only over gap-free ranges). That is a producer fleet, a consumer fleet, an
object-store manifest protocol, and an offset/ack state machine to operate — even though it
correctly replaces Kafka with S3. duckdb-otlp collapses all of that into one static binary:
the daemon (`src/server/main.cpp`) embeds DuckDB, statically links the OTLP extension, runs
`otlp_serve`, and seals directly into a DuckLake/Parquet catalog. There is no Kafka, no
separate consumer process, no manifest CAS protocol, and no offset bookkeeping to reconcile.
For the agent-first / small-team / single-tenant deployment that is the project's target,
"one container, one port, one volume" is a real and defensible advantage.

Second, **single-node local query performance and time-to-first-query**. Because the same
process that ingests can also query, there is no hop between "data landed" and "data
queryable." `read_otlp_*` reads files directly, and the seal lands canonical
columnar Parquet (snake_case schemas inspired by the ClickHouse exporter) that DuckDB
queries natively with no ETL, no schema registry round-trip, and no second engine to stand
up. The foil must traverse producer → S3 → consumer → ClickHouse before a row is queryable;
its own p50 ~2.8 s / p99 ~10 s end-to-end latency is the price of that decoupling. Our
analogous latency floor is `seal_max_age_ms` (default **5 s**, `otlp_server.hpp:48`) plus
append time — comparable, but with *zero* moving parts between accept and query, and an
`otlp_flush` escape hatch that forces an immediate synchronous seal when an interactive
caller wants fresh rows now.

Third, **the SQL/MCP/Quack surface is the product, not an afterthought**. The foil's output
is rows in ClickHouse; the query and agent experience is somebody else's problem. Here the
durable store *is* a DuckDB-queryable catalog, and Quack exposes that connection as a remote
SQL/admin endpoint (`DUCKDB_QUACK_ENABLED=1`). An agent or operator can ingest, flush, and
then immediately `SELECT trace_id, span_name ...` against the very same instance over one
protocol. That "query+store+serve in one" loop — accept OTLP, seal to Parquet, answer
SQL/MCP against it — is the differentiated capability, and it is structurally unavailable to
a design that treats the sink as a terminal write target.

## 2. Where the decoupled pipelined-object-store approach wins

Be honest about the inverse. The foil wins decisively on **raw sustained throughput**. It
sustains 1.1 Gbps *compressed* on 4 vCPU + 8 GB nodes by keeping many large (16 MB) S3
uploads in flight at once across a dedicated encode/compress pool and a dedicated upload
pool — it is built to saturate S3's aggregate bandwidth despite per-request latency. Our own
measured envelope is far lower: ~100k logs/s for 5 minutes on a c7g.xlarge is the
clean/sustainable point per the project's benchmarking, roughly the same *record* rate the
foil drives into a single gateway pod (175k/s) but well short of its Gbps-class aggregate.

It also wins on the structural properties that flow from decoupling:

- **HA and failure isolation.** Producer and consumer fail independently; S3 is the durable
  buffer between them, so a consumer crash loses nothing and a producer crash is bounded by
  what hadn't been committed. duckdb-otlp has a single sealer thread and a single writer
  connection (`writer_con`, serialized by `writer_mutex`); the daemon is one process and one
  failure domain.
- **Backpressure isolation.** When the foil's sink slows, S3 absorbs the backlog and the
  producer keeps accepting. Our backpressure is in-process and crude: `TryReserveAdmission`
  (`otlp_server.cpp:402`) is a single atomic CAS against `max_buffered_bytes` (default
  **512 MiB** of *admitted input bytes*, `otlp_server.hpp:67`), and once that budget is
  exhausted we reject with **503** rather than spill. There is no disk/object-store overflow
  lane — the WAL was removed on purpose (memory-as-source-of-truth), so a sustained
  producer/seal mismatch turns into shed load, not buffered load.
- **Durability semantics on accept.** The foil commits to S3 before acking. We return **202
  Accepted** with `{"status":"buffered"}` (`otlp_server_http.cpp:139–154`) the moment rows
  are parsed and buffered *in memory* — durability is the *next seal*, not the response. Per
  `AGENTS.md` "Known Limitations," `otlp_stop`/`otlp_flush` seal before returning, but a
  plain DB/connection close does **not**: `ShutdownIngest` on the implicit teardown path
  (`db_ptr` already expired) drops buffered rows (`otlp_server.cpp:1190–1218`). That is a
  weaker accept-time guarantee than a system that has already written to object storage.

## 3. The single-sealer throughput ceiling

The ceiling is architectural and the code is explicit about why it exists. All durable
writes funnel through **one sealer thread** (`SealerLoop`, `otlp_server.cpp:1137`) holding
**one writer connection** under **one `writer_mutex`** (`SealOnce` begins by taking that lock
at `:711`). A seal is **one transaction**: the catalog path does `BEGIN TRANSACTION` →
per-signal `AppendCollectionToTable` → `COMMIT` (`:930–949`), i.e. for DuckLake one Parquet
file per signal plus one snapshot per seal. The HTTP worker pool parses, converts, and
buffers concurrently into per-signal `ColumnDataCollection`s (each behind its own mutex), but
*commit is strictly serial*. Seals fire on a size trigger (`seal_target_bytes`, default
**128 MiB** of admitted input bytes, checked in `BufferAppend` at `:687`) or an age trigger
(`seal_max_age_ms`, default **5 s**, `SealAgeDue` at `:1165`).

The crucial measured fact: per the project's own profiling, **seal time is ~97%
append/encode, not COMMIT/S3**. So the ceiling is *not* DuckLake's commit latency or S3
round-trips — it is single-threaded columnar append/encode throughput. The `SealOnce` design
already removes commit serialization from the hot path by swapping each buffer for a fresh
empty `ColumnDataCollection` under the per-signal locks (`:735–762`) so workers keep filling
during a slow COPY; the bottleneck that remains is that the swapped-out collections are
appended and encoded by exactly one thread. You cannot encode faster than one core, and at
128 MiB seals every ~few seconds that one core is the wall.

**Lifting the ceiling without reintroducing small-file / snapshot amplification.** The
single serialized writer exists precisely to avoid DuckLake's optimistic-concurrency
conflicts and tiny-file churn — naive parallelism re-creates exactly that. The viable moves
keep *commit* serial while parallelizing *append/encode*:

- **Parallel encode, serial commit.** Fan the swapped-out per-signal collections (and,
  within a signal, row-group-sized slices) across an encode pool that produces finished
  Parquet *byte buffers / row groups*, then have the single sealer thread stitch them into
  **one** Parquet file per signal and do **one** transaction. This attacks the measured 97%
  (append/encode) while preserving one-file-one-snapshot-per-seal. This is the highest-value,
  lowest-risk change.
- **Bigger seals, not more seals.** Raising `seal_target_bytes` lowers snapshot/file count
  per unit of data (less amplification) at the cost of latency and memory — a knob, not a
  fix, but it moves in the safe direction.
- **Pipeline depth of one.** Allow the *next* seal's encode to begin while the *current*
  seal's COMMIT is in flight, still committing in strict sequence. Bounded depth (two)
  overlaps encode with commit without ever having two concurrent writers against the catalog.

What you must *not* do is run multiple concurrent sealers/transactions against one DuckLake
catalog: that is the tiny-file + snapshot-amplification + optimistic-conflict failure mode
the whole design was built to avoid.

## 4. Ideas worth stealing, mapped onto this codebase

**(a) CAS/seal-batching to amortize commit cost.** The foil amortizes manifest CAS over ~1.3
16 MB batches per commit. Our analogue is already partly present — one transaction already
batches all signals' rows — but each *seal* is still its own DuckLake snapshot. The steal:
make seal cadence adaptive so that under load we coalesce more buffered data into fewer,
larger transactions (raise the effective `seal_target_bytes` / relax age) and lean harder on
the existing post-seal `CHECKPOINT` maintenance (`MaybeRunCatalogMaintenance`,
`otlp_server.cpp:1031`, which already bin-packs toward `target_file_size` = 256 MiB and
expires old snapshots) to keep snapshot/file count sublinear in ingest volume. The hooks
exist; the policy is the work.

**(b) A stable batch-identity scheme for idempotent re-ingest.** This is the clearest gap.
duckdb-otlp has **no idempotency or dedup token today**, and the Parquet path is *explicitly
at-least-once* — `SealOnce`'s parquet branch documents that a COPY cannot be rolled back, so
a mid-write failure re-buffers and re-exports and "downstream readers must dedupe"
(`otlp_server.cpp:771–778`, `:896–900`; mirrored in `OtlpServerConfig::parquet_export_path`,
`otlp_server.hpp:31–36`). Worse, on a partial parquet seal we count `exported_rows` into
`committed_rows_total` even when `success=false`. Stealing the foil's deterministic identity
tuple `(source, sink, low_sequence, high_sequence, schema_version)` would let us:
attach a monotonic per-server seal sequence (we already maintain `seal_sequence` /
`seals_total`) plus a request/source identifier to each buffered batch, stamp it into the
sealed rows (or Parquet file path/metadata), and let a reader or compaction pass dedupe on
it. That converts our at-least-once Parquet path from "callers must hand-roll dedup" into "a
documented, dedupable batch key" — without changing the buffered/202 model. It also makes
producer retries (and the re-buffer-on-seal-failure path) safe rather than silently
duplicative.

**(c) Pipelined parallel upload lanes — and what breaks if you parallelize naively.** The
foil's throughput comes from many overlapping S3 uploads across dedicated encode and upload
pools. The *safe* import is lane-parallel **encode** feeding a single committer (see §3): an
encode pool turns swapped-out collections into Parquet row groups concurrently, and the lone
sealer thread assembles + commits them serially. What **breaks under naive parallelization**
is giving each lane its own `writer_con` and its own transaction against the DuckLake
catalog: you immediately get (i) DuckLake optimistic-concurrency conflicts as concurrent
snapshots race, (ii) N× the Parquet files and snapshots per unit time — the exact tiny-file
amplification the single serialized writer was built to prevent, which then loads the
`CHECKPOINT` maintenance path with O(files) merge work — and (iii) loss of the clean
"one seal = one transaction = recoverable unit" invariant that the re-buffer-on-failure logic
(`:855–913`, `:950–1011`) depends on for correctness. The discipline to keep: **parallelize
the bytes, serialize the commit.**

## Bottom line

We are competitive precisely where the foil is not — operational simplicity, time-to-first-
query, and SQL/MCP-as-product on one node — and we are not competitive on raw sustained
Gbps-class throughput, HA, or accept-time durability, where the decoupled object-store
pipeline is structurally stronger. The single-sealer ceiling is append/encode-bound (~97%),
so the 1.0-relevant engineering bet is *parallelize encode, keep commit serial, and add a
stable dedupable batch identity* — buying throughput and at-least-once correctness without
re-importing the small-file/snapshot amplification the serialized writer exists to prevent.

*Sources: [OpenData — Ingesting 1Gbps of logs into ClickHouse for $180/month](https://www.opendata.dev/blog/ingesting-1gbps-logs-to-clickhouse/).*
