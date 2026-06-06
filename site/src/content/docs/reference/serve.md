---
title: "OTLP HTTP Ingest Server"
---

You can run an embedded HTTP server that accepts live OTLP/HTTP exports and streams them into DuckDB. Point any OpenTelemetry exporter at the server. The server buffers rows, then commits them in batches to the connection's default catalog, a DuckLake lakehouse, or another attached writable catalog such as an Iceberg REST catalog.

Native extension builds include the server. WASM builds omit it. Live ingestion uses HTTP only, with no gRPC listener.

For a runnable walkthrough, see the [Live Ingest Quickstart](../../quickstart/serve/). For lakehouse examples, see [Stream to Local DuckLake](../../guides/stream-to-local-ducklake/), [Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/), [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/), and [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/). For plain files or object storage, see [Stream to Parquet](../../guides/stream-to-parquet/). For the implementation model, see [Architecture](../../architecture/#otlp-http-ingest-server).

## Functions

The extension registers five server functions (four lifecycle, one diagnostic):

| Function | What it does |
|----------|-------------|
| `otlp_serve([uri], ...)` | Start an HTTP server and create/validate target tables. Returns one row describing the listener. |
| `otlp_flush(uri)` | Force a synchronous commit of buffered rows when readers need fresh data. Returns commit stats. It leaves catalog maintenance alone. |
| `otlp_stop(uri)` | Stop the server listening on `uri` (commits remaining rows first). Returns a status string. |
| `otlp_server_list()` | List all running servers with live counters, buffer state, and health. |
| `otlp_seal_list()` | List recent seal attempts with append, commit, row, byte, and error telemetry. |

### `otlp_serve([uri], ...)`

Starts an OTLP/HTTP ingest server bound to `uri`. The `uri` argument is optional; with no argument it defaults to `otlp:localhost:4318`.

```sql
-- Stream into an attached catalog
SELECT * FROM otlp_serve('otlp:localhost:4318', catalog := 'lake', token := 'my-dev-token-123456');
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` (positional) | VARCHAR | `otlp:localhost:4318` | Listen URI. See [URI scheme](#uri-scheme). |
| `catalog` | VARCHAR | *(default catalog)* | Name of the target catalog. Empty means the connection's **default catalog** (in-memory or file). Set this to an attached writable catalog such as DuckLake or an Iceberg REST catalog to stream OTLP into a lakehouse. See [Catalog targeting](#catalog-targeting). |
| `token` | VARCHAR | *(random, see below)* | Auth token clients must present. Must be at least 16 characters. If you omit it, `otlp_serve` generates a random 32-hex-character token and returns it in `auth_token`. |
| `schema` | VARCHAR | `main` | Schema (within the catalog) that holds the target tables. |
| `parquet_export_path` | VARCHAR | *(none)* | Plain Parquet export root. When set, each seal writes the sealed rows to `<root>/<table>/year=YYYY/month=MM/day=DD/*.parquet` as the **only** durable store (no local table copy); a read-only view per signal is created over the files for inspection. Mutually exclusive with a `catalog` target. Export is **at-least-once** (a `COPY` cannot be rolled back). |
| `create_tables` | BOOLEAN | `true` | Create the six target tables if they don't exist. When `false`, the tables must already exist with the expected columns or `otlp_serve` fails fast. |
| `allow_other_hostname` | BOOLEAN | `false` | Allow binding to a non-localhost host. By default `otlp_serve` permits only `localhost`, `127.0.0.1`, and `::1`. |
| `max_body_bytes` | UBIGINT | `16777216` (16 MiB) | Reject request bodies larger than this with `413`. Must be greater than zero. |
| `http_threads` | UBIGINT | host-based bounded default | Worker threads for concurrent HTTP requests. Must be greater than zero when set. |
| `max_buffered_bytes` | UBIGINT | `536870912` (512 MiB) | Backpressure cap. POSTs that would exceed this return `503`. |
| `seal_target_bytes` | UBIGINT | `134217728` (128 MiB) | Request an asynchronous seal when admitted, uncommitted request bytes reach this threshold. Larger values write fewer, larger files at the cost of a larger in-memory crash-loss window (still bounded by `seal_max_age_ms`). Must be greater than zero. |
| `seal_max_age_ms` | BIGINT | `5000` | Request an asynchronous seal when the oldest buffered row reaches this age. Must be greater than zero. |
| `target_file_size` | UBIGINT | `268435456` (256 MiB) | DuckLake only. **Output** Parquet file size the post-seal `CHECKPOINT` merge bin-packs toward; bounds compaction write amplification (files already at target are left alone). Distinct from `seal_target_bytes`, which is admitted *input* bytes. Must be greater than zero. |
| `maintenance_retention_ms` | BIGINT | `900000` (15 min) | DuckLake only. How old snapshots and unused data files must be before the post-seal `CHECKPOINT` expires and deletes them (`expire_older_than` / `delete_older_than`). Keep it longer than your longest read; time-travel below this window is unavailable. Must be greater than zero. |

**Output columns** (one row):

| Column | Type | Description |
|--------|------|-------------|
| `listen_uri` | VARCHAR | The `otlp:` URI the server is bound to. |
| `listen_url` | VARCHAR | The equivalent `http://` base URL (POST endpoints hang off this). |
| `auth_token` | VARCHAR | The token clients must present (the value you passed, or the generated one). |
| `schema_name` | VARCHAR | Schema holding the target tables. |
| `logs_table` | VARCHAR | `otlp_logs` |
| `traces_table` | VARCHAR | `otlp_traces` |
| `metrics_gauge_table` | VARCHAR | `otlp_metrics_gauge` |
| `metrics_sum_table` | VARCHAR | `otlp_metrics_sum` |
| `metrics_histogram_table` | VARCHAR | `otlp_metrics_histogram` |
| `metrics_exp_histogram_table` | VARCHAR | `otlp_metrics_exp_histogram` |
| `catalog_name` | VARCHAR | Target catalog. Empty for the connection's default catalog. |

Starting a second server on the same URI fails (`OTLP server already exists`). The DuckDB `DatabaseInstance` owns the server lifetime: DuckDB stops all servers when the database closes, but it does **not** commit their buffers at that point (see Durability below). Call `otlp_stop` before closing the database to avoid losing buffered rows.

### `otlp_flush(uri)`

Forces a **synchronous commit**: the server writes its in-memory buffer to the target in one transaction before the function returns. Normal ingest can rely on background commits, and `otlp_stop` performs a final commit. Use `otlp_flush` when readers need the latest accepted rows while the server stays running. `otlp_flush` handles durability and read freshness; it leaves compaction and other catalog maintenance alone.

```sql
-- Force a commit
SELECT * FROM otlp_flush('otlp:localhost:4318');
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` (positional) | VARCHAR | *(required)* | Listen URI of the server to flush. |

**Output columns** (one row):

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `sealed` on success, or `No server found listening on <uri>`. The literal success value means the batch commit completed. |
| `sealed_rows` | UBIGINT | Rows committed by this call. |
| `seals_total` | UBIGINT | Total batch commits performed by this server since startup. |
| `error` | VARCHAR | Commit error detail, or `NULL` if none. |

### `otlp_stop(uri)`

Stops the server listening on `uri` and frees the port. Any buffered rows are **committed first**, so a graceful stop loses no data.

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

**Output column:**

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `Stopped listening on <uri>` if a server was stopped, or `No server found listening on <uri>` if none matched. |

### `otlp_server_list()`

Lists running OTLP servers with live counters and buffer state. Takes no arguments.

```sql
SELECT
  listen_uri,
  catalog_name,
  total_rows,
  buffered_rows,
  last_seal_age_ms AS last_commit_age_ms,
  is_listening
FROM otlp_server_list();
```

**Output columns** (one row per running server):

| Column | Type | Description |
|--------|------|-------------|
| `listen_uri` | VARCHAR | The `otlp:` listen URI. |
| `listen_url` | VARCHAR | The `http://` base URL. |
| `host` | VARCHAR | Bound host. |
| `port` | USMALLINT | Bound port. |
| `catalog_name` | VARCHAR | Target catalog. Empty for the default catalog. |
| `schema_name` | VARCHAR | Schema holding the target tables. |
| `active_requests` | UBIGINT | Requests in progress. |
| `total_requests` | UBIGINT | Requests handled since startup (includes failures). |
| `total_rows` | UBIGINT | Rows **accepted** (buffered) since startup. Once the buffer drains, this equals the rows committed. A `/v1/metrics` request counts rows across all four metric tables. |
| `buffered_rows` | UBIGINT | Rows in the buffer that the writer has not committed. |
| `admitted_bytes` | UBIGINT | Encoded request bytes admitted but not yet released by a successful seal. |
| `buffered_bytes` | UBIGINT | Approximate decoded heap held by the in-memory buffers. Unlike `admitted_bytes` (which bounds *encoded input* via `max_buffered_bytes`), this reflects the real memory footprint and grows unbounded while a seal is stuck — watch it to detect backpressure-vs-OOM risk. |
| `seal_target_bytes` | UBIGINT | Configured size trigger for requesting a seal. |
| `seal_max_age_ms` | BIGINT | Configured age trigger for requesting a seal. |
| `oldest_buffered_age_ms` | BIGINT | Age (ms) of the oldest buffered row, or `NULL` when empty. |
| `last_seal_age_ms` | BIGINT | Age (ms) since the last successful batch commit, or `NULL` if none has completed. |
| `seals_total` | UBIGINT | Batch commits performed since startup. |
| `committed_rows_total` | UBIGINT | Rows committed since startup. |
| `seal_failures_total` | UBIGINT | Failed batch commits since startup. |
| `is_listening` | BOOLEAN | `false` once the listener has fallen over (e.g. an error after a successful bind). |
| `last_error` | VARCHAR | Last fatal listener error, or `NULL` if none. |
| `seal_last_error` | VARCHAR | Last batch commit error, or `NULL` if none. |
| `maintenance_runs_total` | UBIGINT | Successful post-seal catalog maintenance (`CHECKPOINT`) passes since startup. Stays `0` for the default catalog and for catalogs where maintenance is unsupported/disabled. |
| `maintenance_failures_total` | UBIGINT | Failed maintenance passes since startup. |
| `last_maintenance_age_ms` | BIGINT | Age (ms) since the last successful maintenance pass, or `NULL` if none has run. |
| `maintenance_last_error` | VARCHAR | Last maintenance error, or `NULL` if none. |

Use `is_listening` / `last_error` to detect a dead listener. Use `seal_last_error` to inspect writer failures, such as catalog conflicts. Use `maintenance_runs_total` / `last_maintenance_age_ms` to confirm compaction is keeping up.

### `otlp_seal_list()`

Lists the bounded in-memory history of recent seal attempts for all running servers. `duration_ms` covers the complete seal attempt. For transaction-backed targets, `append_duration_ms` measures appending buffered chunks into the destination tables and `commit_duration_ms` measures `COMMIT`, including catalog and object-storage work. The remaining duration is buffer swapping, transaction setup, and bookkeeping. Plain Parquet export does not have a transaction commit, so both phase fields are zero.

## Catalog targeting

The target of a server is `<catalog>.<schema>.<table>`. The live-ingest tables keep the same column names as the file readers, but the nanosecond timestamp columns (`time_unix_nano`, `start_time_unix_nano`, …) are stored as DuckDB `TIMESTAMP` (microsecond) for catalog compatibility, where the file readers expose `TIMESTAMP_NS` — so a query that mixes a live table with `read_otlp_*` sees two types and loses sub-microsecond precision on the live side (see the [Schema Reference](../schemas/#type-system-notes)).

- **Default catalog** (`catalog` omitted): rows land in the connection's default catalog, either an in-memory database or the file you opened DuckDB with. Use this zero-setup path when you do not need a lakehouse catalog. The server still buffers ingest (a POST returns `202`); rows become durable in that database at the next background commit.
- **DuckLake catalog** (`catalog := '<attached_db>'`): rows stream into a [DuckLake](https://ducklake.select) lakehouse with Parquet data files on local or object storage, tracked by a catalog. Attach the catalog first, then name it:

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'otlp_data/');

CALL otlp_serve('otlp:localhost:4318', catalog := 'lake');
-- rows buffer and commit into Parquet under otlp_data/
SELECT count(*) FROM lake.main.otlp_logs;
```

Each batch commit writes **one Parquet data file per signal** plus one DuckLake snapshot. After a conservative number of successful automatic row-seals, `duckdb-otlp` runs best-effort catalog-native maintenance with DuckDB's non-force `CHECKPOINT lake` when recent ingest rate and pending bytes leave ample admission headroom. On a DuckLake catalog, `CHECKPOINT` merges adjacent files and expires/cleans old snapshots and data files in one pass — turning the many small per-seal files into compacted, query-efficient files. At startup the server sets the DuckLake options `CHECKPOINT` reads so this is **bounded**: `target_file_size` caps the merge output (files already at target are left alone, so re-compaction is O(new), not O(total)), and `expire_older_than` / `delete_older_than` (from `maintenance_retention_ms`) gate how old snapshots/files must be before reclaim. See [Durability and background commits](#durability-and-background-commits).

- **Iceberg REST catalog** (`catalog := '<attached_db>'`): rows stream into tables in an attached writable Iceberg REST catalog. Attach the catalog with DuckDB's `iceberg` extension, create the target schema, then pass the catalog and schema to `otlp_serve`; see [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/) and [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/) for managed provider paths.
  DuckDB's Iceberg REST catalog docs have no useful `CHECKPOINT` maintenance path today. The internal maintenance probe uses generic `CHECKPOINT <catalog>` only. If the catalog reports checkpointing as unsupported, the server disables automatic maintenance for that server and ingest durability continues normally.

- **Plain Parquet export** (`parquet_export_path := '/data/otlp-parquet'` or `parquet_export_path := 's3://bucket/prefix'`): each seal writes the sealed rows straight to `<path>/<table>/year=YYYY/month=MM/day=DD/*.parquet`. The Parquet dataset is the only durable store — no local table copy is kept (a read-only view per signal is created over the files for inspection), and it is mutually exclusive with a `catalog` target. Because a `COPY` is a file write and cannot be rolled back, export is **at-least-once**: a signal that already exported is never re-written, but a seal whose `COPY` fails part-way can re-export that signal's rows on retry, so deduplicate downstream if you need exactly-once. Use this when you want partitioned Parquet without a lakehouse catalog; see [Stream to Parquet](../../guides/stream-to-parquet/).

## URI scheme

Listen URIs use the `otlp:` scheme:

| Form | Example |
|------|---------|
| `otlp:host:port` | `otlp:localhost:4318` |
| `otlp://host:port` | `otlp://127.0.0.1:4318` |
| IPv6 (host in brackets) | `otlp:[::1]:4318` |

By default, `otlp_serve` allows only `localhost`, `127.0.0.1`, and `::1`. To bind to any other host (for example `0.0.0.0` to accept remote exporters), pass `allow_other_hostname := true`. `otlp_serve` rejects non-localhost hosts before it binds a socket.

The scalar function **`otlp_uri_parser(uri)`** parses an `otlp:` URI and returns a `STRUCT(host VARCHAR, port USMALLINT, ipv6 BOOLEAN, url VARCHAR)` — the same parsing `otlp_serve` uses, useful for validating a URI or deriving the `http://` base URL up front.

## HTTP endpoints

The `http://` base URL from `listen_url` exposes:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/logs` | Ingest logs into `otlp_logs`. |
| POST | `/v1/traces` | Ingest traces into `otlp_traces`. |
| POST | `/v1/metrics` | Ingest metrics. Fans out across all four metric tables: `otlp_metrics_gauge`, `otlp_metrics_sum`, `otlp_metrics_histogram`, `otlp_metrics_exp_histogram`. |
| GET | `/healthz` | Liveness probe. Returns `200` with `{"status":"ok"}`. No auth required. |
| GET | `/readyz` | Readiness probe. Returns `200` with `{"status":"ready"}` once the listener is bound, and `503` with `{"status":"degraded"}` when buffered rows are not committing (a seal has failed, rows are still buffered, and the last successful seal is absent or several seal cycles old). No auth required. |

Tables live in `<catalog>.<schema>`, chosen by `otlp_serve(catalog := ..., schema := ...)`.

### Content types

The server picks a parser from the request `Content-Type`:

| Content-Type | Format |
|--------------|--------|
| `application/json`, `application/otlp+json` | OTLP/JSON |
| `application/x-ndjson` | newline-delimited OTLP/JSON (JSONL) |
| `application/x-protobuf`, `application/protobuf`, `application/otlp` | OTLP/protobuf |

Any other content type returns `415`.

### Content encodings

`Content-Encoding: identity` (or no header), `gzip`, and `deflate` are accepted. Any other content encoding returns `415`.

## Authentication

Every POST must present the configured token through one of these headers:

- `Authorization: Bearer <token>` (case-insensitive scheme)
- `x-api-key: <token>`

The server checks the two headers independently, so a malformed `Authorization` header does not mask a valid `x-api-key`. A missing or invalid token returns `401`. The server compares tokens with a constant-time check.

Tokens must be at least 16 characters. Auto-generated tokens (when `token` is omitted) are 32 hex characters (128 bits of entropy).

## Responses and status codes

A successful POST returns `202 Accepted` after the server buffers rows:

```json
{"status":"buffered","rows":42,"batches":1}
```

A `202` means the server validated, converted, and accepted the rows into the in-memory buffer. **The rows are not yet durable** (see below).

Errors return JSON shaped like `{"error":"<reason>","message":"<detail>"}`:

| Status | When |
|--------|------|
| `400` | OTLP body failed to parse (or other invalid input). |
| `401` | Missing or invalid auth token. |
| `413` | Body larger than `max_body_bytes`. |
| `415` | Unsupported `Content-Type` or `Content-Encoding`. |
| `503` | Buffer admission full (request would exceed `max_buffered_bytes`). Retry with backoff. |
| `500` | Internal error (also written to `duckdb_logs`). |

## Durability and background commits

The server **buffers ingest and commits rows in batches** for each target. Batch commits avoid per-request tiny files and write conflicts: a single serialized writer prevents concurrent catalog writes from the ingest server.

**The flow:**

1. A POST reserves admission bytes, parses, converts, and appends rows into the relevant per-signal in-memory buffer, then returns `202`. The bounded worker pool does this concurrently; append locks only the target signal buffer.
2. A single background writer commits the buffer to the target in **one transaction** when any trigger fires:
   - admitted request-body bytes reach the internal size threshold, 128 MiB today,
   - the oldest buffered row reaches the internal age limit, about 5 seconds today, or
   - an explicit [`otlp_flush`](#otlp_flushuri-).
3. For a DuckLake target, each batch commit writes **one Parquet data file per signal** plus one snapshot.
4. For named catalogs, the server may follow successful automatic row-seals with best-effort, non-force `CHECKPOINT <catalog>` outside the ingest transaction when recent ingest rate and pending bytes leave ample admission headroom. Treat this as internal scheduling. The server skips the default catalog. The hook also skips explicit `otlp_flush`, sustained high ingest, high pending buffered bytes, and shutdown drains; unsupported checkpoint implementations log once and disable the hook for that server.

**Durability contract:**

- A `202` is **not durable.** Rows become durable at the next background commit, on `otlp_stop`, or on `otlp_flush`.
- A crash or hard kill loses buffered-but-uncommitted rows (**at-most-once** for that window).
- `otlp_stop` and `otlp_flush` **commit remaining rows before returning**, so those calls lose no accepted rows. A plain **database/connection close does NOT commit buffered rows**. The drain runs after DuckDB tears down the instance, when it can no longer write, so DuckDB can drop buffered rows. Prefer `otlp_stop` before closing the database. Use `otlp_flush` when the server should keep running but readers need durable rows now.

The project tracks a future durable raw-spool journal for at-least-once delivery.

**Backpressure:** if admitting a request would exceed `max_buffered_bytes` (default 512 MiB) across in-flight and uncommitted accepted payloads, the POST returns `503` before parse/transform work. Clients should retry with backoff.

**Seal cadence:** `seal_target_bytes` and `seal_max_age_ms` are size and age triggers for the single asynchronous writer. They control batching latency and file/transaction size; they do not raise durable write throughput. `max_buffered_bytes` remains the separate admission cap.

**Keeping DuckLake tidy:** each batch commit leaves one Parquet file per signal, so a high seal cadence creates many small files. For DuckLake, the post-seal `CHECKPOINT` merges those into larger files and reclaims old snapshots/files when recent ingest leaves enough admission headroom. The merge is **bounded** by the `target_file_size` option the server sets at startup (files already at target are skipped, so re-compaction cost scales with new data, not total data), and reclaim is gated by `maintenance_retention_ms` (`expire_older_than` / `delete_older_than`). The hook skips per-seal maintenance and stays outside the ingest transaction; a maintenance failure leaves committed rows intact. `otlp_flush` still forces only ingest durability and leaves compaction to catalog maintenance.

## Concurrency model

- The server runs a bounded httplib worker pool. Workers parse, convert, and buffer requests concurrently; each signal table has its own buffer lock. In the daemon, set `DUCKDB_OTLP_HTTP_THREADS` to override the host-based default.
- A single background writer thread writes to the target catalog. Serial writes let DuckLake, which uses optimistic concurrency, avoid conflict retries and tiny-file churn.

## Verifying ingest under load

`make test` cannot issue HTTP POSTs, so the SQL logic tests cover only the lifecycle functions. To exercise the ingest hot path, run the manual concurrency harness. It covers auth, content-type handling, the metrics fan-out, buffering and batch commits, and Arrow → DuckDB conversion under concurrency:

```bash
uv run --script test/manual/otlp_serve_concurrency.py

# Override the payload / concurrency:
OTLP_PAYLOAD=test/data/logs_simple.jsonl OTLP_CONCURRENCY=64 \
    uv run --script test/manual/otlp_serve_concurrency.py

# Exercise the DuckLake path (writes Parquet under the given dir and checks the
# automatic catalog-maintenance event):
OTLP_DUCKLAKE_DIR=/tmp/otlp_lake \
    uv run --script test/manual/otlp_serve_concurrency.py
```

It covers auth and validation errors, low-buffer backpressure, metrics fanout, stop-under-load, concurrent flush/stop, and the optional local DuckLake maintenance checkpoint event, then reconciles accepted rows against committed row counts. Run it against a TSan/ASan build to catch races.

## See also

- [Live Ingest Quickstart](../../quickstart/serve/): POST one log to the default catalog with `curl`.
- [Stream to Local DuckLake](../../guides/stream-to-local-ducklake/): write live OTLP rows to local DuckLake.
- [Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/): write live OTLP rows to DuckLake with Neon and R2.
- [Stream to Parquet](../../guides/stream-to-parquet/): write live OTLP rows to partitioned Parquet files on disk or S3.
- [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/): write live OTLP rows to Amazon S3 Tables as an Iceberg catalog.
- [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/): write live OTLP rows to Cloudflare R2 Data Catalog as an Iceberg catalog.
- [Architecture](../../architecture/#otlp-http-ingest-server): buffer, background writer, and `otlp_flush` internals.
- [Schema Reference](../schemas/): columns of the target tables.
