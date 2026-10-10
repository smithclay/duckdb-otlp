---
title: "Live Ingest Server"
---

You can run an embedded server that accepts live OpenTelemetry exports and streams them into DuckDB. Point any OpenTelemetry exporter at the server. The server buffers rows, then commits them in batches to the connection's default catalog, a DuckLake lakehouse, or another attached writable catalog such as an Iceberg REST catalog.

The server speaks three wire protocols across two scheme-bound functions:

| Function | Scheme | Transport | Wire protocol |
|----------|--------|-----------|---------------|
| `otlp_serve` (default) | `otlp:` (port 4318) | HTTP | **OTLP/HTTP** (JSON, NDJSON, protobuf) |
| `otlp_serve(transport := 'grpc')` | `otlp:` | gRPC | **OTLP/gRPC** unary `Export` |
| `otap_serve` | `otap:` (port 4317) | gRPC | **OTAP/Arrow** bidirectional streaming |

All three share one buffering/seal core, the same parameters, catalog targeting, token auth, and lifecycle functions — only the wire differs. The transport is **not** encoded in the scheme: `otlp_serve` picks HTTP vs gRPC via the `transport` parameter, and `otap_serve` is always gRPC. Each function rejects the other's scheme.

A **server** is one ingest pipeline — one set of in-memory buffers, one admission cap, one writer, and one catalog-maintenance schedule — fed by one or more **listeners**. Pass a list of URIs to start several listeners on one server, for example OTLP/HTTP and OTLP/gRPC together:

```sql
SELECT listen_uri, transport
FROM otlp_serve(['otlp:localhost:4318', 'otlp:localhost:4317'], transport := ['http', 'grpc'],
                catalog := 'lake', token := 'my-dev-token-123456');
```

Each catalog target (catalog + schema, or Parquet export root) has at most one server, so it has a single writer. A second `otlp_serve`/`otap_serve` call for a target that already has a server fails; list every URI in one call instead.

Native extension builds include the server; WASM builds omit it entirely (no HTTP and no gRPC). See [gRPC transport](#grpc-transport) for the OTLP/gRPC and OTAP/Arrow service contracts.

For a runnable walkthrough, see the [Live Ingest Quickstart](../../quickstart/serve/). For lakehouse examples, see [Stream to Local DuckLake](../../guides/stream-to-local-ducklake/), [Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/), [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/), and [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/). For plain files or object storage, see [Stream to Parquet](../../guides/stream-to-parquet/). For the implementation model, see [Architecture](../../architecture/#otlp-http-ingest-server).

## Command-line interface

The daemon ships as `duckdb-otlp`, a CLI for macOS and Linux that also converts
OTLP/OTAP files, exports the catalog, and runs one-shot SQL. A bare invocation
starts a local receiver with no configuration at all. See the
[CLI Reference](../cli/) for the full command, flag, and environment-variable
surface; this page documents the SQL functions underneath it.

```sh
duckdb-otlp                                  # OTLP/HTTP 4318 + OTLP/gRPC 4317 on loopback
duckdb-otlp --host 0.0.0.0 --token "$TOKEN"  # a non-loopback bind requires a token
```

## Docker daemon transports

The image starts HTTP by default. Choose standard OTLP
listeners with `DUCKDB_OTLP_TRANSPORTS`, or with the `--http`/`--grpc` flags
(and the matching `DUCKDB_OTLP_HTTP_PORT` / `DUCKDB_OTLP_GRPC_PORT` variables),
which take precedence over the transport list:

| Setting | Listeners |
| --- | --- |
| `http` (default) | OTLP/HTTP on `OTEL_HTTP_ADDR` (default `0.0.0.0:4318`) |
| `grpc` | Standard OTLP/gRPC on `OTEL_GRPC_ADDR` (default `0.0.0.0:4317`) |
| `http,grpc` | Both, on separate ports |

For example, accept both protocols into the same local DuckLake:

```sh
docker run --rm -p 4317:4317 -p 4318:4318 \
  -e DUCKDB_MODE=local-ducklake \
  -e DUCKDB_OTLP_TRANSPORTS=http,grpc \
  -e DUCKDB_OTLP_TOKEN=replace-with-a-private-token \
  -v otlp-data:/data ghcr.io/smithclay/duckdb-otlp:latest
```

Both listeners feed **one server**: the same catalog, schema, bearer token,
buffers, writer, and maintenance schedule. `DUCKDB_OTLP_MAX_BUFFERED_BYTES` and
`DUCKDB_OTLP_SEAL_TARGET_BYTES` apply to the whole process, not to each
listener. Startup must succeed for every listener. On shutdown the daemon stops
every listener, then drains the shared buffers once. If a later listener cannot
start, the earlier ones are closed and the daemon exits with an error. Duplicate/unknown transports, empty list entries, and matching HTTP/gRPC
ports are rejected. Whitespace around list entries is allowed.

`DUCKDB_OTLP_LISTEN_URI` remains a single-listener override, and cannot be
combined with `--http`/`--grpc`/`--otap`. With `grpc`, use an
`otlp:` URI (for example `otlp:0.0.0.0:4317`); the transport setting selects gRPC.
Do not combine this URI override with `http,grpc`; use the two bind-address
variables instead. The existing `otap:` URI mode still starts OTAP/Arrow when
`DUCKDB_OTLP_TRANSPORTS` is unset. OTAP/Arrow is distinct from standard OTLP/gRPC,
and cannot be combined with the transport list.

The image's `doctor` command (also accepted as `healthcheck`) checks every enabled listener, using HTTP
`/readyz` for HTTP and TCP connect for gRPC. A TCP check confirms a bound socket,
not successful ingestion or durable writes. Because the listeners share one
server, the HTTP `/readyz` reports the commit health of rows from both
transports. `DUCKDB_OTLP_HTTP_THREADS` applies
only to the HTTP listener. Quack remains a separate opt-in administrative endpoint.

For Cloud Run, select `grpc`, expose its port as `h2c`, and use a compatible
probe instead of HTTP `/readyz` on that port. Cloud Run terminates TLS and
forwards cleartext HTTP/2 to the container. Its single ingress port does not
expose both daemon ports; running `http,grpc` does not multiplex the protocols
onto one port. See [Cloud Run HTTP/2 configuration](https://docs.cloud.google.com/run/docs/configuring/http2).

## Functions

The extension registers six server functions (two to start a server, two lifecycle, two diagnostic):

| Function | What it does |
|----------|-------------|
| `otlp_serve([uri], ...)` | Start an **OTLP** server (OTLP/HTTP, or OTLP/gRPC with `transport := 'grpc'`) on one or more listeners and create/validate target tables. Returns one row per listener. |
| `otap_serve([uri], ...)` | Start an **OTAP/Arrow** gRPC streaming server and create/validate target tables. Same parameters and output as `otlp_serve`. Returns one row per listener. |
| `otlp_flush(uri)` | Force a synchronous commit of buffered rows when readers need fresh data. Returns commit stats. It leaves catalog maintenance alone. |
| `otlp_stop(uri)` | Stop the server that owns listener `uri`, including its other listeners (commits remaining rows first). Returns a status string. |
| `otlp_stop()` | Stop every server on this database, committing each first. Returns one row per server. |
| `otlp_server_list()` | List every running listener with its server's live counters, buffer state, and health. |
| `otlp_seal_list()` | List recent seal attempts with append, commit, row, byte, and error telemetry. |

`otlp_flush`, `otlp_stop`, `otlp_server_list`, and `otlp_seal_list` are **transport-agnostic** — they dispatch by the scheme-aware canonical listen URI, so an `otap:` server and an `otlp:` server are distinct entries managed by the same lifecycle functions. Any listener's URI names its whole server.

### `otlp_serve([uri], ...)`

Starts an OTLP ingest server with a listener on `uri`. The `uri` argument is optional; with no argument it defaults to `otlp:localhost:4318`. Pass a `VARCHAR[]` list to start several listeners on the same server.

```sql
-- Stream into an attached catalog
SELECT * FROM otlp_serve('otlp:localhost:4318', catalog := 'lake', token := 'my-dev-token-123456');
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` (positional) | VARCHAR or VARCHAR[] | `otlp:localhost:4318` | Listen URI, or a list of distinct listen URIs that feed one server. See [URI scheme](#uri-scheme). |
| `transport` | VARCHAR or VARCHAR[] | `http` | `http` (OTLP/HTTP) or `grpc` (OTLP/gRPC unary). One value applies to every URI; a list must have one entry per URI. `otap_serve` accepts only `grpc`. |
| `catalog` | VARCHAR | *(default catalog)* | Name of the target catalog. Empty means the connection's **default catalog** (in-memory or file). Set this to an attached writable catalog such as DuckLake or an Iceberg REST catalog to stream OTLP into a lakehouse. See [Catalog targeting](#catalog-targeting). |
| `token` | VARCHAR | *(random, see below)* | Auth token clients must present. Must be at least 16 characters. If you omit it, `otlp_serve` generates a random 32-hex-character token and returns it in `auth_token`. Ignored when `disable_auth := true`. |
| `disable_auth` | BOOLEAN | `false` | Accept every request **without** checking the token. No token is generated or validated, and `auth_token` comes back empty. Opt-in for trusted local networks and for producers that cannot attach a bearer token (e.g. the otel-arrow OTAP exporter). See [Authentication](#authentication). |
| `schema` | VARCHAR | `main` | Schema (within the catalog) that holds the target tables. |
| `parquet_export_path` | VARCHAR | *(none)* | Plain Parquet export root. When set, each seal writes the sealed rows to `<root>/<table>/year=YYYY/month=MM/day=DD/*.parquet` as the **only** durable store (no local table copy); a read-only view per signal is created over the files for inspection. Mutually exclusive with a `catalog` target. Export is **at-least-once** (a `COPY` cannot be rolled back). |
| `create_tables` | BOOLEAN | `true` | Create the six target tables if they don't exist. When `false`, the tables must already exist with the expected columns or `otlp_serve` fails fast. |
| `allow_other_hostname` | BOOLEAN | `false` | Allow binding to a non-localhost host. By default `otlp_serve` permits only `localhost`, `127.0.0.1`, and `::1`. |
| `max_body_bytes` | UBIGINT | `16777216` (16 MiB) | Reject request bodies larger than this with `413`. Must be greater than zero. |
| `http_threads` | UBIGINT | host-based bounded default | Worker threads for concurrent HTTP requests, per HTTP listener. Must be greater than zero when set. |
| `max_buffered_bytes` | UBIGINT | `536870912` (512 MiB) | Backpressure cap for the whole server, shared by every listener. Requests that would exceed it return `503` (gRPC: `RESOURCE_EXHAUSTED`). |
| `seal_target_bytes` | UBIGINT | `134217728` (128 MiB) | Request an asynchronous seal when admitted, uncommitted request bytes reach this threshold. Larger values write fewer, larger files at the cost of a larger in-memory crash-loss window (still bounded by `seal_max_age_ms`). Must be greater than zero. |
| `seal_max_age_ms` | BIGINT | `5000` | Request an asynchronous seal when the oldest buffered row reaches this age. Must be greater than zero. |
| `target_file_size` | UBIGINT | `268435456` (256 MiB) | DuckLake only. **Output** Parquet file size the post-seal `CHECKPOINT` merge bin-packs toward; bounds compaction write amplification (files already at target are left alone). Distinct from `seal_target_bytes`, which is admitted *input* bytes. Must be greater than zero. |
| `maintenance_retention_ms` | BIGINT | `900000` (15 min) | DuckLake only. How old snapshots and unused data files must be before the post-seal `CHECKPOINT` expires and deletes them (`expire_older_than` / `delete_older_than`), and how old an untracked data file must be before the orphan sweep deletes it. Keep it longer than your longest read; time-travel below this window is unavailable. Must be greater than zero. |
| `promote_resource_attributes` | VARCHAR | *(none)* | Comma-separated **resource** attribute keys to promote into first-class columns at ingest. See [Attribute promotion](#attribute-promotion). Catalog mode only. |
| `promote_scope_attributes` | VARCHAR | *(none)* | Comma-separated **scope** attribute keys to promote into first-class columns at ingest. See [Attribute promotion](#attribute-promotion). Catalog mode only. |
| `attributes_as_variant` | BOOLEAN | `false` | Create and fill the attribute bags as `VARIANT` instead of `VARCHAR` holding JSON text. See [Attributes as VARIANT](#attributes-as-variant). |

**Output columns** (one row per listener):

| Column | Type | Description |
|--------|------|-------------|
| `listen_uri` | VARCHAR | The `otlp:` URI the listener is bound to. |
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
| `transport` | VARCHAR | `http` or `grpc`. |

Starting a second server on the same URI, or for the same catalog target, fails (`OTLP server already exists`). If any listener fails to bind, the listeners already started are closed and no server is registered. The DuckDB `DatabaseInstance` owns the server lifetime: DuckDB stops all servers when the database closes, but it does **not** commit their buffers at that point (see Durability below). Call `otlp_stop` before closing the database to avoid losing buffered rows — `otlp_stop()` with no argument covers every server, including any you did not start yourself.

### `otap_serve([uri], ...)`

Starts an **OTAP/Arrow** gRPC streaming server bound to `uri`. The `uri` argument is optional; with no argument it defaults to `otap:localhost:4317`. `otap_serve` is gRPC-only — `transport` must be `'grpc'` or omitted (OTAP/Arrow is always gRPC); any other value is rejected.

```sql
-- Stream OTAP/Arrow into an attached DuckLake catalog
SELECT listen_uri, listen_url FROM otap_serve('otap:localhost:4317', catalog := 'lake', token := 'my-dev-token-123456');
```

It takes the **same named parameters** and returns the **same output columns** as [`otlp_serve`](#otlp_serveuri-); only the transport, scheme, and default port differ. It serves the canonical OTAP/Arrow bidirectional-streaming services (`Arrow{Logs,Traces,Metrics}Service`) for all six signals, sharing the same catalog/Parquet targeting, token auth, buffered group-commit, and lifecycle functions. See [gRPC transport](#grpc-transport) for the service and RPC contract.

Two parameter notes for the gRPC path:

- `max_body_bytes` becomes the gRPC server's **maximum decoding message size** (the largest single `BatchArrowRecords`/`Export` message accepted), rather than an HTTP body cap.
- `http_threads` is **ignored** — it tunes only the HTTP worker pool; the gRPC path uses its own async runtime.

Because the wire is gRPC (HTTP/2), an `otap:` server exposes **no HTTP endpoints** — the `/v1/*` POST paths and the `/healthz` / `/readyz` probes are HTTP-only. The `listen_url` output column is still populated with a derived `http://host:port` string for display, but it is not a usable HTTP base URL; clients connect with a gRPC client, and a liveness probe should use a TCP connect rather than `/readyz`. The same applies to an `otlp_serve(transport := 'grpc')` listener.

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

Stops the server that owns listener `uri`: every listener of that server stops accepting and frees its port, then the shared buffers are **committed once**, so a graceful stop loses no data.

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

### `otlp_stop()`

Stops **every** server on this database, committing each one before returning, and returns one row per server. The registry stays usable afterwards: this is a graceful drain, not teardown, so a later `otlp_serve` still works.

```sql
SELECT status, dropped_rows FROM otlp_stop();
```

Prefer this over `otlp_stop(uri)` in a shutdown path. A stop that names one URI reaches only that server, so a server started somewhere else — by another connection, or by the daemon's [`--init-sql`](../cli/) script — survives to database teardown, where the final commit can no longer write and its buffered rows are dropped. With no servers running it returns a single `No OTLP servers are running` row.

**Output columns (both forms):**

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `Stopped listening on <uri>[, <uri>...]` listing every listener that was stopped, or `No server found listening on <uri>` if none matched. |
| `dropped_rows` | UBIGINT | Rows still buffered after the final commit failed, and therefore lost. `0` on a clean stop. |

### `otlp_server_list()`

Lists running listeners with their server's live counters and buffer state. Takes no arguments. Listeners of the same server report identical buffer, seal, and maintenance values; `listen_uri`, `transport`, `is_listening`, and `last_error` are per listener.

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

**Output columns** (one row per listener):

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
| `maintenance_failures_total` | UBIGINT | Failed maintenance passes since startup. Excludes contended passes. |
| `last_maintenance_age_ms` | BIGINT | Age (ms) since the last successful maintenance pass, or `NULL` if none has run. |
| `maintenance_last_error` | VARCHAR | Last maintenance error, or `NULL` if none. |
| `promoted_columns_total` | UBIGINT | Promoted attribute columns per signal table, or `0` when [attribute promotion](#attribute-promotion) is off/disabled. |
| `transport` | VARCHAR | The listener's transport: `http` or `grpc`. |
| `maintenance_contended_total` | UBIGINT | Maintenance passes skipped because another writer checkpointed the same catalog first (for example a second receiver process). Not a failure. |

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

Each batch commit writes **one Parquet data file per signal** plus one DuckLake snapshot. After a conservative number of successful automatic row-seals — or about 10 minutes after the last attempt whenever any row-seal is pending, even if ingest has gone idle — `duckdb-otlp` runs best-effort catalog-native maintenance with DuckDB's non-force `CHECKPOINT lake` when recent ingest rate and pending bytes leave ample admission headroom. On a DuckLake catalog, `CHECKPOINT` merges adjacent files and expires/cleans old snapshots and data files in one pass — turning the many small per-seal files into compacted, query-efficient files. At startup the server sets the DuckLake options `CHECKPOINT` reads so this is **bounded**: `target_file_size` caps the merge output (files already at target are left alone, so re-compaction is O(new), not O(total)), and `expire_older_than` / `delete_older_than` (from `maintenance_retention_ms`) gate how old snapshots/files must be before reclaim. See [Durability and background commits](#durability-and-background-commits).

  Several processes can share one DuckLake catalog (for example two receiver instances during a deploy). Their `CHECKPOINT`s can race: the loser fails with a catalog error such as PostgreSQL's `could not serialize access due to concurrent delete`. The server counts that as **contention**, not failure: it increments `maintenance_contended_total`, leaves `maintenance_failures_total` and `maintenance_last_error` alone, and tries again at the next interval. The losing checkpoint can leave a Parquet file the catalog never tracked. After each successful `CHECKPOINT` on a DuckLake catalog, the server runs `ducklake_delete_orphaned_files` with `older_than` set to `maintenance_retention_ms`, so it removes those files but never a file a seal or checkpoint may still be writing.

  DuckLake **inlines** small inserts: rows below its data-inlining row limit are stored in the catalog database (for example PostgreSQL) instead of a Parquet file, and only `CHECKPOINT` flushes them to Parquet. At low volume, most seals are inlined, so no Parquet appears until the next maintenance pass. The time-based trigger bounds that delay. The daemon also prints each `CHECKPOINT` success and failure to stderr. To skip inlining entirely, attach with `DATA_INLINING_ROW_LIMIT 0`. The daemon does this when `DUCKLAKE_DATA_INLINING_ROW_LIMIT=0` is set, and every seal then writes Parquet directly, at the cost of one small file per signal per seal until compaction.

- **Iceberg REST catalog** (`catalog := '<attached_db>'`): rows stream into tables in an attached writable Iceberg REST catalog. Attach the catalog with DuckDB's `iceberg` extension, create the target schema, then pass the catalog and schema to `otlp_serve`; see [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/) and [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/) for managed provider paths.
  DuckDB's Iceberg REST catalog docs have no useful `CHECKPOINT` maintenance path today. The internal maintenance probe uses generic `CHECKPOINT <catalog>` only. If the catalog reports checkpointing as unsupported, the server disables automatic maintenance for that server and ingest durability continues normally.

- **Plain Parquet export** (`parquet_export_path := '/data/otlp-parquet'` or `parquet_export_path := 's3://bucket/prefix'`): each seal writes the sealed rows straight to `<path>/<table>/year=YYYY/month=MM/day=DD/*.parquet`. The Parquet dataset is the only durable store — no local table copy is kept (a read-only view per signal is created over the files for inspection), and it is mutually exclusive with a `catalog` target. Because a `COPY` is a file write and cannot be rolled back, export is **at-least-once**: a signal that already exported is never re-written, but a seal whose `COPY` fails part-way can re-export that signal's rows on retry, so deduplicate downstream if you need exactly-once. Use this when you want partitioned Parquet without a lakehouse catalog; see [Stream to Parquet](../../guides/stream-to-parquet/).

## Attribute promotion

Resource and scope attributes land as JSON objects in the `resource_attributes` and `scope_attributes` columns (see [Schemas](../schemas/)), so filtering one means a full-scan `json_extract_string(...)` with no row-group pruning. Attribute promotion lifts **operator-named** keys out of that JSON into their own top-level columns at ingest, so the scanner gets per-row-group min/max statistics and can skip files. It is the right tool for the low-cardinality, stable identity attributes you filter and group by most — `service.name` is already a column, but `deployment.environment`, `k8s.namespace.name`, `cloud.region`, and the like are not.

```sql
SELECT * FROM otlp_serve(
    'otlp:0.0.0.0:4318',
    catalog := 'lake',
    promote_resource_attributes := 'deployment.environment, k8s.namespace.name',
    promote_scope_attributes    := 'telemetry.sdk.name'
);
```

- **Column naming.** Each key becomes `resource_attr_<key>` or `scope_attr_<key>` (non-alphanumeric characters become `_`), e.g. `deployment.environment` → `resource_attr_deployment_environment`, on **all six** signal tables. The columns are added once when the server starts.
- **The bag is kept.** A promoted column is an accelerator, not a replacement — the original key stays in `resource_attributes`/`scope_attributes`. Rows written *before* a key was promoted read back `NULL` for its column, so query across old and new data by COALESCEing the column with the same extract the server projects. Over a JSON bag that is:

  ```sql
  COALESCE(resource_attr_deployment_environment,
           json_extract_string(resource_attributes, '$."deployment.environment"'))
  ```

  and with [`attributes_as_variant`](#attributes-as-variant):

  ```sql
  COALESCE(resource_attr_deployment_environment,
           CAST(variant_extract(resource_attributes, 'deployment.environment') AS VARCHAR))
  ```

  Neither extract runs on the other's type: `json_extract_string` over a VARIANT bag fails with `Malformed JSON`, because it is handed DuckDB's display form rather than JSON text.

- **No auto-discovery.** The promoted set is exactly what you list — there is no workload observation or automatic promotion.
- **Catalog mode only.** Promotion adds real columns via `ALTER TABLE`, so it requires a catalog target (DuckLake). Combining it with `parquet_export_path` (or the daemon's `--mode parquet`) is rejected at startup rather than ignored — there is no table to add the columns to. On an Iceberg REST catalog it works only if the catalog supports `ADD COLUMN`; otherwise the server logs a warning and disables promotion (ingest continues).
- **Type.** Promoted columns are `VARCHAR` (the JSON-extracted text).
- `otlp_server_list().promoted_columns_total` reports the promoted column count per signal.

The daemon exposes the same option as `--promote-resource-attributes` / `--promote-scope-attributes` (or `DUCKDB_OTLP_PROMOTE_RESOURCE_ATTRIBUTES` / `DUCKDB_OTLP_PROMOTE_SCOPE_ATTRIBUTES`), comma-separated. The daemon image must have the `json` extension available; if it cannot load it, promotion disables itself with a log.

## Attributes as VARIANT

`attributes_as_variant := true` makes the server create and fill every `*_attributes` column as DuckDB's [`VARIANT`](https://duckdb.org/docs/stable/sql/data_types/variant) type instead of `VARCHAR` holding JSON text. The readers take the same flag — see [Attributes as VARIANT](../schemas/#attributes-as-variant) for what changes about querying — and the daemon exposes it as `--attributes-as-variant` (or `DUCKDB_OTLP_ATTRIBUTES_AS_VARIANT=1`).

```sql
SELECT * FROM otlp_serve('otlp:0.0.0.0:4318', catalog := 'lake', attributes_as_variant := true);
```

- **It is part of the destination's shape, not a runtime preference.** The server creates its six signal tables with the column types the flag implies, and on startup it validates the types it finds. Pointing a server at tables created the other way fails with an error naming the migration rather than writing the wrong encoding into them. Under `parquet_export_path` the same check reads the existing dataset instead: files cannot be migrated in place, and two encodings under one root make the older rows unreadable (`union_by_name` hands them back as VARIANT *strings*), so a flipped flag is refused and the remedy is a new export root.
- **Migrating an existing catalog** means one `ALTER` per bag column per signal table, with an explicit `USING` — a bare cast from `VARCHAR` would store the whole JSON document as a VARIANT *string* instead of parsing it:

  ```sql
  ALTER TABLE lake.main.otlp_logs
    ALTER COLUMN resource_attributes SET DATA TYPE VARIANT
    USING CAST(resource_attributes AS JSON)::VARIANT;
  ```

- **A DuckDB file catalog needs storage version 1.5.0.** `VARIANT` columns cannot be stored in a DuckDB database file written at an older storage version, and new files default to a backwards-compatible one — so a plain `--database`/`ATTACH` catalog fails at table creation with *"VARIANT columns are not supported in storage versions prior to v1.5.0"*. Create that database with `ATTACH 'x.duckdb' (STORAGE_VERSION 'v1.5.0')` (an `--init-sql` script is the place for it). DuckLake and `parquet_export_path` write Parquet and are unaffected.
- **DuckLake and Parquet.** `VARIANT` columns are written to Parquet in the [Parquet variant encoding](https://duckdb.org/docs/stable/sql/data_types/variant) and shredded into typed subcolumns, which is where the storage win comes from. DuckLake stores them natively from DuckLake 0.4; a catalog older than that cannot hold the column type.
- **Text output casts back to JSON.** `duckdb-otlp export --format json` (and `--to x.json`) casts `VARIANT` columns to `JSON` for you, so the file holds real JSON values. In your own SQL — `duckdb-otlp query --format json`, or any `COPY ... TO '*.json'` you write — cast the bag yourself with `CAST(resource_attributes AS JSON)`: DuckDB's json writer otherwise emits a `VARIANT` through its display form, as the string `"{'k': 1}"`. Parquet stores `VARIANT` natively and needs no cast.
- **Attribute promotion still works.** With `VARIANT` bags the promoted column is filled by `CAST(variant_extract(bag, 'key') AS VARCHAR)` rather than `json_extract_string`, and stays `VARCHAR` either way.
- **What it costs, measured.** The Rust backend has no VARIANT encoder: it emits each bag as JSON either way, and the extension converts that text to `VARIANT` once per chunk on the way in. On a 4-core machine, 80k log records with 8 resource + 5 record attributes each, ingested over OTLP/HTTP into `parquet` mode:

  | | JSON text | VARIANT | change |
  |---|---|---|---|
  | POST throughput | 31.8k rec/s | 29.9k rec/s | −6% |
  | Ingest incl. the final seal | 3.1 s | 4.4 s | +42% |
  | Parquet on disk | 1.90 MB | 1.15 MB | **−40%** |

  Reading the same data back from files (200k records) costs +16% in the scan and −49% on disk. `scripts/benchmark_catalog_ingest.py --attributes-as-variant` runs the daemon e2e benchmark with the flag on.

- **Query speed depends on your DuckDB version, and the difference is large.** On DuckDB 1.5, shredding happens on *write* but a read reconstructs the whole `VARIANT` per row, so extracting a key is several times slower than `json_extract_string` over the same data. DuckDB 2.0 adds shredded execution straight from storage and extraction pushdown into scans, which turns that around: the same query reads one shredded subcolumn instead of rebuilding a bag. Measured on the same Parquet files, 200k log records, by the same query on both engines:

  | Query | 1.5: JSON → VARIANT | 2.0: JSON → VARIANT |
  |---|---|---|
  | String key, repeated resource bags | 3 ms → 445 ms | 28 ms → **3 ms** |
  | String key, per-record bags | 45 ms → 256 ms | 52 ms → **3 ms** |
  | `GROUP BY` an attribute | 34 ms → 483 ms | 30 ms → **4 ms** |
  | Numeric key (`>= 500`) | 59 ms → 317 ms | 53 ms → 124 ms |

  So on 1.5 the flag buys smaller files and typed values but costs query speed; on 2.0 it is a large win on string keys and `GROUP BY`, while numeric extraction stays somewhat behind. [Attribute promotion](#attribute-promotion) remains the answer for the handful of keys you filter on constantly, on either version.

## URI scheme

The scheme binds the URI to a serve function (no mixing): `otlp:` is `otlp_serve`, the OTLP server (OTLP/HTTP by default on port 4318, or OTLP/gRPC unary with `transport := 'grpc'`); `otap:` is `otap_serve`, the [OTAP/Arrow gRPC server](#grpc-transport) (port 4317). Each function rejects the other's scheme.

| Form | Example | Server |
|------|---------|--------|
| `otlp:host:port` | `otlp:localhost:4318` | OTLP/HTTP (or OTLP/gRPC with `transport := 'grpc'`) |
| `otlp://host:port` | `otlp://127.0.0.1:4318` | OTLP/HTTP |
| `otap:host:port` | `otap:localhost:4317` | OTAP/Arrow (gRPC) |
| IPv6 (host in brackets) | `otlp:[::1]:4318` | OTLP/HTTP |

The scheme is part of the canonical key, so `otap:host:4317` and `otlp:host:4318` are distinct servers in `otlp_server_list` / `otlp_stop` / `otlp_flush`. By default, `otlp_serve`/`otap_serve` allow only `localhost`, `127.0.0.1`, and `::1`. To bind to any other host (for example `0.0.0.0` to accept remote exporters), pass `allow_other_hostname := true`; non-localhost hosts are rejected before a socket is bound.

The scalar function **`otlp_uri_parser(uri)`** parses an `otlp:`/`otap:` URI and returns a `STRUCT(host VARCHAR, port USMALLINT, ipv6 BOOLEAN, url VARCHAR)` — the same parsing the serve functions use, useful for validating a URI up front.

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

### Disabling authentication

`disable_auth := true` turns auth off entirely: the server accepts every request without checking any header, mints no token, and returns an empty `auth_token`. The same flag applies to the gRPC transports (`otlp_serve(transport := 'grpc')` and `otap_serve`), which otherwise reject a bad token with `UNAUTHENTICATED`.

This exists for two cases: trusted, network-isolated deployments, and OTLP/OTAP producers that have no way to attach a bearer token — notably the otel-arrow OTAP exporter, which has no header/auth configuration. It is **off by default**; only enable it when the listener is otherwise protected (loopback bind, private network, or a sidecar that terminates auth), since anyone who can reach the port can write to your tables.

## gRPC transport

There are two gRPC entry points, each bound to its own scheme and serving a **disjoint** service family, for all six signals:

- **`otlp_serve('otlp:...', transport := 'grpc')`** — standard **OTLP/gRPC** unary `Export`. `otlp_serve` defaults to `otlp:localhost:4318` (HTTP); point it at the conventional gRPC port for this, e.g. `otlp:localhost:4317`.
- **`otap_serve('otap:...')`** — canonical **OTAP/Arrow** bidirectional streaming. Defaults to `otap:localhost:4317`; gRPC-only.

Both share everything below the wire: the same parameters, catalog/Parquet targeting, token auth, buffered group-commit ("seal") path, backpressure cap, and lifecycle functions (`otlp_flush` / `otlp_stop` / `otlp_server_list` / `otlp_seal_list`). Because the service sets are disjoint, calling the other family on a listener returns `UNIMPLEMENTED`.

| Server | Service | RPC | Wire format |
|--------|---------|-----|-------------|
| `otlp_serve(transport := 'grpc')` | `opentelemetry.proto.collector.{logs,trace,metrics}.v1.{Logs,Trace,Metrics}Service` | `Export` (unary) | Standard **OTLP/gRPC** |
| `otap_serve` | `opentelemetry.proto.experimental.arrow.v1.Arrow{Logs,Traces,Metrics}Service` | `Arrow{Logs,Traces,Metrics}` (bidirectional streaming) | **OTAP/Arrow** (`stream BatchArrowRecords` → `stream BatchStatus`) |

Notes:

- **Auth** is the bearer token in the gRPC `authorization` metadata (`Bearer <token>`); a bad token is rejected with `UNAUTHENTICATED`, unless [`disable_auth`](#disabling-authentication) is set. Backpressure surfaces as `RESOURCE_EXHAUSTED` (the gRPC equivalent of HTTP `503`).
- **OTAP streaming** keeps one stateful decoder per stream, so later messages can reuse the Arrow dictionaries/schemas established by earlier ones. The server returns one `BatchStatus` per received `BatchArrowRecords`. A message that fails to decode is nacked and the stream is closed (the decoder is poisoned); a message nacked for backpressure leaves the stream open.
- **Metrics** decode into up to four shapes per message, each buffered independently — a backpressure nack partway through a metrics message can leave earlier shapes buffered.
- The gRPC stack (tokio + tonic) is statically linked into the extension; it adds no new shared-library dependencies and, like the HTTP server, is native-only (absent from WASM builds).

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
   - an explicit [`otlp_flush`](#otlp_flushuri).
3. For a DuckLake target, each batch commit writes **one Parquet data file per signal** plus one snapshot.
4. For named catalogs, the server may follow successful automatic row-seals (and, when a row-seal is pending, the idle writer after about 10 minutes) with best-effort, non-force `CHECKPOINT <catalog>` outside the ingest transaction when recent ingest rate and pending bytes leave ample admission headroom. Treat this as internal scheduling. The server skips the default catalog. The hook also skips explicit `otlp_flush`, sustained high ingest, high pending buffered bytes, and shutdown drains; unsupported checkpoint implementations log once and disable the hook for that server.

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
