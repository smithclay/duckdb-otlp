# Live Ingest Server

You can run an embedded server that accepts live OpenTelemetry exports and streams them into DuckDB. Point any OpenTelemetry exporter at the server. The server buffers rows, then commits them in batches to the connection's default catalog, a DuckLake lakehouse, or another attached writable catalog such as an Iceberg REST catalog.

`otlp_serve` uses the `otlp:` scheme. Choose the wire transport with the `transport` option.

| Transport | Wire protocols |
|-----------|----------------|
| `http` (default) | **OTLP/HTTP** (JSON, NDJSON, protobuf) |
| `grpc` | **OTLP/gRPC** unary `Export` and **OTAP/Arrow** bidirectional streaming on the same port |

A **server** is one ingest pipeline — one set of in-memory buffers, one admission cap, one writer, and one catalog-maintenance schedule — fed by one or more **listeners**. Pass a list of URIs to start several listeners on one server, for example HTTP and gRPC into the same catalog:

```sql
SELECT listen_uri, transport
FROM otlp_serve(['otlp:localhost:4318', 'otlp:localhost:4317'], transport := ['http', 'grpc'],
                catalog := 'lake', token := 'my-dev-token-123456');
```

Each catalog target (catalog + schema, or Parquet export root) has at most one server, so it has a single writer. List every listener URI for the target in one `otlp_serve` call.

Native extension builds include the server; WASM builds omit it entirely (no HTTP and no gRPC). See [gRPC transport](#grpc-transport) for the OTLP/gRPC and OTAP/Arrow service contracts.

For a runnable walkthrough, see the [Live Ingest Quickstart](../../quickstart/serve/). For lakehouse examples, see [Stream to Local DuckLake](../../guides/stream-to-local-ducklake/), [Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/), [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/), and [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/). For plain files or object storage, see [Stream to Parquet](../../guides/stream-to-parquet/). For the implementation model, see [Architecture](../../architecture/#otlp-http-ingest-server).

## Command-line interface

The `duckdb-otlp` CLI runs the live receiver on macOS and Linux. A bare invocation
starts a local receiver with the default settings. Use the `otlp` extension in
DuckDB to read files and run queries. See the [CLI Reference](../cli/) for commands,
flags, and environment variables.

```sh
duckdb-otlp                                  # OTLP/HTTP 4318 + gRPC 4317 on loopback
duckdb-otlp --host 0.0.0.0 --token "$TOKEN"  # a non-loopback bind requires a token
```

## Docker daemon transports

The image starts OTLP/HTTP on `0.0.0.0:4318` (`DUCKDB_OTLP_HTTP_PORT=4318`). Each
port setting selects its listener; `0` turns one off:

| Setting | Listener |
| --- | --- |
| `--http PORT` / `DUCKDB_OTLP_HTTP_PORT` | OTLP/HTTP |
| `--grpc PORT` / `DUCKDB_OTLP_GRPC_PORT` | OTLP/gRPC and OTAP/Arrow |

All of them bind `--host` / `DUCKDB_OTLP_HOST`. For example, accept both protocols
into the same local DuckLake:

```sh
docker run --rm -p 4317:4317 -p 4318:4318 \
  -e DUCKDB_MODE=local-ducklake \
  -e DUCKDB_OTLP_GRPC_PORT=4317 \
  -e DUCKDB_OTLP_TOKEN=replace-with-a-private-token \
  -v otlp-data:/data ghcr.io/smithclay/duckdb-otlp:latest
```

Both listeners feed **one server**: the same catalog, schema, bearer token,
buffers, writer, and maintenance schedule. `DUCKDB_OTLP_MAX_BUFFERED_BYTES` and
`DUCKDB_OTLP_SEAL_TARGET_BYTES` apply to the whole process, not to each
listener. Startup must succeed for every listener. On shutdown the daemon stops
every listener, then drains the shared buffers once. If a later listener cannot
start, the earlier ones are closed and the daemon exits with an error. Two
listeners on the same port are rejected.

The image's `doctor` command (also accepted as `healthcheck`) checks every enabled listener, using HTTP
`/readyz` for HTTP and TCP connect for gRPC. A TCP check confirms a bound socket,
not successful ingestion or durable writes. Because the listeners share one
server, the HTTP `/readyz` reports the commit health of rows from both
transports. `DUCKDB_OTLP_HTTP_THREADS` applies
only to the HTTP listener. Quack remains a separate opt-in administrative endpoint.

For Cloud Run, set `DUCKDB_OTLP_GRPC_PORT` and `DUCKDB_OTLP_HTTP_PORT=0`, expose its port as `h2c`, and use a compatible
probe instead of HTTP `/readyz` on that port. Cloud Run terminates TLS and
forwards cleartext HTTP/2 to the container. Its single ingress port does not
expose both daemon ports; running both does not multiplex the protocols
onto one port. See [Cloud Run HTTP/2 configuration](https://docs.cloud.google.com/run/docs/configuring/http2).

## Functions

The extension registers four server functions.

| Function | What it does |
|----------|-------------|
| `otlp_serve([uri], ...)` | Start an ingest server on one or more listeners and create or validate the target tables. Returns one row per listener. |
| `otlp_flush([listen_uri])` | Commit buffered rows synchronously for the selected server or every server. Returns commit stats per server. |
| `otlp_stop([listen_uri])` | Stop the selected server or every server, committing buffered rows first. Returns a status and dropped-row count per server. |
| `otlp_server_list()` | List every running listener with its server's live counters, buffer state, and health. |

`otlp_flush`, `otlp_stop`, and `otlp_server_list` cover every transport. Any listener’s canonical URI names its whole server.

### `otlp_serve([uri], ...)`

Starts an ingest server with a listener on `uri`. The `uri` argument is optional; with no argument it defaults to `otlp:localhost:4318`. Pass a `VARCHAR[]` list to start several listeners on the same server.

```sql
-- Stream into an attached catalog
SELECT * FROM otlp_serve('otlp:localhost:4318', catalog := 'lake', token := 'my-dev-token-123456');
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` (positional) | VARCHAR or VARCHAR[] | `otlp:localhost:4318` | Listen URI, or a list of distinct listen URIs that feed one server. See [URI scheme](#uri-scheme). |
| `transport` | VARCHAR or VARCHAR[] | `http` | `http` (OTLP/HTTP) or `grpc` (OTLP/gRPC and OTAP/Arrow). A single value applies to every URI. A list must have one entry per URI. |
| `catalog` | VARCHAR | *(default catalog)* | Name of the target catalog. Empty means the connection's **default catalog** (in-memory or file). Set this to an attached writable catalog such as DuckLake or an Iceberg REST catalog to stream telemetry into a lakehouse. See [Catalog targeting](#catalog-targeting). |
| `token` | VARCHAR | *(random, see below)* | Auth token clients must present. Must be at least 16 characters. If you omit it, `otlp_serve` generates a random 32-hex-character token and returns it in `auth_token`. Ignored when `disable_auth := true`. |
| `disable_auth` | BOOLEAN | `false` | Accept every request **without** checking the token. No token is generated or validated, and `auth_token` comes back empty. Opt-in for trusted local networks and for producers that cannot attach a bearer token (e.g. the otel-arrow OTAP exporter). See [Authentication](#authentication). |
| `schema` | VARCHAR | `main` | Schema (within the catalog) that holds the target tables. |
| `parquet_export_path` | VARCHAR | *(none)* | Plain Parquet export root. When set, each seal writes the sealed rows to `<root>/<table>/year=YYYY/month=MM/day=DD/*.parquet` as the **only** durable store (no local table copy); a read-only view per signal is created over the files for inspection (see `parquet_views`). Mutually exclusive with a `catalog` target. Export is **at-least-once** (a `COPY` cannot be rolled back). |
| `parquet_views` | BOOLEAN | `true` | With `parquet_export_path`: create the read-only inspection view for each signal after its first export. Creating a view reads the footer of every file already in that signal's dataset, so it happens at most once per server and is skipped when the schema already holds that name. Set `false` when nothing reads the views. The `duckdb-otlp` daemon passes `false` unless Quack is enabled. |
| `create_tables` | BOOLEAN | `true` | Create the six target tables if they don't exist. When `false`, the tables must already exist with the expected columns or `otlp_serve` fails fast. |
| `allow_other_hostname` | BOOLEAN | `false` | Allow binding to a non-localhost host. By default `otlp_serve` permits only `localhost`, `127.0.0.1`, and `::1`. |
| `max_body_bytes` | UBIGINT | `16777216` (16 MiB) | Reject request bodies larger than this with `413`. Must be greater than zero. |
| `http_threads` | UBIGINT | host-based bounded default | Worker threads for concurrent HTTP requests, per HTTP listener. Must be greater than zero when set. |
| `max_buffered_bytes` | UBIGINT | `536870912` (512 MiB) | Backpressure cap for the whole server, shared by every listener. Requests that would exceed it return `503` (gRPC: `RESOURCE_EXHAUSTED`). |
| `seal_target_bytes` | UBIGINT | `134217728` (128 MiB) | Request an asynchronous seal when admitted, uncommitted request bytes reach this threshold. Larger values write fewer, larger files at the cost of a larger in-memory crash-loss window (still bounded by `seal_max_age_ms`). Must be greater than zero. |
| `seal_max_age_ms` | BIGINT | `5000` | Request an asynchronous seal when the oldest buffered row reaches this age. Must be greater than zero. |
| `maintenance_interval_ms` | BIGINT | `300000` (5 min) | Named catalogs only. The most often the background writer runs `CHECKPOINT <catalog>`; it runs only after rows were committed since the last attempt. Must be greater than zero. |
| `promote_resource_attributes` | VARCHAR | *(none)* | Comma-separated **resource** attribute keys to promote into first-class columns at ingest. See [Attribute promotion](#attribute-promotion). Catalog mode only. |
| `promote_scope_attributes` | VARCHAR | *(none)* | Comma-separated **scope** attribute keys to promote into first-class columns at ingest. See [Attribute promotion](#attribute-promotion). Catalog mode only. |

**Output columns** (one row per listener):

| Column | Type | Description |
|--------|------|-------------|
| `listen_uri` | VARCHAR | The `otlp:` URI the listener is bound to. |
| `listen_url` | VARCHAR | The derived `http://host:port` URL. For gRPC clients, use the host and port. |
| `auth_token` | VARCHAR | The token clients must present (the value you passed, or the generated one). |
| `schema_name` | VARCHAR | Schema holding the target tables. |
| `catalog_name` | VARCHAR | Target catalog. Empty for the connection's default catalog. |
| `transport` | VARCHAR | `http` or `grpc`. |

Starting a second server on the same URI, or for the same catalog target, fails (`OTLP server already exists`). If any listener fails to bind, the listeners already started are closed and no server is registered. The DuckDB `DatabaseInstance` owns the server lifetime: DuckDB stops all servers when the database closes, but it does **not** commit their buffers at that point (see Durability below). Call `otlp_stop` before closing the database to avoid losing buffered rows — `otlp_stop()` with no argument covers every server, including any you did not start yourself.

### `otlp_flush()` and `otlp_flush(listen_uri)`

Commits accepted rows to their targets before returning and leaves the servers running. Catalog maintenance continues on its own schedule.

With no argument, it flushes every server on the database. Pass a listener’s `VARCHAR` URI to flush only its server. An unknown URI raises an error.

```sql
SELECT * FROM otlp_flush();
SELECT * FROM otlp_flush('otlp:localhost:4318');
```

**Output columns** (one row per server):

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `sealed` on success, `error` on a failed commit, or `No OTLP servers are running` when called without a URI and no servers are running. |
| `sealed_rows` | UBIGINT | Rows committed by this call. |
| `seals_total` | UBIGINT | Total batch commits performed by this server since startup. |
| `error` | VARCHAR | Commit error detail, or `NULL` if none. |

### `otlp_stop(listen_uri)`

Stops every listener of the server that owns `listen_uri` and commits the shared buffers once. An unknown URI raises an error.

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

### `otlp_stop()`

Stops **every** server on this database, committing each one before returning, and returns one row per server. The registry stays usable afterwards: this is a graceful drain, not teardown, so a later `otlp_serve` still works.

```sql
SELECT status, dropped_rows FROM otlp_stop();
```

Use this at shutdown to include servers started by another connection or by the daemon’s [`--init-sql`](../cli/) script. With no servers running it returns a single `No OTLP servers are running` row.

**Output columns (both forms):**

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `Stopped listening on <uri>[, <uri>...]` listing the listeners stopped. A stop-all row reports any close error. With no servers, stop-all reports `No OTLP servers are running`. |
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

The DuckDB `OTLP` log records seal attempts at DEBUG on success and WARNING on failure. `duration_ms` covers the whole attempt, including recovery on failure. `append_duration_ms` measures writing the buffered rows, and `commit_duration_ms` measures the transaction commit. Plain Parquet export has no transaction commit.

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

At most once per `maintenance_interval_ms` (5 minutes by default), the server runs best-effort maintenance with `CHECKPOINT lake`. DuckLake uses the catalog's `target_file_size` for compaction and its `expire_older_than` and `delete_older_than` settings to reclaim old snapshots and files. Set those catalog-wide options with `CALL lake.set_option(...)`. The daemon's DuckLake presets take `DUCKDB_OTLP_TARGET_FILE_SIZE` in bytes and `DUCKDB_OTLP_MAINTENANCE_RETENTION_MS` in milliseconds. See the [CLI reference](../cli/) for defaults.

  Several processes can share one DuckLake catalog (for example two receiver instances during a deploy). Their `CHECKPOINT`s can race: the loser fails with a catalog error such as PostgreSQL's `could not serialize access due to concurrent delete`. The server records this in `maintenance_contended_total` and tries again at the next interval. A losing checkpoint can leave Parquet files the catalog never tracked. After each successful DuckLake `CHECKPOINT`, the server runs `ducklake_delete_orphaned_files` using the catalog's `delete_older_than` setting.

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
- **The JSON blob is kept.** A promoted column is an accelerator, not a replacement — the original key stays in `resource_attributes`/`scope_attributes`. Rows written *before* a key was promoted read back `NULL` for its column, so query across old and new data with:

  ```sql
  COALESCE(resource_attr_deployment_environment,
           json_extract_string(resource_attributes, '$."deployment.environment"'))
  ```

- **No auto-discovery.** The promoted set is exactly what you list — there is no workload observation or automatic promotion.
- **Catalog mode only.** Promotion adds real columns via `ALTER TABLE`, so it requires a catalog target (DuckLake). It is ignored under `parquet_export_path`. On an Iceberg REST catalog it works only if the catalog supports `ADD COLUMN`; otherwise the server logs a warning and disables promotion (ingest continues).
- **Type.** Promoted columns are `VARCHAR` (the JSON-extracted text).
- `otlp_server_list().promoted_columns_total` reports the promoted column count per signal.

The daemon exposes the same option via `DUCKDB_OTLP_PROMOTE_RESOURCE_ATTRIBUTES` and `DUCKDB_OTLP_PROMOTE_SCOPE_ATTRIBUTES` (comma-separated). The daemon image must have the `json` extension available; if it cannot load it, promotion disables itself with a log.

## URI scheme

Both transports use the `otlp:` scheme. The host defaults to `localhost` and the port to `4318`. Use port `4317` for the conventional gRPC endpoint.

| Form | Example |
|------|---------|
| `otlp:host:port` | `otlp:localhost:4318` |
| `otlp://host:port` | `otlp://127.0.0.1:4318` |
| IPv6 (host in brackets) | `otlp:[::1]:4318` |

By default, `otlp_serve` allows loopback hosts. To bind to another host, such as `0.0.0.0`, pass `allow_other_hostname := true`.

The scalar function **`otlp_uri_parser(uri)`** parses an `otlp:` URI and returns a `STRUCT(host VARCHAR, port USMALLINT, ipv6 BOOLEAN, url VARCHAR)`.

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

`disable_auth := true` turns auth off entirely: the server accepts every request without checking any header, mints no token, and returns an empty `auth_token`. The flag applies to every listener and both gRPC service families.

This exists for two cases: trusted, network-isolated deployments, and OTLP/OTAP producers that have no way to attach a bearer token — notably the otel-arrow OTAP exporter, which has no header/auth configuration. It is **off by default**; only enable it when the listener is otherwise protected (loopback bind, private network, or a sidecar that terminates auth), since anyone who can reach the port can write to your tables.

## gRPC transport

Use `otlp_serve('otlp:localhost:4317', transport := 'grpc')` to serve both service families. They share the target tables, auth, buffers, and backpressure cap.

| Service | RPC | Wire format |
|---------|-----|-------------|
| `opentelemetry.proto.collector.{logs,trace,metrics}.v1.{Logs,Trace,Metrics}Service` | `Export` (unary) | **OTLP/gRPC** |
| `opentelemetry.proto.experimental.arrow.v1.Arrow{Logs,Traces,Metrics}Service` | `Arrow{Logs,Traces,Metrics}` (bidirectional streaming) | **OTAP/Arrow** (`stream BatchArrowRecords` → `stream BatchStatus`) |

`max_body_bytes` caps the size of each decoded gRPC message. `http_threads` tunes the HTTP worker pool. Use a TCP connection to probe the gRPC port. The `/healthz` and `/readyz` endpoints are on HTTP listeners.

- **Auth** is the bearer token in the gRPC `authorization` metadata (`Bearer <token>`); a bad token is rejected with `UNAUTHENTICATED`, unless [`disable_auth`](#disabling-authentication) is set. Backpressure surfaces as `RESOURCE_EXHAUSTED` (the gRPC equivalent of HTTP `503`), a batch that fails to convert as `INVALID_ARGUMENT` (HTTP `400`), and a server-side failure as `INTERNAL` (HTTP `500`, also written to `duckdb_logs`).
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

1. A POST reserves admission bytes and stages decoded rows. The worker appends them to the shared in-memory buffers under one lock, then returns `202`.
2. A single writer swaps out the buffers at each seal. It commits all signals in one transaction for a catalog target, or writes each signal separately for direct Parquet export. A seal runs when any trigger fires:
   - admitted request-body bytes reach the internal size threshold, 128 MiB today,
   - the oldest buffered row reaches the internal age limit, about 5 seconds today, or
   - an explicit [`otlp_flush`](#otlp_flush-and-otlp_flushlisten_uri).
3. For a DuckLake target, each batch commit writes **one Parquet data file per signal** plus one snapshot.
4. For named catalogs, the writer runs best-effort, non-force `CHECKPOINT <catalog>` outside the ingest transaction at most once per `maintenance_interval_ms`, when rows were committed since the last attempt. The server skips the default catalog, explicit `otlp_flush`, and shutdown drains; unsupported checkpoint implementations log once and disable maintenance for that server. While a checkpoint runs, commits wait for it and ingest keeps buffering against `max_buffered_bytes`.

**Durability contract:**

- A `202` is **not durable.** Rows become durable at the next background commit, on `otlp_stop`, or on `otlp_flush`.
- A crash or hard kill loses buffered-but-uncommitted rows (**at-most-once** for that window).
- Call `otlp_stop()` before closing the database so every server can drain while DuckDB can still write. Check the returned status and `dropped_rows` for drain failures. Use `otlp_flush()` to commit buffered rows and leave the servers running. A failed flush reports `error` and leaves uncommitted rows buffered for retry.

The project tracks a future durable raw-spool journal for at-least-once delivery.

**Backpressure:** if admitting a request would exceed `max_buffered_bytes` (default 512 MiB) across in-flight and uncommitted accepted payloads, the POST returns `503` before parse/transform work. Clients should retry with backoff.

**Seal cadence:** `seal_target_bytes` and `seal_max_age_ms` are size and age triggers for the single asynchronous writer. They control batching latency and file/transaction size; they do not raise durable write throughput. `max_buffered_bytes` remains the separate admission cap.

DuckLake maintenance runs outside the ingest transaction, so a failed checkpoint leaves committed rows intact. `otlp_flush` commits buffered rows. The scheduled `CHECKPOINT` handles compaction and cleanup.

## Concurrency model

- A bounded httplib worker pool parses and converts HTTP requests concurrently. Workers hold the shared buffer lock only when appending staged rows. In the daemon, set `DUCKDB_OTLP_HTTP_THREADS` to override the host-based default.
- A single background writer thread writes to the target catalog. Serial writes let DuckLake, which uses optimistic concurrency, avoid conflict retries and tiny-file churn.

## Verifying ingest under load

`GEN=ninja make test` runs the SQL logic tests and the C++ seal harness on supported native builds. The daemon suite in `test/server/` exercises the wire protocols and SIGTERM shutdown. The distribution workflow runs it in `docker-smoke`.

```bash
DUCKDB_OTLP_SERVER_BIN=build/release/extension/otlp/duckdb-otlp \
  uv run --with pytest --with grpcio --with opentelemetry-proto --with duckdb==2.0.0 pytest test/server
```

## See also

- [Live Ingest Quickstart](../../quickstart/serve/): POST one log to the default catalog with `curl`.
- [Stream to Local DuckLake](../../guides/stream-to-local-ducklake/): write live OTLP rows to local DuckLake.
- [Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/): write live OTLP rows to DuckLake with Neon and R2.
- [Stream to Parquet](../../guides/stream-to-parquet/): write live OTLP rows to partitioned Parquet files on disk or S3.
- [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/): write live OTLP rows to Amazon S3 Tables as an Iceberg catalog.
- [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/): write live OTLP rows to Cloudflare R2 Data Catalog as an Iceberg catalog.
- [Architecture](../../architecture/#otlp-http-ingest-server): buffer, background writer, and `otlp_flush` internals.
- [Schema Reference](../schemas/): columns of the target tables.
