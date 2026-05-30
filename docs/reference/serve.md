# OTLP HTTP Ingest Server

The extension can run an embedded HTTP server that accepts live OTLP/HTTP exports and streams them into DuckDB. Point any OpenTelemetry exporter at the server and rows are buffered and group-committed to your target — most usefully a **DuckLake lakehouse** (Parquet on local/object storage with a catalog), or the connection's default in-memory/file catalog.

> **Not available in WASM builds.** The server requires the native extension. Live ingestion is HTTP-only (no gRPC).

For a copy-pasteable walkthrough, see the [Live Ingest Quickstart](../quickstart/serve.md). For how it works internally, see [Architecture](../architecture.md#otlp-http-ingest-server).

## Functions

The extension registers four lifecycle functions:

| Function | What it does |
|----------|-------------|
| `otlp_serve([uri], ...)` | Start an HTTP server and create/validate target tables. Returns one row describing the listener. |
| `otlp_flush(uri, ...)` | Force a synchronous seal of the buffer (optionally compact). Returns seal stats. |
| `otlp_stop(uri)` | Stop the server listening on `uri` (seals remaining rows first). Returns a status string. |
| `otlp_server_list()` | List all running servers with live counters, buffer state, and health. |

### `otlp_serve([uri], ...)`

Starts an OTLP/HTTP ingest server bound to `uri`. The `uri` argument is optional; with no argument it defaults to `otlp:localhost:4318`.

```sql
-- Stream into an attached DuckLake catalog
SELECT * FROM otlp_serve('otlp:localhost:4318', catalog := 'lake', token := 'my-dev-token-123456');
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` (positional) | VARCHAR | `otlp:localhost:4318` | Listen URI. See [URI scheme](#uri-scheme). |
| `catalog` | VARCHAR | *(default catalog)* | Name of the target catalog. Empty means the connection's **default catalog** (in-memory or file). Set to an **attached DuckLake catalog name** to stream OTLP into a lakehouse. See [Catalog targeting](#catalog-targeting). |
| `token` | VARCHAR | *(random, see below)* | Auth token clients must present. Must be at least 16 characters. If omitted, a random 32-hex-character token is generated and returned in `auth_token`. |
| `schema` | VARCHAR | `main` | Schema (within the catalog) that holds the target tables. |
| `create_tables` | BOOLEAN | `true` | Create the six target tables if they don't exist. When `false`, the tables must already exist with the expected columns or `otlp_serve` fails fast. |
| `allow_other_hostname` | BOOLEAN | `false` | Allow binding to a non-localhost host. By default only `localhost`, `127.0.0.1`, and `::1` are permitted. |
| `max_body_bytes` | UBIGINT | `16777216` (16 MiB) | Reject request bodies larger than this with `413`. Must be greater than zero. |
| `seal_target_bytes` | UBIGINT | `67108864` (64 MiB) | Seal the buffer once buffered bytes reach this size. See [Durability and the seal model](#durability-and-the-seal-model). |
| `seal_max_age_ms` | UBIGINT | `5000` | Seal the buffer once the oldest buffered row reaches this age (ms). |
| `max_buffered_bytes` | UBIGINT | `536870912` (512 MiB) | Backpressure cap. POSTs that would exceed this return `503`. |

**Output columns** (one row):

| Column | Type | Description |
|--------|------|-------------|
| `listen_uri` | VARCHAR | The `otlp:` URI the server is bound to. |
| `listen_url` | VARCHAR | The equivalent `http://` base URL (POST endpoints hang off this). |
| `auth_token` | VARCHAR | The token clients must present (the value you passed, or the generated one). |
| `catalog_name` | VARCHAR | Target catalog. Empty for the connection's default catalog. |
| `schema_name` | VARCHAR | Schema holding the target tables. |
| `logs_table` | VARCHAR | `otlp_logs` |
| `traces_table` | VARCHAR | `otlp_traces` |
| `metrics_gauge_table` | VARCHAR | `otlp_metrics_gauge` |
| `metrics_sum_table` | VARCHAR | `otlp_metrics_sum` |
| `metrics_histogram_table` | VARCHAR | `otlp_metrics_histogram` |
| `metrics_exp_histogram_table` | VARCHAR | `otlp_metrics_exp_histogram` |

Starting a second server on the same URI fails (`OTLP server already exists`). The server's lifetime is tied to the DuckDB `DatabaseInstance`: all servers are stopped automatically when the database closes, but their buffers are **not** sealed at that point (see Durability below) — `otlp_flush`/`otlp_stop` before closing to avoid losing buffered rows.

### `otlp_flush(uri, ...)`

Forces a **synchronous seal**: the server's in-memory buffer is committed to the target in one transaction before the function returns. Use it to make buffered rows durable on demand (e.g. right before querying), and — with `checkpoint := true` — to compact a DuckLake catalog.

```sql
-- Force a seal and compact the DuckLake catalog
SELECT * FROM otlp_flush('otlp:localhost:4318', checkpoint := true);
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` (positional) | VARCHAR | *(required)* | Listen URI of the server to flush. |
| `checkpoint` | BOOLEAN | `false` | After the seal, also run DuckLake compaction: `CALL ducklake_merge_adjacent_files('<catalog>')` then `CHECKPOINT '<catalog>'`. Merges the small per-seal Parquet files. |

**Output columns** (one row):

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `sealed` on success, or `No server found listening on <uri>`. |
| `sealed_rows` | UBIGINT | Rows committed by this seal. |
| `seals_total` | UBIGINT | Total seals performed by this server since startup. |
| `error` | VARCHAR | Seal error detail, or `NULL` if none. |

Run `otlp_flush(uri, checkpoint := true)` periodically (e.g. from a scheduled query) to keep DuckLake file counts low — compaction is **off by default**, so without it each seal leaves a separate small Parquet file per signal.

### `otlp_stop(uri)`

Stops the server listening on `uri` and frees the port. Any buffered rows are **sealed first**, so a graceful stop loses no data.

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

**Output column:**

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `Stopped listening on <uri>` if a server was stopped, or `No server found listening on <uri>` if none matched. |

### `otlp_server_list()`

Lists every running server with live counters and buffer state. Takes no arguments.

```sql
SELECT listen_uri, catalog_name, total_rows, buffered_rows, last_seal_age_ms, is_listening
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
| `active_requests` | UBIGINT | Requests currently being handled. |
| `total_requests` | UBIGINT | Requests handled since startup (includes failures). |
| `total_rows` | UBIGINT | Rows **accepted** (buffered) since startup. Once the buffer drains, this equals the rows sealed. A `/v1/metrics` request counts rows across all four metric tables. |
| `buffered_rows` | UBIGINT | Rows currently in the buffer, not yet sealed. |
| `buffered_bytes` | UBIGINT | Approximate bytes currently buffered. |
| `last_seal_age_ms` | BIGINT | Age (ms) since the last seal, or `NULL` if never sealed. |
| `seals_total` | UBIGINT | Seals performed since startup. |
| `is_listening` | BOOLEAN | `false` once the listener has fallen over (e.g. an error after a successful bind). |
| `last_error` | VARCHAR | Last fatal listener error, or `NULL` if none. |
| `seal_last_error` | VARCHAR | Last seal error, or `NULL` if none. |

`is_listening` / `last_error` detect a dead listener; `seal_last_error` surfaces a failing writer (e.g. a catalog conflict).

## Catalog targeting

The target of a server is `<catalog>.<schema>.<table>`:

- **Default catalog** (`catalog` omitted): rows land in the connection's default catalog — an in-memory database, or whatever file you opened DuckDB with. This is the **ephemeral / no-lakehouse** path: fast and zero-setup, but tied to that one database. Ingest is still buffered (a POST returns `202`); rows become durable in that database at the next seal.
- **DuckLake catalog** (`catalog := '<attached_db>'`): rows stream straight into a [DuckLake](https://ducklake.select) lakehouse — Parquet data files on local or object storage, tracked by a catalog. Attach the catalog first, then name it:

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'otlp_data/');

CALL otlp_serve('otlp:localhost:4318', catalog := 'lake');
-- rows buffer and seal into Parquet under otlp_data/
SELECT count(*) FROM lake.main.otlp_logs;
```

Each seal writes **one Parquet data file per signal** plus one DuckLake snapshot — see [Durability and the seal model](#durability-and-the-seal-model).

## URI scheme

Listen URIs use the `otlp:` scheme:

| Form | Example |
|------|---------|
| `otlp:host:port` | `otlp:localhost:4318` |
| `otlp://host:port` | `otlp://127.0.0.1:4318` |
| IPv6 (host in brackets) | `otlp:[::1]:4318` |

Only `localhost`, `127.0.0.1`, and `::1` are allowed by default. To bind to any other host (for example `0.0.0.0` to accept remote exporters), pass `allow_other_hostname := true`. Non-localhost hosts are rejected before a socket is bound.

## HTTP endpoints

The `http://` base URL from `listen_url` exposes:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/logs` | Ingest logs into `otlp_logs`. |
| POST | `/v1/traces` | Ingest traces into `otlp_traces`. |
| POST | `/v1/metrics` | Ingest metrics. Fans out across all four metric tables: `otlp_metrics_gauge`, `otlp_metrics_sum`, `otlp_metrics_histogram`, `otlp_metrics_exp_histogram`. |
| GET | `/healthz` | Liveness probe. Returns `200` with `{"status":"ok"}`. No auth required. |

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

`Content-Encoding: identity` (or no header) is always accepted. `gzip` and `deflate` are accepted only when the extension is built with zlib support; otherwise they return `415`.

## Authentication

Every POST must present the configured token, via **either**:

- `Authorization: Bearer <token>` — the scheme is case-insensitive, or
- `x-api-key: <token>`

The two are checked independently, so a malformed `Authorization` header does not mask a valid `x-api-key`. A missing or invalid token returns `401`. Tokens are compared with a constant-time check.

Tokens must be at least 16 characters. Auto-generated tokens (when `token` is omitted) are 32 hex characters (128 bits of entropy).

## Responses and status codes

A successful POST is **buffered**, not committed, and returns `202 Accepted`:

```json
{"status":"buffered","rows":42,"batches":1}
```

A `202` means the rows were validated, converted, and accepted into the in-memory buffer — **they are not yet durable** (see below).

Errors return JSON shaped like `{"error":"<reason>","message":"<detail>"}`:

| Status | When |
|--------|------|
| `400` | OTLP body failed to parse (or other invalid input). |
| `401` | Missing or invalid auth token. |
| `413` | Body larger than `max_body_bytes`. |
| `415` | Unsupported `Content-Type` or `Content-Encoding`. |
| `503` | Buffer admission full (request would exceed `max_buffered_bytes`). Retry with backoff. |
| `500` | Internal error (also written to `duckdb_logs`). |

## Durability and the seal model

Ingest is **buffered and group-committed** for every target (default catalog and DuckLake alike). This avoids per-request tiny files and write conflicts: a single serialized writer means DuckLake's optimistic concurrency never has to retry.

**The flow:**

1. A POST reserves admission bytes, parses, converts, and appends rows into the relevant per-signal in-memory buffer, then returns `202`. The 128-thread worker pool does this concurrently; append locks only the target signal buffer.
2. A single background **sealer** thread group-commits the buffer to the target in **one transaction** when any trigger fires:
   - buffered bytes reach `seal_target_bytes` (default 64 MiB), or
   - the oldest buffered row reaches `seal_max_age_ms` (default 5000 ms), or
   - an explicit [`otlp_flush`](#otlp_flushuri-).
3. For a DuckLake target, each seal writes **one Parquet data file per signal** plus one snapshot.

**Durability contract:**

- A `202` is **not durable.** Rows become durable at the next seal — within `seal_max_age_ms`, or immediately on `otlp_flush`.
- A crash or hard kill loses buffered-but-unsealed rows (**at-most-once** for that window).
- `otlp_stop` and `otlp_flush` **seal remaining rows before returning**, so those lose nothing. A plain **database/connection close does NOT seal** (the drain runs after the DuckDB instance is torn down, when it can no longer write) — buffered-but-unsealed rows are dropped. Call `otlp_flush`/`otlp_stop` before closing the database to guarantee durability.

> A durable raw-spool journal for at-least-once delivery is a documented future enhancement, not yet implemented.

**Backpressure:** if admitting a request would exceed `max_buffered_bytes` (default 512 MiB) across in-flight and unsealed accepted payloads, the POST returns `503` before parse/transform work. Clients should retry with backoff.

**Keeping DuckLake tidy:** each seal leaves a small Parquet file per signal. Run `otlp_flush(uri, checkpoint := true)` periodically to merge them (`ducklake_merge_adjacent_files` + `CHECKPOINT`). Compaction is off unless you ask for it.

## Concurrency model

- The server runs a 128-thread httplib worker pool. Workers parse, convert, and buffer requests concurrently; each signal table has its own buffer lock.
- A single **sealer** thread is the only writer to the target catalog. Serializing writes is what lets DuckLake (which uses optimistic concurrency) avoid conflict retries and tiny-file churn.

## Verifying ingest under load

`make test` cannot issue HTTP POSTs, so the SQL logic tests cover only the lifecycle surface. To exercise the ingest hot path (auth, content-type handling, the metrics fan-out, buffering + sealing, and Arrow → DuckDB conversion under concurrency), run the manual concurrency harness:

```bash
uv run python test/manual/otlp_serve_concurrency.py

# Override the payload / concurrency:
OTLP_PAYLOAD=test/data/logs_simple.jsonl OTLP_CONCURRENCY=64 \
    uv run python test/manual/otlp_serve_concurrency.py

# Exercise the DuckLake path (writes Parquet under the given dir):
OTLP_DUCKLAKE_DIR=/tmp/otlp_lake \
    uv run python test/manual/otlp_serve_concurrency.py
```

It covers auth and validation errors, low-buffer backpressure, metrics fanout, stop-under-load, and concurrent flush/stop, then reconciles accepted rows against committed row counts. Run it against a TSan/ASan build to catch races.

## See also

- [Live Ingest Quickstart](../quickstart/serve.md) — start a DuckLake server and POST with `curl`.
- [Architecture](../architecture.md#otlp-http-ingest-server) — buffer, sealer, and `otlp_flush` internals.
- [Schema Reference](schemas.md) — columns of the target tables.
