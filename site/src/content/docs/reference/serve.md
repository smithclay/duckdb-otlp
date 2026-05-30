---
title: "OTLP HTTP Ingest Server"
---

The extension can run an embedded HTTP server that accepts live OTLP/HTTP exports and streams them into DuckDB. Point any OpenTelemetry exporter at the server and rows are buffered, then committed to your target in batches: the connection's default catalog, a DuckLake lakehouse, or another attached writable catalog such as an Iceberg REST catalog.

> **Not available in WASM builds.** The server requires the native extension. Live ingestion is HTTP-only (no gRPC).

For a copy-pasteable walkthrough, see the [Live Ingest Quickstart](../../quickstart/serve/). For lakehouse examples, see [Stream to DuckLake](../../guides/stream-to-ducklake/) and [Stream to Iceberg](../../guides/stream-to-iceberg/). For how it works internally, see [Architecture](../../architecture/#otlp-http-ingest-server).

## Functions

The extension registers four lifecycle functions:

| Function | What it does |
|----------|-------------|
| `otlp_serve([uri], ...)` | Start an HTTP server and create/validate target tables. Returns one row describing the listener. |
| `otlp_flush(uri)` | Optionally force a synchronous commit of buffered rows. Returns commit stats. |
| `otlp_stop(uri)` | Stop the server listening on `uri` (commits remaining rows first). Returns a status string. |
| `otlp_server_list()` | List all running servers with live counters, buffer state, and health. |

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
| `token` | VARCHAR | *(random, see below)* | Auth token clients must present. Must be at least 16 characters. If omitted, a random 32-hex-character token is generated and returned in `auth_token`. |
| `schema` | VARCHAR | `main` | Schema (within the catalog) that holds the target tables. |
| `create_tables` | BOOLEAN | `true` | Create the six target tables if they don't exist. When `false`, the tables must already exist with the expected columns or `otlp_serve` fails fast. |
| `allow_other_hostname` | BOOLEAN | `false` | Allow binding to a non-localhost host. By default only `localhost`, `127.0.0.1`, and `::1` are permitted. |
| `max_body_bytes` | UBIGINT | `16777216` (16 MiB) | Reject request bodies larger than this with `413`. Must be greater than zero. |
| `max_buffered_bytes` | UBIGINT | `536870912` (512 MiB) | Backpressure cap. POSTs that would exceed this return `503`. |

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

Starting a second server on the same URI fails (`OTLP server already exists`). The server's lifetime is tied to the DuckDB `DatabaseInstance`: all servers are stopped automatically when the database closes, but their buffers are **not** committed at that point (see Durability below). Call `otlp_stop` before closing the database to avoid losing buffered rows.

### `otlp_flush(uri)`

Forces a **synchronous commit**: the server's in-memory buffer is written to the target in one transaction before the function returns. This is optional for normal ingest because the background writer commits automatically and `otlp_stop` performs a final commit. Use `otlp_flush` only when readers need the latest accepted rows immediately while the server stays running.

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

Lists every running server with live counters and buffer state. Takes no arguments.

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
| `active_requests` | UBIGINT | Requests currently being handled. |
| `total_requests` | UBIGINT | Requests handled since startup (includes failures). |
| `total_rows` | UBIGINT | Rows **accepted** (buffered) since startup. Once the buffer drains, this equals the rows committed. A `/v1/metrics` request counts rows across all four metric tables. |
| `buffered_rows` | UBIGINT | Rows currently in the buffer, not yet committed. |
| `last_seal_age_ms` | BIGINT | Age (ms) since the last successful batch commit, or `NULL` if none has completed. |
| `seals_total` | UBIGINT | Batch commits performed since startup. |
| `is_listening` | BOOLEAN | `false` once the listener has fallen over (e.g. an error after a successful bind). |
| `last_error` | VARCHAR | Last fatal listener error, or `NULL` if none. |
| `seal_last_error` | VARCHAR | Last batch commit error, or `NULL` if none. |

`is_listening` / `last_error` detect a dead listener; `seal_last_error` surfaces a failing writer (e.g. a catalog conflict).

## Catalog targeting

The target of a server is `<catalog>.<schema>.<table>`:

- **Default catalog** (`catalog` omitted): rows land in the connection's default catalog — an in-memory database, or whatever file you opened DuckDB with. This is the **ephemeral / no-lakehouse** path: fast and zero-setup, but tied to that one database. Ingest is still buffered (a POST returns `202`); rows become durable in that database at the next background commit.
- **DuckLake catalog** (`catalog := '<attached_db>'`): rows stream straight into a [DuckLake](https://ducklake.select) lakehouse — Parquet data files on local or object storage, tracked by a catalog. Attach the catalog first, then name it:

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'otlp_data/');

CALL otlp_serve('otlp:localhost:4318', catalog := 'lake');
-- rows buffer and commit into Parquet under otlp_data/
SELECT count(*) FROM lake.main.otlp_logs;
```

Each batch commit writes **one Parquet data file per signal** plus one DuckLake snapshot — see [Durability and background commits](#durability-and-background-commits).
- **Iceberg REST catalog** (`catalog := '<attached_db>'`): rows stream into tables in an attached writable Iceberg REST catalog. Attach the catalog with DuckDB's `iceberg` extension, create the target schema, then pass the catalog and schema to `otlp_serve`; see [Stream to Iceberg](../../guides/stream-to-iceberg/).

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

## Durability and background commits

Ingest is **buffered and committed in batches** for every target. This avoids per-request tiny files and write conflicts: a single serialized writer means lakehouse catalogs do not receive concurrent writes from the ingest server.

**The flow:**

1. A POST reserves admission bytes, parses, converts, and appends rows into the relevant per-signal in-memory buffer, then returns `202`. The 128-thread worker pool does this concurrently; append locks only the target signal buffer.
2. A single background writer commits the buffer to the target in **one transaction** when any trigger fires:
   - admitted request-body bytes reach the internal size threshold, currently 64 MiB,
   - the oldest buffered row reaches the internal age limit, currently about 5 seconds, or
   - an explicit [`otlp_flush`](#otlp_flushuri-).
3. For a DuckLake target, each batch commit writes **one Parquet data file per signal** plus one snapshot.

**Durability contract:**

- A `202` is **not durable.** Rows become durable at the next automatic background commit, on `otlp_stop`, or immediately on `otlp_flush`.
- A crash or hard kill loses buffered-but-uncommitted rows (**at-most-once** for that window).
- `otlp_stop` and `otlp_flush` **commit remaining rows before returning**, so those lose nothing. A plain **database/connection close does NOT commit buffered rows** (the drain runs after the DuckDB instance is torn down, when it can no longer write) — buffered rows can be dropped. Prefer `otlp_stop` before closing the database; use `otlp_flush` only when the server should keep running but readers need durable rows now.

> A durable raw-spool journal for at-least-once delivery is a documented future enhancement, not yet implemented.

**Backpressure:** if admitting a request would exceed `max_buffered_bytes` (default 512 MiB) across in-flight and uncommitted accepted payloads, the POST returns `503` before parse/transform work. Clients should retry with backoff.

**Keeping DuckLake tidy:** each batch commit leaves a small Parquet file per signal. DuckLake compaction is separate from `otlp_flush`; run DuckLake maintenance explicitly when file counts need cleanup.

## Concurrency model

- The server runs a 128-thread httplib worker pool. Workers parse, convert, and buffer requests concurrently; each signal table has its own buffer lock.
- A single background writer thread is the only writer to the target catalog. Serializing writes is what lets DuckLake (which uses optimistic concurrency) avoid conflict retries and tiny-file churn.

## Verifying ingest under load

`make test` cannot issue HTTP POSTs, so the SQL logic tests cover only the lifecycle surface. To exercise the ingest hot path (auth, content-type handling, the metrics fan-out, buffering + batch commits, and Arrow → DuckDB conversion under concurrency), run the manual concurrency harness:

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

- [Live Ingest Quickstart](../../quickstart/serve/) — POST one log to the default catalog with `curl`.
- [Stream to DuckLake](../../guides/stream-to-ducklake/) — write live OTLP rows to DuckLake.
- [Stream to Iceberg](../../guides/stream-to-iceberg/) — write live OTLP rows to an Iceberg REST catalog.
- [Architecture](../../architecture/#otlp-http-ingest-server) — buffer, background writer, and `otlp_flush` internals.
- [Schema Reference](../schemas/) — columns of the target tables.
