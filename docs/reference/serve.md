# OTLP HTTP Ingest Server

The extension can run an embedded HTTP server that accepts live OTLP/HTTP exports and appends them into DuckDB tables. It is the streaming counterpart to the `read_otlp_*` file readers: instead of reading exported files, you point any OpenTelemetry exporter at the server and rows land in tables as requests arrive.

> **Not available in WASM builds.** The server requires the native extension. Live ingestion is HTTP-only (no gRPC).

For a copy-pasteable walkthrough, see the [Live Ingest Quickstart](../quickstart/serve.md). For how it works internally, see [Architecture](../architecture.md#otlp-http-ingest-server).

## Functions

The extension registers three lifecycle functions:

| Function | What it does |
|----------|-------------|
| `otlp_serve([uri], ...)` | Start an HTTP server and create/validate target tables. Returns one row describing the listener. |
| `otlp_stop(uri)` | Stop the server listening on `uri`. Returns a status string. |
| `otlp_server_list()` | List all running servers with live request/row counters and health. |

### `otlp_serve([uri], ...)`

Starts an OTLP/HTTP ingest server bound to `uri`. The `uri` argument is optional; with no argument it defaults to `otlp:localhost:4318`.

```sql
SELECT * FROM otlp_serve('otlp:localhost:4318', token := 'my-dev-token-123456');
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` (positional) | VARCHAR | `otlp:localhost:4318` | Listen URI. See [URI scheme](#uri-scheme). |
| `token` | VARCHAR | *(random, see below)* | Auth token clients must present. Must be at least 16 characters. If omitted, a random 32-hex-character token is generated and returned in `auth_token`. |
| `schema` | VARCHAR | `main` | Schema that holds the target tables. |
| `create_tables` | BOOLEAN | `true` | Create the six target tables if they don't exist. When `false`, the tables must already exist with the expected columns or `otlp_serve` fails fast. |
| `allow_other_hostname` | BOOLEAN | `false` | Allow binding to a non-localhost host. By default only `localhost`, `127.0.0.1`, and `::1` are permitted. |
| `max_body_bytes` | UBIGINT | `16777216` (16 MB) | Reject request bodies larger than this with `413`. Must be greater than zero. |

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

Starting a second server on the same URI fails (`OTLP server already exists`). The server's lifetime is tied to the DuckDB `DatabaseInstance`: all servers are stopped automatically when the database closes.

### `otlp_stop(uri)`

Stops the server listening on `uri` and frees the port.

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

**Output column:**

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR | `Stopped listening on <uri>` if a server was stopped, or `No server found listening on <uri>` if none matched. |

### `otlp_server_list()`

Lists every running server with live counters. Takes no arguments.

```sql
SELECT listen_uri, total_requests, total_rows, is_listening FROM otlp_server_list();
```

**Output columns** (one row per running server):

| Column | Type | Description |
|--------|------|-------------|
| `listen_uri` | VARCHAR | The `otlp:` listen URI. |
| `listen_url` | VARCHAR | The `http://` base URL. |
| `host` | VARCHAR | Bound host. |
| `port` | USMALLINT | Bound port. |
| `schema_name` | VARCHAR | Schema holding the target tables. |
| `active_requests` | UBIGINT | Requests currently being handled. |
| `total_requests` | UBIGINT | Requests handled since startup (includes failures). |
| `total_rows` | UBIGINT | Rows committed since startup. A `/v1/metrics` request counts rows across all four metric tables. |
| `is_listening` | BOOLEAN | `false` once the listener has fallen over (e.g. an error after a successful bind). |
| `last_error` | VARCHAR | Last fatal listener error, or `NULL` if none. |

`is_listening` and `last_error` let you detect a dead listener that is still registered.

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

Tables are created in the schema chosen by `otlp_serve(schema := ...)`.

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

Successful ingest returns `200` with a JSON summary:

```json
{"status":"ok","rows":42,"batches":1}
```

Errors return JSON shaped like `{"error":"<reason>","message":"<detail>"}`:

| Status | When |
|--------|------|
| `400` | OTLP body failed to parse (or other invalid input). |
| `401` | Missing or invalid auth token. |
| `413` | Body larger than `max_body_bytes`. |
| `415` | Unsupported `Content-Type` or `Content-Encoding`. |
| `500` | Internal error (also written to `duckdb_logs`). |

## Concurrency and durability

The model mirrors [`duckdb-quack`](https://github.com/hannes/quack):

- The server runs a 128-thread httplib worker pool. Each keep-alive connection holds a worker for its lifetime.
- Each worker thread owns its own long-lived DuckDB `Connection`, so requests parse, convert, and append in parallel. A `Connection` is never shared across threads.
- Each request runs in its own transaction: for a `/v1/metrics` request, all four signal tables commit or roll back together.
- A `200` means the rows were committed — ingest is durable per request. DuckDB's per-table append lock briefly serializes the commit step.

**Backpressure today** is bounded only by `max_body_bytes` and the worker-pool / keep-alive caps. There is no bounded request queue or `429`/`503` request-shedding yet; that is a tracked follow-up.

## Verifying ingest under load

`make test` cannot issue HTTP POSTs, so the SQL logic tests cover only the lifecycle surface. To exercise the ingest hot path (auth, content-type handling, the metrics fan-out, per-request transactions, and Arrow → DuckDB conversion under concurrency), run the manual concurrency harness:

```bash
uv run python test/manual/otlp_serve_concurrency.py

# Override the payload / concurrency:
OTLP_PAYLOAD=test/data/logs_simple.jsonl OTLP_CONCURRENCY=64 \
    uv run python test/manual/otlp_serve_concurrency.py
```

It fires N concurrent POSTs of a real OTLP payload, then reconciles `total_requests` / `total_rows` from `otlp_server_list()` against the committed row counts. Run it against a TSan/ASan build to catch races.

## See also

- [Live Ingest Quickstart](../quickstart/serve.md) — start a server and POST with `curl`.
- [Architecture](../architecture.md#otlp-http-ingest-server) — `OtlpServer` / `HttpOtlpServer` internals.
- [Schema Reference](schemas.md) — columns of the target tables.
