# Live Ingest Quickstart

Stream live OTLP/HTTP exports straight into DuckDB tables. This walkthrough starts an ingest server with SQL, POSTs a sample payload with `curl`, and queries the rows that landed.

> Requires the **native** extension (the server is not available in WASM builds).

## Step 1: Start the server

From a DuckDB shell with the extension loaded:

```sql
INSTALL otlp FROM community;
LOAD otlp;

-- Start a server on localhost:4318 with a fixed token.
SELECT listen_url, auth_token
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

**Output:**
```
┌────────────────────────┬──────────────────┐
│       listen_url       │    auth_token    │
├────────────────────────┼──────────────────┤
│ http://localhost:4318  │ dev-token-123456 │
└────────────────────────┴──────────────────┘
```

If you omit `token`, a random 32-character token is generated and returned in `auth_token` — copy it from the result.

`otlp_serve` also creates the target tables (`otlp_logs`, `otlp_traces`, and the four `otlp_metrics_*` tables) in the `main` schema. Leave this DuckDB session running; the server lives as long as the database is open.

## Step 2: POST a payload with curl

In another terminal, POST a sample OTLP/JSON logs file. The `Content-Type` selects the parser and the `Authorization` header carries the token:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Content-Type: application/x-ndjson' \
  -H 'Authorization: Bearer dev-token-123456' \
  --data-binary @test/data/logs_simple.jsonl
```

**Response:**
```json
{"status":"ok","rows":1,"batches":1}
```

(`test/data/logs_simple.jsonl` is in the [extension repo](https://github.com/smithclay/duckdb-otlp). `application/x-ndjson` is the content type for newline-delimited OTLP/JSON; use `application/json` for a single JSON document or `application/x-protobuf` for protobuf.)

You can confirm the server is up without auth:

```bash
curl -sS http://localhost:4318/healthz
# {"status":"ok"}
```

## Step 3: Query the ingested rows

Back in the DuckDB session, query the table the rows landed in:

```sql
SELECT count(*) FROM otlp_logs;
```

```
┌──────────────┐
│ count_star() │
├──────────────┤
│      1       │
└──────────────┘
```

Check the live server counters:

```sql
SELECT total_requests, total_rows, is_listening FROM otlp_server_list();
```

## Step 4: Stop the server

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
-- Stopped listening on otlp:localhost:4318
```

Servers are also stopped automatically when the DuckDB database closes.

## Endpoints at a glance

| Method | Path | Lands in |
|--------|------|----------|
| POST | `/v1/logs` | `otlp_logs` |
| POST | `/v1/traces` | `otlp_traces` |
| POST | `/v1/metrics` | `otlp_metrics_gauge`, `otlp_metrics_sum`, `otlp_metrics_histogram`, `otlp_metrics_exp_histogram` |
| GET | `/healthz` | (liveness, no auth) |

## Next steps

- **[Serve Reference](../reference/serve.md)** — all parameters, content types, auth, status codes, and the concurrency/durability model.
- **[Architecture](../architecture.md#otlp-http-ingest-server)** — how the server is wired internally.
- Point a real OpenTelemetry Collector or SDK exporter at `http://localhost:4318` (the standard OTLP/HTTP port), setting the bearer token or `x-api-key` header.
