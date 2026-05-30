# Live Ingest Quickstart

Stream live OTLP/HTTP exports into a **DuckLake lakehouse** — Parquet data files tracked by a catalog. This walkthrough attaches a DuckLake catalog, starts an ingest server pointed at it, POSTs a sample payload with `curl`, forces a seal, and queries the rows.

> Requires the **native** extension (the server is not available in WASM builds). Ingest is HTTP-only (no gRPC).

## Step 1: Attach a DuckLake catalog and start the server

From a DuckDB shell with the extension loaded:

```sql
INSTALL otlp FROM community;
LOAD otlp;

-- Attach a DuckLake lakehouse: a catalog plus a Parquet data directory.
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'otlp_data/');

-- Start a server that streams OTLP straight into the 'lake' catalog.
SELECT listen_url, auth_token, catalog_name
FROM otlp_serve('otlp:localhost:4318', catalog := 'lake', token := 'dev-token-123456');
```

**Output:**
```
┌────────────────────────┬──────────────────┬──────────────┐
│       listen_url       │    auth_token    │ catalog_name │
├────────────────────────┼──────────────────┼──────────────┤
│ http://localhost:4318  │ dev-token-123456 │ lake         │
└────────────────────────┴──────────────────┴──────────────┘
```

If you omit `token`, a random 32-character token is generated and returned in `auth_token` — copy it from the result.

`otlp_serve` creates the target tables (`otlp_logs`, `otlp_traces`, and the four `otlp_metrics_*` tables) in `lake.main`. Leave this DuckDB session running; the server lives as long as the database is open.

> **No lakehouse?** Omit `catalog` to land rows in the connection's default (in-memory/file) catalog instead — see [Ephemeral ingest](#ephemeral-ingest-no-lakehouse).

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
{"status":"buffered","rows":1,"batches":1}
```

A `202 Accepted` with `"status":"buffered"` means the rows were validated and accepted into the server's in-memory buffer — **they are not durable yet.** Buffered rows are group-committed ("sealed") into Parquet automatically or on demand with `otlp_flush`.

(`test/data/logs_simple.jsonl` is in the [extension repo](https://github.com/smithclay/duckdb-otlp). `application/x-ndjson` is the content type for newline-delimited OTLP/JSON; use `application/json` for a single JSON document or `application/x-protobuf` for protobuf.)

You can confirm the server is up without auth:

```bash
curl -sS http://localhost:4318/healthz
# {"status":"ok"}
```

## Step 3: Seal the buffer and query

Back in the DuckDB session, force a synchronous seal so the buffered rows are written to Parquet now:

```sql
SELECT status, sealed_rows FROM otlp_flush('otlp:localhost:4318');
```

```
┌─────────┬─────────────┐
│ status  │ sealed_rows │
├─────────┼─────────────┤
│ sealed  │      1      │
└─────────┴─────────────┘
```

Now query the rows in the lakehouse:

```sql
SELECT count(*) FROM lake.main.otlp_logs;
```

```
┌──────────────┐
│ count_star() │
├──────────────┤
│      1       │
└──────────────┘
```

Check live server counters and buffer state:

```sql
SELECT catalog_name, total_rows, buffered_rows, last_seal_age_ms, seals_total
FROM otlp_server_list();
```

## Step 4: Stop the server

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
-- Stopped listening on otlp:localhost:4318
```

`otlp_stop` (and `otlp_flush`) **seal remaining buffered rows first**, so nothing is lost. A plain database/connection close stops the server but does **not** seal — buffered-but-unsealed rows are dropped — so call `otlp_flush` or `otlp_stop` before closing the database to guarantee durability.

## Ephemeral ingest (no lakehouse)

For quick local exploration without a lakehouse, omit `catalog` to land rows in the connection's default catalog (in-memory, or whatever file you opened DuckDB with):

```sql
SELECT listen_url, auth_token FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
-- ... POST as in Step 2 (still returns 202 buffered) ...
SELECT status FROM otlp_flush('otlp:localhost:4318');  -- seal into the default catalog
SELECT count(*) FROM otlp_logs;
```

Ingest is buffered here too: a POST returns `202`, and rows become durable in that database at the next seal. This path is fast and zero-setup but tied to a single database — use a DuckLake catalog for a durable, queryable lakehouse.

## Endpoints at a glance

| Method | Path | Lands in |
|--------|------|----------|
| POST | `/v1/logs` | `otlp_logs` |
| POST | `/v1/traces` | `otlp_traces` |
| POST | `/v1/metrics` | `otlp_metrics_gauge`, `otlp_metrics_sum`, `otlp_metrics_histogram`, `otlp_metrics_exp_histogram` |
| GET | `/healthz` | (liveness, no auth) |

## Next steps

- **[Serve Reference](../reference/serve.md)** — all parameters, catalog targeting, content types, auth, status codes, and the buffered seal/durability model.
- **[Architecture](../architecture.md#otlp-http-ingest-server)** — how the buffer + single-writer sealer are wired internally.
- Point a real OpenTelemetry Collector or SDK exporter at `http://localhost:4318` (the standard OTLP/HTTP port), setting the bearer token or `x-api-key` header.
- Schedule `otlp_flush('otlp:localhost:4318')` periodically when readers need freshly durable rows.
