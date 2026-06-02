---
title: "Live Ingest Quickstart"
---

Start a local OTLP/HTTP endpoint, POST one log record, and query it from DuckDB.

This quickstart requires the **native** extension. WASM builds do not include the ingest server. Live ingestion uses HTTP.

## Step 1: Start the server

From a DuckDB shell with the extension loaded:

```sql
INSTALL otlp FROM community;
LOAD otlp;

SELECT listen_url, auth_token
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

**Output:**

```text
┌────────────────────────┬──────────────────┐
│       listen_url       │    auth_token    │
├────────────────────────┼──────────────────┤
│ http://localhost:4318  │ dev-token-123456 │
└────────────────────────┴──────────────────┘
```

Leave this DuckDB session running. With no `catalog` parameter, rows land in the connection's default catalog: in memory, or in the DuckDB file you opened.

## Step 2: POST one log

In another terminal:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer dev-token-123456' \
  -H 'Content-Type: application/json' \
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"curl-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from curl"}}]}]}]}'
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

A `202 Accepted` with `"status":"buffered"` means the server parsed the row and accepted it into the in-memory buffer. You can query it after the next background commit, after `otlp_stop`, or after `otlp_flush`.

You can confirm the server is up without auth:

```bash
curl -sS http://localhost:4318/healthz
# {"status":"ok"}
curl -sS http://localhost:4318/readyz
# {"status":"ready"}
```

## Step 3: Stop and query

Back in DuckDB, stop the server before closing the database:

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

`otlp_stop` commits remaining buffered rows before it returns, so an explicit `otlp_flush` is not required for this quickstart.

Now query the accepted row:

```sql
SELECT time_unix_nano, service_name, severity_text, body
FROM otlp_logs;
```

```text
┌─────────────────────┬──────────────┬───────────────┬─────────────────┐
│   time_unix_nano    │ service_name │ severity_text │      body       │
├─────────────────────┼──────────────┼───────────────┼─────────────────┤
│ 2024-01-01 00:00:00 │ curl-demo    │ INFO          │ hello from curl │
└─────────────────────┴──────────────┴───────────────┴─────────────────┘
```

## Next steps

- **[Stream to Local DuckLake](../../guides/stream-to-local-ducklake/)** - land OTLP rows in local Parquet files tracked by DuckLake.
- **[Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/)** - land OTLP rows in DuckLake with Neon and R2.
- **[Stream to Parquet](../../guides/stream-to-parquet/)** - land OTLP rows as partitioned Parquet files on disk or S3.
- **[Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/)** - land OTLP rows in Amazon S3 Tables as an Iceberg catalog.
- **[Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/)** - land OTLP rows in Cloudflare R2 Data Catalog as an Iceberg catalog.
- **[Live Ingest Reference](../../reference/serve/)** - all parameters, endpoints, auth, status codes, buffering, and durability.
