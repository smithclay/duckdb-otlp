---
title: "Live Ingest Quickstart"
---

Start a local OTLP/HTTP endpoint, POST one log record, and query it from DuckDB.

> Requires the **native** extension. The ingest server is not available in WASM builds, and live ingestion is HTTP-only.

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

A `202 Accepted` with `"status":"buffered"` means the row was parsed and accepted into the in-memory buffer. It becomes queryable after the next automatic background commit, after `otlp_stop`, or immediately after an optional `otlp_flush`.

You can confirm the server is up without auth:

```bash
curl -sS http://localhost:4318/healthz
# {"status":"ok"}
```

## Step 3: Stop and query

Back in DuckDB, stop the server before closing the database:

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

`otlp_stop` commits remaining buffered rows before it returns, so an explicit `otlp_flush` is not required for this quickstart.

Now query the accepted row:

```sql
SELECT timestamp, service_name, severity_text, body
FROM otlp_logs;
```

```text
┌─────────────────────┬──────────────┬───────────────┬─────────────────┐
│      timestamp      │ service_name │ severity_text │      body       │
├─────────────────────┼──────────────┼───────────────┼─────────────────┤
│ 2024-01-01 00:00:00 │ curl-demo    │ INFO          │ hello from curl │
└─────────────────────┴──────────────┴───────────────┴─────────────────┘
```

## Next steps

- **[Stream to DuckLake](../../guides/stream-to-ducklake/)** - land OTLP rows in Parquet files tracked by DuckLake.
- **[Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/)** - land OTLP rows in Amazon S3 Tables as an Iceberg catalog.
- **[Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/)** - land OTLP rows in Cloudflare R2 Data Catalog as an Iceberg catalog.
- **[Live Ingest Reference](../../reference/serve/)** - all parameters, endpoints, auth, status codes, buffering, and durability.
