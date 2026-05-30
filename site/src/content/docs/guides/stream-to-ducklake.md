---
title: "How to Stream OTLP to DuckLake"
---

Use `otlp_serve(..., catalog := 'lake')` to stream OTLP/HTTP exports into a DuckLake lakehouse. The extension buffers accepted rows and commits them in batches, so a busy ingest stream writes fewer Parquet files than one file per request.

> Requires the native extension. Live ingestion is HTTP-only and is not available in WASM builds.

## Attach DuckLake

Start DuckDB and attach a DuckLake catalog:

```sql
INSTALL otlp FROM community;
LOAD otlp;

INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'otlp_data/');
```

The `metadata.ducklake` file stores the catalog metadata. The `otlp_data/` directory stores Parquet data files.

## Start the ingest server

```sql
SELECT listen_url, auth_token, catalog_name
FROM otlp_serve(
  'otlp:localhost:4318',
  catalog := 'lake',
  token := 'dev-token-123456'
);
```

`otlp_serve` creates the target tables in `lake.main` if they do not already exist:

- `otlp_logs`
- `otlp_traces`
- `otlp_metrics_gauge`
- `otlp_metrics_sum`
- `otlp_metrics_histogram`
- `otlp_metrics_exp_histogram`

Leave this DuckDB session running while clients send OTLP/HTTP requests.

## POST logs

In another terminal, POST a sample OTLP/JSON logs file:

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

`application/x-ndjson` is for newline-delimited OTLP/JSON. Use `application/json` for one JSON document or `application/x-protobuf` for protobuf.

## Query committed rows

Rows are accepted before they are durable. They commit automatically in the background, currently when the oldest buffered row is about 5 seconds old or when admitted request-body bytes reach about 64 MiB.

To inspect buffer and commit counters:

```sql
SELECT
  catalog_name,
  total_rows,
  buffered_rows,
  last_seal_age_ms AS last_commit_age_ms,
  seals_total AS commits_total
FROM otlp_server_list();
```

When `buffered_rows` returns to `0`, accepted rows have been committed.

Query the DuckLake tables:

```sql
SELECT count(*) FROM lake.main.otlp_logs;

SELECT timestamp, service_name, severity_text, body
FROM lake.main.otlp_logs
ORDER BY timestamp DESC
LIMIT 20;
```

## Stop cleanly

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

`otlp_stop` commits remaining buffered rows before returning. A plain database or connection close stops the server but does not commit buffered rows, so stop the server before closing DuckDB. Use `otlp_flush('otlp:localhost:4318')` only when readers need the latest accepted rows immediately while the server keeps running.

DuckLake file compaction is separate from `otlp_flush`; run DuckLake maintenance when file counts need cleanup.

## See also

- [DuckLake documentation](https://ducklake.select/)
- [Live Ingest Reference](../../reference/serve/)
- [Live Ingest Quickstart](../../quickstart/serve/)
