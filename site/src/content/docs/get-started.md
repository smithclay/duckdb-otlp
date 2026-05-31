---
title: "Get Started"
---

Use the DuckDB OpenTelemetry Extension to query OpenTelemetry files, then try the native OTLP/HTTP ingest server.

## Prerequisites

- DuckDB 0.10 or later.
- A native DuckDB build for live ingest.
- The repository checkout if you want to use the bundled `test/data/` samples:

```bash
git clone https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp
```

## 1. Install and Load

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

## 2. Read OTLP Files

The table functions accept local paths, globs, and DuckDB-supported remote file systems.

```sql
SELECT
    trace_id,
    span_name,
    duration / 1000000 AS duration_ms
FROM read_otlp_traces('test/data/traces_simple.jsonl')
ORDER BY duration DESC;
```

```sql
SELECT timestamp, service_name, severity_text, body
FROM read_otlp_logs('test/data/logs_simple.jsonl')
WHERE severity_text = 'ERROR';
```

```sql
SELECT timestamp, metric_name, value
FROM read_otlp_metrics_gauge('test/data/metrics_simple.jsonl');
```

Histogram metrics use typed readers too:

```sql
SELECT metric_name, count, sum, bucket_counts, explicit_bounds
FROM read_otlp_metrics_histogram('test/data/metrics_simple.jsonl');
```

See the [Schema Reference](../reference/schemas/) for every column emitted by each reader.

## 3. Stream One Log over OTLP/HTTP

The ingest server is available in native builds only. It accepts OTLP/HTTP, buffers rows in memory, and commits them in batches. A POST returning `202` means the rows are accepted but not durable until an automatic background commit, a graceful `otlp_stop`, or an optional `otlp_flush`. Current native builds commit automatically when the oldest buffered row is about 5 seconds old, or when admitted request-body bytes reach about 64 MiB.

Start a local server:

```sql
SELECT listen_url
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

Send one OTLP log record:

```bash
curl -sS http://localhost:4318/v1/logs -H 'Authorization: Bearer dev-token-123456' -H 'Content-Type: application/json' -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"curl-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from curl"}}]}]}]}'
```

Stop the server before closing the database. `otlp_stop` commits remaining buffered rows before it returns:

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

Then query the accepted row:

```sql
SELECT timestamp, service_name, severity_text, body
FROM otlp_logs;
```

You can query while the server is still running after the automatic background commit. Use `otlp_flush` only when you need the latest accepted rows visible immediately.

For durable lakehouse ingest, pass `catalog` to an attached lakehouse catalog; see [Stream to DuckLake](../guides/stream-to-ducklake/) or [Stream to Amazon S3 Tables](../guides/stream-to-s3-tables/).

## Next Steps

- [Live Ingest Quickstart](../quickstart/serve/) - POST one log record over OTLP/HTTP.
- [Stream to DuckLake](../guides/stream-to-ducklake/) - stream OTLP into DuckLake/Parquet.
- [Stream to Amazon S3 Tables](../guides/stream-to-s3-tables/) - stream OTLP into Amazon S3 Tables as an Iceberg catalog.
- [How to Configure the OpenTelemetry Collector](../setup/collector/) - export OTLP files from the OpenTelemetry Collector.
- [How-to Guides](../guides/) - common query and export tasks.
- [API Reference](../reference/api/) - function signatures and capability notes.
- [Schema Reference](../reference/schemas/) - complete column lists.

## Common Issues

**Extension `otlp` not found**

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

**File does not exist**

Use a path relative to DuckDB's current working directory, or use an absolute path:

```sql
SELECT * FROM read_otlp_traces('/full/path/to/traces.jsonl');
```

**Rows posted to the server are not visible yet**

Wait for the automatic background commit, or stop the server with `otlp_stop('otlp:localhost:4318')` before closing the database. Use `otlp_flush('otlp:localhost:4318')` only when the server should keep running and readers need the latest accepted rows immediately.
