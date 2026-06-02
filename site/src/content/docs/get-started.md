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
    name,
    duration_time_unix_nano / 1000000 AS duration_ms
FROM read_otlp_traces('test/data/traces_simple.jsonl')
ORDER BY duration_time_unix_nano DESC;
```

```sql
SELECT time_unix_nano, service_name, severity_text, body
FROM read_otlp_logs('test/data/logs_simple.jsonl')
WHERE severity_text = 'ERROR';
```

```sql
SELECT time_unix_nano, name, coalesce(double_value, int_value::DOUBLE) AS value
FROM read_otlp_metrics_gauge('test/data/metrics_simple.jsonl');
```

Histogram metrics use typed readers too:

```sql
SELECT name, count, sum, bucket_counts, explicit_bounds
FROM read_otlp_metrics_histogram('test/data/metrics_simple.jsonl');
```

See the [Schema Reference](../reference/schemas/) for every column emitted by each reader.

## 3. Stream One Log over OTLP/HTTP

Native builds include the ingest server. You can POST OTLP/HTTP to it, and DuckDB buffers rows in memory before batch commits. When a POST returns `202`, DuckDB has accepted the rows, but it has not made them durable yet. DuckDB commits rows after about 5 seconds in the buffer, when admitted request-body bytes reach about 64 MiB, when you call `otlp_stop`, or when you call `otlp_flush`.

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
SELECT time_unix_nano, service_name, severity_text, body
FROM otlp_logs;
```

You can query while the server still runs after the background commit. Use `otlp_flush` when readers need the latest accepted rows before the next scheduled commit.

For durable lakehouse ingest, pass `catalog` to an attached lakehouse catalog. See [Stream to Local DuckLake](../guides/stream-to-local-ducklake/), [Stream to Remote DuckLake](../guides/stream-to-remote-ducklake/), [Stream to Amazon S3 Tables](../guides/stream-to-s3-tables/), or [Stream to Cloudflare R2 Data Catalog](../guides/stream-to-r2-data-catalog/). To write partitioned Parquet files without a lakehouse catalog, see [Stream to Parquet](../guides/stream-to-parquet/).

## Next Steps

- [Live Ingest Quickstart](../quickstart/serve/) - POST one log record over OTLP/HTTP.
- [Stream to Local DuckLake](../guides/stream-to-local-ducklake/) - stream OTLP into local DuckLake/Parquet.
- [Stream to Remote DuckLake](../guides/stream-to-remote-ducklake/) - stream OTLP into DuckLake with Neon and R2.
- [Stream to Parquet](../guides/stream-to-parquet/) - stream OTLP into partitioned Parquet files on disk or S3.
- [Stream to Amazon S3 Tables](../guides/stream-to-s3-tables/) - stream OTLP into Amazon S3 Tables as an Iceberg catalog.
- [Stream to Cloudflare R2 Data Catalog](../guides/stream-to-r2-data-catalog/) - stream OTLP into Cloudflare R2 Data Catalog as an Iceberg catalog.
- [How to Configure the OpenTelemetry Collector](../setup/collector/) - export OTLP files from the OpenTelemetry Collector.
- [How-to Guides](../guides/) - common query and export tasks.
- [API Reference](../reference/api/) - function signatures and capability notes.
- [Schema Reference](../reference/schemas/) - column lists for every reader.

## Common Issues

**Extension `otlp` not found**

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

**File does not exist**

Use a path relative to DuckDB's current working directory, or pass an absolute path:

```sql
SELECT * FROM read_otlp_traces('/full/path/to/traces.jsonl');
```

**Rows posted to the server are not visible yet**

Wait for the background commit, or stop the server with `otlp_stop('otlp:localhost:4318')` before closing the database. Use `otlp_flush('otlp:localhost:4318')` when the server should keep running and readers need the latest accepted rows before the next scheduled commit.
