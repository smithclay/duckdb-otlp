# Duckspan Cookbook

Common, copy-paste-ready recipes for working with OTLP exports in DuckDB. Start with the [collector setup guide](../setup/collector.md) if you need to generate data using the OpenTelemetry Collector file exporter.

## Export telemetry to Parquet (and read it back)

Persisting telemetry to Parquet keeps files compact while preserving Duckspan’s column types.

```sql
-- 1. Load the extension
LOAD otlp;

-- 2. Stream multiple OTLP exports into a Parquet dataset
COPY (
  SELECT *
  FROM read_otlp_traces('otel-export/*.jsonl')
) TO 'warehouse/daily_traces.parquet' (FORMAT PARQUET);

-- 3. Re-open the Parquet file later (no extension load required)
SELECT TraceId, SpanName, Duration
FROM read_parquet('warehouse/daily_traces.parquet')
WHERE Duration >= 1000000000;
```

Tips:
- Combine multiple signal types by writing each to a dedicated Parquet file or DuckDB table (`daily_traces.parquet`, `daily_logs.parquet`, `daily_metrics.parquet`).
- For partitioned writes, replace the `COPY` with `INSERT INTO` or `CREATE TABLE AS` against a DuckDB table backed by Parquet.

## Build typed metrics tables

Turn the metrics union schema into tables that match each metric shape.

```sql
LOAD otlp;

CREATE TABLE metrics_gauge AS
SELECT Timestamp, ServiceName, MetricName, Value
FROM read_otlp_metrics('otel-export/telemetry.jsonl')
WHERE MetricType = 'gauge';

CREATE TABLE metrics_histogram AS
SELECT Timestamp, ServiceName, MetricName,
       Count, Sum, BucketCounts, ExplicitBounds
FROM read_otlp_metrics('otel-export/telemetry.jsonl')
WHERE MetricType = 'histogram';
```

Prefer the helper scans when you only need one metric type:

```sql
INSERT INTO metrics_gauge
SELECT *
FROM read_otlp_metrics_gauge('otel-export/telemetry.jsonl');
```

Refer to the [schema reference](../reference/schemas.md#metrics-read_otlp_metrics) for every column available on the union and helper functions.

## Filter noisy telemetry during ingest

Use the `on_error` named parameter to continue scanning when malformed rows appear.

```sql
SELECT *
FROM read_otlp_logs('s3://otel-bucket/logs/*.jsonl', on_error := 'skip')
WHERE SeverityText IN ('ERROR', 'FATAL');
```

Retrieve parse diagnostics for the current connection:

```sql
SELECT * FROM read_otlp_scan_stats();
```

## Analyze traces across files

Grep across entire directories or object-store prefixes using globs and DuckDB predicates.

```sql
SELECT ServiceName,
       COUNT(*) AS span_count,
       AVG(Duration) / 1000000 AS avg_duration_ms
FROM read_otlp_traces('s3://otel-archive/2024/05/*.pb')
WHERE SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY span_count DESC;
```

## Discover available options

Check which named parameters the extension supports without leaving DuckDB:

```sql
SELECT *
FROM read_otlp_options();
```

Each option is documented alongside its default value and whether it supports automatic filter pushdown.

---

Need more detail on column layouts? Head over to the [schema reference](../reference/schemas.md). Looking for ways to collect fresh data? The [collector setup guide](../setup/collector.md) has you covered.
