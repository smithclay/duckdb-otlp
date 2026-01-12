# Cookbook

Common, copy-paste-ready recipes for working with OTLP exports in DuckDB. Start with the [collector setup guide](../setup/collector.md) if you need to generate data using the OpenTelemetry Collector file exporter.

## Export telemetry to Parquet (and read it back)

Persisting telemetry to Parquet keeps files compact while preserving column types.

```sql
-- 1. Load the extension
LOAD otlp;

-- 2. Stream multiple OTLP exports into a Parquet dataset
COPY (
  SELECT *
  FROM read_otlp_traces('otel-export/*.jsonl')
) TO 'warehouse/daily_traces.parquet' (FORMAT PARQUET);

-- 3. Re-open the Parquet file later (no extension load required)
SELECT trace_id, span_name, duration
FROM read_parquet('warehouse/daily_traces.parquet')
WHERE duration >= 1000000000;
```

Tips:
- Combine multiple signal types by writing each to a dedicated Parquet file or DuckDB table (`daily_traces.parquet`, `daily_logs.parquet`, `daily_metrics.parquet`).
- For partitioned writes, replace the `COPY` with `INSERT INTO` or `CREATE TABLE AS` against a DuckDB table backed by Parquet.

## Build typed metrics tables

Create persistent tables from metric data:

```sql
LOAD otlp;

CREATE TABLE metrics_gauge AS
SELECT timestamp, service_name, metric_name, value
FROM read_otlp_metrics_gauge('otel-export/telemetry.jsonl');

CREATE TABLE metrics_sum AS
SELECT timestamp, service_name, metric_name, value, aggregation_temporality, is_monotonic
FROM read_otlp_metrics_sum('otel-export/telemetry.jsonl');
```

Refer to the [schema reference](../reference/schemas.md#metrics) for all available columns.

## Analyze traces across files

Query across entire directories or object-store prefixes using globs and DuckDB predicates.

```sql
SELECT service_name,
       COUNT(*) AS span_count,
       AVG(duration) / 1000000 AS avg_duration_ms
FROM read_otlp_traces('s3://otel-archive/2024/05/*.pb')
WHERE span_kind = 2  -- SERVER
GROUP BY service_name
ORDER BY span_count DESC;
```

## Filter logs by severity

```sql
SELECT timestamp, service_name, body
FROM read_otlp_logs('app-logs/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp DESC;
```

## Join traces to logs

Correlate log records with their parent traces:

```sql
SELECT t.span_name, l.body, l.severity_text
FROM read_otlp_traces('traces/*.jsonl') t
JOIN read_otlp_logs('logs/*.jsonl') l
  ON t.trace_id = l.trace_id
WHERE l.severity_text = 'ERROR';
```

---

Need more detail on column layouts? Head over to the [schema reference](../reference/schemas.md). Looking for ways to collect fresh data? The [collector setup guide](../setup/collector.md) has you covered.
