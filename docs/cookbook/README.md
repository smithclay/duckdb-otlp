# Cookbook

This page is kept for older links. The maintained cookbook is [docs/guides/cookbook.md](../guides/cookbook.md).

## Export Telemetry to Parquet

```sql
LOAD otlp;

COPY (
  SELECT *
  FROM read_otlp_traces('otel-export/*.jsonl')
) TO 'warehouse/daily_traces.parquet' (FORMAT PARQUET);

SELECT trace_id, span_name, duration
FROM read_parquet('warehouse/daily_traces.parquet')
WHERE duration >= 1000000000;
```

## Build Typed Metrics Tables

```sql
CREATE TABLE metrics_gauge AS
SELECT * FROM read_otlp_metrics_gauge('otel-export/telemetry.jsonl');

CREATE TABLE metrics_sum AS
SELECT * FROM read_otlp_metrics_sum('otel-export/telemetry.jsonl');

CREATE TABLE metrics_histogram AS
SELECT * FROM read_otlp_metrics_histogram('otel-export/telemetry.jsonl');

CREATE TABLE metrics_exp_histogram AS
SELECT * FROM read_otlp_metrics_exp_histogram('otel-export/telemetry.jsonl');
```

## Filter Logs

```sql
SELECT timestamp, service_name, body
FROM read_otlp_logs('app-logs/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp DESC;
```

For current recipes, see [docs/guides/cookbook.md](../guides/cookbook.md).
