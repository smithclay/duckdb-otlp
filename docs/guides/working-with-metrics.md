# Working with Metrics

The DuckDB OTLP extension exposes shape-specific metric readers. OTLP metric types have different columns, so `read_otlp_metrics()` is intentionally unsupported for now.

Use:

- `read_otlp_metrics_gauge(path)`
- `read_otlp_metrics_sum(path)`
- `read_otlp_metrics_histogram(path)`
- `read_otlp_metrics_exp_histogram(path)`

`read_otlp_metrics_summary(path)` is registered but not implemented yet.

See the [Schema Reference](../reference/schemas.md#metrics) for complete column details.

## Query Gauge Metrics

```sql
SELECT timestamp, service_name, metric_name, value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE metric_name LIKE 'system.cpu%';
```

## Query Sum and Counter Metrics

```sql
SELECT
  timestamp,
  service_name,
  metric_name,
  value,
  aggregation_temporality,
  is_monotonic
FROM read_otlp_metrics_sum('metrics/*.jsonl')
WHERE is_monotonic;
```

## Query Histogram Metrics

```sql
SELECT
  timestamp,
  metric_name,
  count,
  sum,
  bucket_counts,
  explicit_bounds
FROM read_otlp_metrics_histogram('metrics/*.jsonl')
WHERE metric_name = 'http.server.duration';
```

## Query Exponential Histograms

```sql
SELECT
  timestamp,
  metric_name,
  count,
  sum,
  scale,
  positive_bucket_counts,
  negative_bucket_counts
FROM read_otlp_metrics_exp_histogram('metrics/*.jsonl');
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

## Aggregate Metrics Over Time

```sql
SELECT
  date_trunc('hour', timestamp) AS hour,
  metric_name,
  avg(value) AS avg_value,
  max(value) AS max_value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE metric_name = 'system.cpu.utilization'
GROUP BY hour, metric_name
ORDER BY hour DESC;
```

## Filter by Service or Attributes

Attribute columns are JSON strings. Use DuckDB JSON functions to filter nested keys:

```sql
SELECT timestamp, metric_name, value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE json_extract_string(resource_attributes, '$."deployment.environment"') = 'prod'
  AND service_name = 'api-gateway';
```

## Export Metrics to Parquet

Write each metric shape to its own Parquet file or table:

```sql
COPY (
  SELECT * FROM read_otlp_metrics_histogram('metrics/*.jsonl')
) TO 'warehouse/daily_metric_histograms.parquet' (FORMAT PARQUET);
```

## See Also

- [API Reference](../reference/api.md#metrics)
- [Schema Reference](../reference/schemas.md#metrics)
- [How-to Guides](README.md)
