# Working with Metrics

Learn how to query and analyze OpenTelemetry metrics using the DuckDB OTLP extension.

## Understanding the Metrics Schema

The extension provides two ways to query metrics:

1. **Union schema** - `read_otlp_metrics()` returns all metric types with a `MetricType` discriminator
2. **Typed helpers** - `read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}()` return specific metric types

See the [Metrics Schema Reference](../reference/schemas.md#metrics-read_otlp_metrics) for complete column details.

## Query All Metrics

Use the union schema to see all metrics together:

```sql
LOAD otlp;

SELECT Timestamp, ServiceName, MetricName, MetricType
FROM read_otlp_metrics('metrics/*.jsonl')
LIMIT 100;
```

## Filter by Metric Type

Focus on specific metric types using the discriminator:

```sql
-- Gauges only
SELECT Timestamp, MetricName, Value
FROM read_otlp_metrics('metrics/*.jsonl')
WHERE MetricType = 'gauge';

-- Sums/counters only
SELECT Timestamp, MetricName, Value, IsMonotonic
FROM read_otlp_metrics('metrics/*.jsonl')
WHERE MetricType = 'sum';
```

## Use Typed Helper Functions

For cleaner queries, use the typed helpers:

```sql
-- Query gauge metrics
SELECT Timestamp, ServiceName, MetricName, Value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE MetricName LIKE 'system.cpu%';

-- Query histogram metrics
SELECT Timestamp, MetricName, Count, Sum, BucketCounts, ExplicitBounds
FROM read_otlp_metrics_histogram('metrics/*.jsonl')
WHERE MetricName = 'http.server.duration';
```

## Build Typed Metrics Tables

Split the union schema into separate tables for each metric type:

```sql
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

Or use the helper functions directly:

```sql
CREATE TABLE metrics_gauge AS
SELECT * FROM read_otlp_metrics_gauge('otel-export/telemetry.jsonl');
```

See the [Cookbook](cookbook.md#build-typed-metrics-tables) for more patterns.

## Aggregate Metrics Over Time

Calculate statistics across time windows:

```sql
SELECT
  date_trunc('hour', Timestamp) AS hour,
  MetricName,
  AVG(Value) AS avg_value,
  MAX(Value) AS max_value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE MetricName = 'system.cpu.utilization'
GROUP BY hour, MetricName
ORDER BY hour DESC;
```

## Analyze Histogram Metrics

Work with histogram buckets and percentiles:

```sql
SELECT
  MetricName,
  Count,
  Sum / Count AS avg_value,
  ExplicitBounds,
  BucketCounts
FROM read_otlp_metrics_histogram('metrics/*.jsonl')
WHERE MetricName = 'http.server.duration';
```

## Filter by Service or Attributes

Use resource attributes to filter metrics:

```sql
SELECT Timestamp, MetricName, Value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE ResourceAttributes['deployment.environment'] = 'prod'
  AND ResourceAttributes['service.name'] = 'api-gateway';
```

## Export Metrics to Parquet

Archive metrics for long-term storage:

```sql
COPY (
  SELECT * FROM read_otlp_metrics('metrics/*.jsonl')
) TO 'warehouse/daily_metrics.parquet' (FORMAT PARQUET);
```

See the [Exporting to Parquet Guide](exporting-to-parquet.md) for more details.

## More Examples

For additional recipes and patterns, see the [Cookbook](cookbook.md).

## See Also

- [API Reference](../reference/api.md#metrics) - Metrics function signatures
- [Metrics Schema](../reference/schemas.md#metrics-read_otlp_metrics) - Union schema and helpers
- [Building Dashboards](building-dashboards.md) - Create visualizations from metrics
