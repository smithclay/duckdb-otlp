# Metrics Schema Reference

Complete schema documentation is available in [schemas.md](schemas.md#metrics-read_otlp_metrics).

## Quick Reference

`read_otlp_metrics(path, ...)` returns a **27-column union schema**:

### Common Columns (all metrics)

- `Timestamp` (TIMESTAMP)
- `ServiceName` (VARCHAR)
- `MetricName` (VARCHAR)
- `MetricDescription` (VARCHAR)
- `MetricUnit` (VARCHAR)
- **`MetricType`** (VARCHAR) - `'gauge'`, `'sum'`, `'histogram'`, `'exp_histogram'`, `'summary'`
- `ResourceAttributes` (MAP<VARCHAR, VARCHAR>)
- `ScopeName` (VARCHAR)
- `ScopeVersion` (VARCHAR)
- `Attributes` (MAP<VARCHAR, VARCHAR>)

### Type-Specific Columns

**Gauge** (`MetricType = 'gauge'`):
- `Value` (DOUBLE)

**Sum** (`MetricType = 'sum'`):
- `Value` (DOUBLE)
- `AggregationTemporality` (VARCHAR)
- `IsMonotonic` (BOOLEAN)

**Histogram** (`MetricType = 'histogram'`):
- `Count` (BIGINT)
- `Sum` (DOUBLE)
- `BucketCounts` (LIST<BIGINT>)
- `ExplicitBounds` (LIST<DOUBLE>)
- `Min` (DOUBLE)
- `Max` (DOUBLE)

**Exponential Histogram** (`MetricType = 'exp_histogram'`):
- `Count`, `Sum`, `Scale`, `ZeroCount`
- `PositiveOffset`, `PositiveBucketCounts`
- `NegativeOffset`, `NegativeBucketCounts`
- `Min`, `Max`

**Summary** (`MetricType = 'summary'`):
- `Count` (BIGINT)
- `Sum` (DOUBLE)
- `QuantileValues` (LIST<DOUBLE>)
- `QuantileQuantiles` (LIST<DOUBLE>)

## Typed Helper Functions

For cleaner queries, use typed helpers:

```sql
-- Gauge metrics only
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');

-- Sum metrics only
SELECT * FROM read_otlp_metrics_sum('metrics.jsonl');

-- Histogram metrics only
SELECT * FROM read_otlp_metrics_histogram('metrics.jsonl');

-- Exponential histogram metrics
SELECT * FROM read_otlp_metrics_exp_histogram('metrics.jsonl');

-- Summary metrics
SELECT * FROM read_otlp_metrics_summary('metrics.jsonl');
```

## Example Queries

```sql
-- Query all metrics
SELECT Timestamp, MetricName, MetricType
FROM read_otlp_metrics('metrics.jsonl');

-- Filter by type
SELECT Timestamp, MetricName, Value
FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'gauge';

-- Use typed helper
SELECT Timestamp, MetricName, Value
FROM read_otlp_metrics_gauge('metrics.jsonl')
WHERE MetricName LIKE 'system.cpu%';

-- Build typed table
CREATE TABLE metrics_histogram AS
SELECT * FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'histogram';
```

## See Also

- [Full Schema Documentation](schemas.md#metrics-read_otlp_metrics)
- [API Reference](api.md#metrics)
- [Working with Metrics Guide](../guides/working-with-metrics.md)
