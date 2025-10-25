# Metrics Helper Table Functions

The OTLP metrics reader exposes five typed helpers that project the union schema
returned by `read_otlp_metrics()` into the exact column layout for each metric
shape:

| Helper                                | Columns Returned (in order)                                                                |
|---------------------------------------|---------------------------------------------------------------------------------------------|
| `read_otlp_metrics_gauge(path, …)`    | Base columns + `Value`                                                                     |
| `read_otlp_metrics_sum(path, …)`      | Base columns + `Value`, `AggregationTemporality`, `IsMonotonic`                            |
| `read_otlp_metrics_histogram(path, …)`| Base columns + `Count`, `Sum`, `BucketCounts`, `ExplicitBounds`, `Min`, `Max`              |
| `read_otlp_metrics_exp_histogram(path, …)` | Base columns + `Count`, `Sum`, `Scale`, `ZeroCount`, positive/negative bucket data, `Min`, `Max` |
| `read_otlp_metrics_summary(path, …)`  | Base columns + `Count`, `Sum`, `QuantileValues`, `QuantileQuantiles`                       |

The “base columns” shared by every helper are:

```
Timestamp, ServiceName, MetricName, MetricDescription,
MetricUnit, ResourceAttributes, ScopeName, ScopeVersion, Attributes
```

All helpers accept the same parameters as `read_otlp_metrics`:

```sql
SELECT …
FROM read_otlp_metrics_gauge('metrics.jsonl', on_error := 'skip');
```

## Quick Examples

### Gauge metric

```sql
SELECT Timestamp,
       ServiceName,
       MetricName,
       Value
FROM read_otlp_metrics_gauge('test/data/metrics_simple.jsonl')
ORDER BY Timestamp DESC;
```

### Sum metric

```sql
SELECT ServiceName,
       MetricName,
       AVG(Value) AS avg_counter
FROM read_otlp_metrics_sum('test/data/metrics_simple.jsonl')
GROUP BY ALL;
```

### Histogram metric

```sql
SELECT ServiceName,
       MetricName,
       BucketCounts,
       ExplicitBounds
FROM read_otlp_metrics_histogram('test/data/metrics_simple.jsonl')
WHERE ServiceName = 'my-service';
```

### Exponential histogram (when present)

```sql
SELECT Timestamp,
       Scale,
       ZeroCount,
       PositiveBucketCounts,
       NegativeBucketCounts
FROM read_otlp_metrics_exp_histogram('metrics_exp.otlp')
ORDER BY Timestamp DESC;
```

### Summary metric

```sql
SELECT Timestamp,
       MetricName,
       QuantileQuantiles,
       QuantileValues
FROM read_otlp_metrics_summary('metrics_summary.otlp')
WHERE MetricName = 'request.duration';
```

## Choosing Between Union and Helpers

- Use `read_otlp_metrics()` when you need to analyse multiple metric types in
  a single query or construct custom projections.
- Use the helper functions for “typed” tables you can join or store directly.

Because the helpers run on top of the union reader, they support:

- All file formats supported by `read_otlp_metrics()` (JSON/JSONL/protobuf).
- The `on_error` parameter (`fail`, `skip`, `nullify`).
- Filter pushdown on any of the returned columns.

The helpers still scan the full file. If a file does not contain the requested
metric type, the helper simply returns zero rows.

## Cookbook TODOs

- Add worked examples combining multiple metric types.
- Demonstrate joins between live gauge metrics and summary metrics.
- Document common `on_error` patterns for each helper.

Contributions welcome!
>
> - Explain the mapping between the union schema and helper outputs
> - Showcase common filters and aggregations per metric type
> - Add end-to-end example combining gauge + histogram helpers
>
> This document is intentionally a stub; expand as part of the documentation track.
