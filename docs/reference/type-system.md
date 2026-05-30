# Type Notes

The full column list is in [Schema Reference](schemas.md). These are the practical type details most queries need.

## Identifiers

`trace_id`, `span_id`, and `parent_span_id` are lowercase hex strings.

```sql
SELECT trace_id, span_id, span_name
FROM read_otlp_traces('traces.jsonl');
```

## Timestamps and Durations

`timestamp` is `TIMESTAMP_MS`. Related raw timestamp fields such as `end_timestamp`, `observed_timestamp`, and `start_timestamp` are nanoseconds as `BIGINT`.

`duration` is nanoseconds:

```sql
SELECT span_name, duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces.jsonl');
```

## Attributes and Nested Data

Attributes, events, links, buckets, and exemplars are emitted as JSON strings. Use DuckDB JSON functions when filtering nested fields:

```sql
SELECT
  span_name,
  json_extract_string(span_attributes, '$."http.method"') AS method
FROM read_otlp_traces('traces.jsonl');

SELECT
  body,
  json_extract_string(log_attributes, '$."error.type"') AS error_type
FROM read_otlp_logs('logs.jsonl');
```

## Metric Shapes

Metrics use different schemas per shape. Use the typed reader that matches the OTLP metric type:

```sql
SELECT metric_name, value FROM read_otlp_metrics_gauge('metrics.jsonl');
SELECT metric_name, value, is_monotonic FROM read_otlp_metrics_sum('metrics.jsonl');
SELECT metric_name, count, bucket_counts FROM read_otlp_metrics_histogram('metrics.jsonl');
SELECT metric_name, count, positive_bucket_counts FROM read_otlp_metrics_exp_histogram('metrics.jsonl');
```
