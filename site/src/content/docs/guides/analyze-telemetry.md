---
title: "How to Analyze Telemetry"
---

Use the OTLP reader that matches the signal you are analyzing:

- traces: `read_otlp_traces(path)`
- logs: `read_otlp_logs(path)`
- gauges: `read_otlp_metrics_gauge(path)`
- sums and counters: `read_otlp_metrics_sum(path)`
- histograms: `read_otlp_metrics_histogram(path)`
- exponential histograms: `read_otlp_metrics_exp_histogram(path)`

> If you are using the `duckdb-otlp` server, incoming telemetry is already written into regular tables such as `otlp_traces`, `otlp_logs`, and `otlp_metrics_gauge`. Use the same SQL patterns below, but replace the `read_otlp_*('...')` call with your table name.

The examples below use JSONL paths such as `traces/*.jsonl`, but the same readers also accept OTLP JSON and protobuf files. For complete columns and types, use the [Schema Reference](../../reference/schemas/).

## Traces

Find slow server operations:

```sql
SELECT
  name,
  service_name,
  count(*) AS span_count,
  avg(duration_time_unix_nano) / 1000000 AS avg_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_time_unix_nano) / 1000000 AS p95_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE kind = 2
GROUP BY name, service_name
ORDER BY p95_ms DESC
LIMIT 20;
```

Drill into the slowest trace:

```sql
WITH target AS (
  SELECT trace_id
  FROM read_otlp_traces('traces/*.jsonl')
  ORDER BY duration_time_unix_nano DESC
  LIMIT 1
)
SELECT span_id, parent_span_id, service_name, name, duration_time_unix_nano / 1000000 AS duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE trace_id = (SELECT trace_id FROM target)
ORDER BY start_time_unix_nano;
```

See [Traces Schema](../../reference/schemas/#traces-read_otlp_traces) for span fields, status codes, attributes, events, and links.

## Logs

Find recent errors:

```sql
SELECT time_unix_nano, service_name, severity_text, body
FROM read_otlp_logs('logs/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY time_unix_nano DESC
LIMIT 100;
```

Join error logs back to spans:

```sql
SELECT
  l.time_unix_nano,
  l.service_name,
  l.body,
  t.name,
  t.duration_time_unix_nano / 1000000 AS duration_ms
FROM read_otlp_logs('logs/*.jsonl') l
JOIN read_otlp_traces('traces/*.jsonl') t
  ON l.trace_id = t.trace_id
 AND l.span_id = t.span_id
WHERE l.severity_text = 'ERROR'
ORDER BY l.time_unix_nano DESC;
```

See [Logs Schema](../../reference/schemas/#logs-read_otlp_logs) for severity, body, resource attributes, scope fields, and trace correlation columns.

## Metrics

Aggregate gauge metrics over time:

```sql
SELECT
  date_trunc('hour', time_unix_nano) AS hour,
  service_name,
  name,
  avg(coalesce(double_value, int_value::DOUBLE)) AS avg_value,
  max(coalesce(double_value, int_value::DOUBLE)) AS max_value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE name = 'system.cpu.utilization'
GROUP BY hour, service_name, name
ORDER BY hour DESC;
```

Inspect histogram metrics:

```sql
SELECT
  time_unix_nano,
  service_name,
  name,
  count,
  sum,
  bucket_counts,
  explicit_bounds
FROM read_otlp_metrics_histogram('metrics/*.jsonl')
WHERE name = 'http.server.duration'
ORDER BY time_unix_nano DESC
LIMIT 50;
```

Metric shapes have different columns, so choose the shape-specific reader. See [Metrics Schema](../../reference/schemas/#metrics) for gauge, sum, histogram, and exponential histogram fields.

## Attributes

Resource, scope, and signal attributes are JSON strings. Use DuckDB JSON functions to filter nested keys:

```sql
SELECT time_unix_nano, service_name, body
FROM read_otlp_logs('logs/*.jsonl')
WHERE json_extract_string(resource_attributes, '$."deployment.environment"') = 'prod';
```

For malformed files or unexpected parse errors, see [How to handle malformed input](../error-handling/).
