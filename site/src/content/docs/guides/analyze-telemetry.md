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
  span_name,
  service_name,
  count(*) AS span_count,
  avg(duration) / 1000000 AS avg_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY duration) / 1000000 AS p95_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE span_kind = 2
GROUP BY span_name, service_name
ORDER BY p95_ms DESC
LIMIT 20;
```

Drill into the slowest trace:

```sql
WITH target AS (
  SELECT trace_id
  FROM read_otlp_traces('traces/*.jsonl')
  ORDER BY duration DESC
  LIMIT 1
)
SELECT span_id, parent_span_id, service_name, span_name, duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE trace_id = (SELECT trace_id FROM target)
ORDER BY timestamp;
```

See [Traces Schema](../../reference/schemas/#traces-read_otlp_traces) for span fields, status codes, attributes, events, and links.

## Logs

Find recent errors:

```sql
SELECT timestamp, service_name, severity_text, body
FROM read_otlp_logs('logs/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp DESC
LIMIT 100;
```

Join error logs back to spans:

```sql
SELECT
  l.timestamp,
  l.service_name,
  l.body,
  t.span_name,
  t.duration / 1000000 AS duration_ms
FROM read_otlp_logs('logs/*.jsonl') l
JOIN read_otlp_traces('traces/*.jsonl') t
  ON l.trace_id = t.trace_id
 AND l.span_id = t.span_id
WHERE l.severity_text = 'ERROR'
ORDER BY l.timestamp DESC;
```

See [Logs Schema](../../reference/schemas/#logs-read_otlp_logs) for severity, body, resource attributes, scope fields, and trace correlation columns.

## Metrics

Aggregate gauge metrics over time:

```sql
SELECT
  date_trunc('hour', timestamp) AS hour,
  service_name,
  metric_name,
  avg(value) AS avg_value,
  max(value) AS max_value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
WHERE metric_name = 'system.cpu.utilization'
GROUP BY hour, service_name, metric_name
ORDER BY hour DESC;
```

Inspect histogram metrics:

```sql
SELECT
  timestamp,
  service_name,
  metric_name,
  count,
  sum,
  bucket_counts,
  explicit_bounds
FROM read_otlp_metrics_histogram('metrics/*.jsonl')
WHERE metric_name = 'http.server.duration'
ORDER BY timestamp DESC
LIMIT 50;
```

Metric shapes have different columns, so choose the shape-specific reader. See [Metrics Schema](../../reference/schemas/#metrics) for gauge, sum, histogram, and exponential histogram fields.

## Attributes

Resource, scope, and signal attributes are JSON strings. Use DuckDB JSON functions to filter nested keys:

```sql
SELECT timestamp, service_name, body
FROM read_otlp_logs('logs/*.jsonl')
WHERE json_extract_string(resource_attributes, '$."deployment.environment"') = 'prod';
```

For malformed files or unexpected parse errors, see [How to handle malformed input](../error-handling/).
