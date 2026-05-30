# Filtering Logs

Use `read_otlp_logs(path)` to query OTLP log exports. The most useful filters are severity, service, time, body text, and trace correlation.

## Severity and Service

```sql
SELECT timestamp, service_name, severity_text, body
FROM read_otlp_logs('logs/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp DESC;
```

```sql
SELECT timestamp, severity_text, body
FROM read_otlp_logs('logs/*.jsonl')
WHERE service_name = 'checkout-service'
  AND severity_text = 'ERROR';
```

## Time and Text Search

```sql
SELECT timestamp, service_name, body
FROM read_otlp_logs('logs/*.jsonl')
WHERE timestamp >= '2026-05-30 00:00:00'
  AND timestamp < '2026-05-31 00:00:00'
  AND body LIKE '%timeout%';
```

## Join Logs to Spans

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
WHERE l.severity_text = 'ERROR';
```

## Attributes

Attribute columns are JSON strings:

```sql
SELECT
  timestamp,
  service_name,
  body,
  json_extract_string(log_attributes, '$."error.type"') AS error_type
FROM read_otlp_logs('logs/*.jsonl')
WHERE json_extract_string(resource_attributes, '$."deployment.environment"') = 'prod';
```

Malformed files fail fast; see [Error Handling](error-handling.md). For more tasks, see [How-to Guides](README.md).
