# Analyzing Traces

Use `read_otlp_traces(path)` for span-level analysis. Durations are nanoseconds; divide by `1000000` for milliseconds.

## Slow Operations

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

## Error Spans

```sql
SELECT trace_id, span_id, service_name, span_name, status_message, duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE status_code = 2
ORDER BY timestamp DESC
LIMIT 50;
```

## Drill into One Trace

```sql
WITH target AS (
  SELECT trace_id
  FROM read_otlp_traces('traces/*.jsonl')
  WHERE duration > 1000000000
  ORDER BY duration DESC
  LIMIT 1
)
SELECT span_id, parent_span_id, service_name, span_name, span_kind, duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE trace_id = (SELECT trace_id FROM target)
ORDER BY timestamp;
```

## Service Dependencies

```sql
SELECT
  parent.service_name AS caller_service,
  child.service_name AS callee_service,
  count(*) AS call_count
FROM read_otlp_traces('traces/*.jsonl') parent
JOIN read_otlp_traces('traces/*.jsonl') child
  ON parent.trace_id = child.trace_id
 AND parent.span_id = child.parent_span_id
WHERE parent.service_name <> child.service_name
GROUP BY caller_service, callee_service
ORDER BY call_count DESC;
```

## Attributes

Attribute columns are JSON strings:

```sql
SELECT
  span_name,
  json_extract_string(span_attributes, '$."http.method"') AS method,
  json_extract_string(span_attributes, '$."http.route"') AS route,
  duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE json_extract_string(span_attributes, '$."http.method"') = 'POST'
ORDER BY duration DESC;
```

See [Traces Schema](../reference/schemas.md#traces-read_otlp_traces) and [How-to Guides](README.md).
