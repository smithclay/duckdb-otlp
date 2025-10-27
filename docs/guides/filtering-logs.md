# Filtering Logs

Learn how to query and filter OpenTelemetry log records using the DuckDB OTLP extension.

## Basic Log Filtering

Filter logs by severity level to focus on errors and critical issues:

```sql
LOAD otlp;

SELECT Timestamp, ServiceName, SeverityText, Body
FROM read_otlp_logs('otel-export/*.jsonl')
WHERE SeverityText IN ('ERROR', 'FATAL')
ORDER BY Timestamp DESC;
```

## Filter by Service

Target specific services using resource attributes:

```sql
SELECT Timestamp, SeverityText, Body
FROM read_otlp_logs('logs/*.jsonl')
WHERE ServiceName = 'checkout-service'
  AND SeverityText = 'ERROR';
```

## Filter by Time Range

Query logs within a specific time window:

```sql
SELECT Timestamp, ServiceName, Body
FROM read_otlp_logs('logs/*.jsonl')
WHERE Timestamp >= '2024-01-01 00:00:00'
  AND Timestamp < '2024-01-02 00:00:00'
  AND SeverityText = 'ERROR';
```

## Search Log Body Content

Use pattern matching to find specific messages:

```sql
SELECT Timestamp, ServiceName, Body
FROM read_otlp_logs('logs/*.jsonl')
WHERE Body LIKE '%timeout%'
  OR Body LIKE '%connection failed%';
```

## Correlate Logs with Traces

Join logs to traces using TraceId and SpanId:

```sql
SELECT
  l.Timestamp,
  l.ServiceName,
  l.Body,
  t.SpanName,
  t.Duration / 1000000 AS duration_ms
FROM read_otlp_logs('logs/*.jsonl') l
JOIN read_otlp_traces('traces/*.jsonl') t
  ON l.TraceId = t.TraceId
  AND l.SpanId = t.SpanId
WHERE l.SeverityText = 'ERROR';
```

## Handle Noisy or Malformed Logs

Use error handling options to skip invalid log records:

```sql
SELECT *
FROM read_otlp_logs('s3://otel-bucket/logs/*.jsonl', on_error := 'skip')
WHERE SeverityText IN ('ERROR', 'FATAL');
```

Check parse diagnostics:

```sql
SELECT * FROM read_otlp_scan_stats();
```

See the [Error Handling Guide](error-handling.md) for more details.

## More Examples

For additional recipes and patterns, see the [Cookbook](cookbook.md#filter-noisy-telemetry-during-ingest).

## See Also

- [API Reference](../reference/api.md#logs) - `read_otlp_logs` function signature
- [Logs Schema](../reference/schemas.md#logs-read_otlp_logs) - All available columns
- [Error Handling](error-handling.md) - Handle malformed data
