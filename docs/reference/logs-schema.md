# Logs Schema Reference

Complete schema documentation is available in [schemas.md](schemas.md#logs-read_otlp_logs).

## Quick Reference

`read_otlp_logs(path, ...)` returns **15 columns**:

### Timing
- `Timestamp` (TIMESTAMP) - Log record timestamp
- `ObservedTimestamp` (TIMESTAMP) - When log was observed

### Severity
- `SeverityText` (VARCHAR) - Text severity (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)
- `SeverityNumber` (INTEGER) - Numeric severity level

### Content
- `Body` (VARCHAR) - Log message body

### Correlation
- `TraceId` (HUGEINT) - Associated trace ID
- `SpanId` (HUGEINT) - Associated span ID

### Service
- `ServiceName` (VARCHAR) - Service name from resource attributes

### Attributes
- `ResourceAttributes` (MAP<VARCHAR, VARCHAR>) - Resource-level attributes
- `Attributes` (MAP<VARCHAR, VARCHAR>) - Log-level attributes

### Instrumentation
- `ScopeName` (VARCHAR) - Instrumentation scope name
- `ScopeVersion` (VARCHAR) - Instrumentation scope version
- `ResourceSchemaUrl` (VARCHAR) - Resource schema URL
- `ScopeSchemaUrl` (VARCHAR) - Scope schema URL

## Example Queries

```sql
-- Filter by severity
SELECT Timestamp, ServiceName, Body
FROM read_otlp_logs('logs.jsonl')
WHERE SeverityText IN ('ERROR', 'FATAL')
ORDER BY Timestamp DESC;

-- Correlate with traces
SELECT l.Timestamp, l.Body, t.SpanName
FROM read_otlp_logs('logs.jsonl') l
JOIN read_otlp_traces('traces.jsonl') t
  ON l.TraceId = t.TraceId
WHERE l.SeverityText = 'ERROR';

-- Access attributes
SELECT
  Body,
  ResourceAttributes['deployment.environment'] AS env,
  Attributes['error.type'] AS error_type
FROM read_otlp_logs('logs.jsonl')
WHERE SeverityText = 'ERROR';
```

## See Also

- [Full Schema Documentation](schemas.md#logs-read_otlp_logs)
- [API Reference](api.md#logs)
- [Filtering Logs Guide](../guides/filtering-logs.md)
