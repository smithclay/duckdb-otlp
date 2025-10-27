# Traces Schema Reference

Complete schema documentation is available in [schemas.md](schemas.md#traces-read_otlp_traces).

## Quick Reference

`read_otlp_traces(path, ...)` returns **22 columns**:

### Identifiers
- `TraceId` (HUGEINT) - 128-bit trace identifier
- `SpanId` (HUGEINT) - 128-bit span identifier
- `ParentSpanId` (HUGEINT) - Parent span ID
- `TraceState` (VARCHAR) - W3C trace state

### Span Metadata
- `SpanName` (VARCHAR) - Operation name
- `SpanKind` (VARCHAR) - INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER
- `ServiceName` (VARCHAR) - Service name from resource attributes

### Timing
- `Timestamp` (TIMESTAMP) - Span start time
- `EndTimestamp` (TIMESTAMP) - Span end time
- `Duration` (BIGINT) - Duration in nanoseconds (computed)

### Status
- `StatusCode` (VARCHAR) - OK, ERROR, UNSET
- `StatusMessage` (VARCHAR) - Status description

### Attributes
- `ResourceAttributes` (MAP<VARCHAR, VARCHAR>) - Resource-level attributes
- `Attributes` (MAP<VARCHAR, VARCHAR>) - Span-level attributes
- `SpanAttributes` (MAP<VARCHAR, VARCHAR>) - Alias for Attributes

### Instrumentation
- `ScopeName` (VARCHAR) - Instrumentation scope name
- `ScopeVersion` (VARCHAR) - Instrumentation scope version
- `ResourceSchemaUrl` (VARCHAR) - Resource schema URL
- `ScopeSchemaUrl` (VARCHAR) - Scope schema URL

### Structured Data
- `Events` (LIST<STRUCT>) - Span events (name, timestamp, attributes)
- `Links` (LIST<STRUCT>) - Span links (trace_id, span_id, attributes)

## Example Queries

```sql
-- Basic span query
SELECT TraceId, SpanName, Duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces.jsonl')
WHERE Duration > 1000000000;

-- Access attributes
SELECT
  SpanName,
  ResourceAttributes['service.name'] AS service,
  Attributes['http.method'] AS method
FROM read_otlp_traces('traces.jsonl');

-- Analyze events
SELECT
  SpanName,
  unnest(Events) AS event
FROM read_otlp_traces('traces.jsonl');
```

## See Also

- [Full Schema Documentation](schemas.md#traces-read_otlp_traces)
- [API Reference](api.md#traces)
- [Analyzing Traces Guide](../guides/analyzing-traces.md)
