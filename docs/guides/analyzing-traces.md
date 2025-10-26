# Analyzing Traces

Find slow requests, trace bottlenecks, and analyze service dependencies with SQL queries on OTLP trace exports.

## What You'll Learn

In 10 minutes, you'll know how to:
- Find the slowest requests in your system
- Identify bottlenecks by analyzing span duration
- Track errors by status code
- Correlate traces across services

## Prerequisites

- DuckDB with the OTLP extension installed ([Get Started](../get-started.md))
- OTLP trace files (JSON or protobuf format)

## Quick Examples

### Find Slowest Requests

Identify the slowest server-side requests:

```sql
SELECT
    SpanName,
    AVG(Duration) / 1000000 AS avg_duration_ms,
    MAX(Duration) / 1000000 AS max_duration_ms,
    COUNT(*) AS request_count
FROM read_otlp_traces('traces/*.jsonl')
WHERE SpanKind = 'SERVER'
GROUP BY SpanName
ORDER BY avg_duration_ms DESC
LIMIT 10;
```

**Output:**
```
┌──────────────────┬──────────────────┬──────────────────┬───────────────┐
│    SpanName      │ avg_duration_ms  │ max_duration_ms  │ request_count │
├──────────────────┼──────────────────┼──────────────────┼───────────────┤
│ POST /checkout   │     1523.4       │     2401.2       │      142      │
│ GET /search      │     1205.7       │     1893.5       │      523      │
│ PUT /cart/items  │     1089.2       │     1450.8       │      289      │
└──────────────────┴──────────────────┴──────────────────┴───────────────┘
```

### Find Error Traces

List traces with errors:

```sql
SELECT
    TraceId,
    SpanName,
    StatusCode,
    StatusMessage,
    Duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE StatusCode = 'ERROR'
ORDER BY Timestamp DESC
LIMIT 20;
```

### Analyze by Service

See which services are contributing most to latency:

```sql
SELECT
    ServiceName,
    COUNT(*) AS span_count,
    AVG(Duration) / 1000000 AS avg_duration_ms,
    MAX(Duration) / 1000000 AS max_duration_ms
FROM read_otlp_traces('traces/*.jsonl')
GROUP BY ServiceName
ORDER BY avg_duration_ms DESC;
```

## Step-by-Step: Tracing a Slow Request

### 1. Identify Slow Traces

Find traces where the root span exceeded 1 second:

```sql
SELECT
    TraceId,
    SpanName,
    Duration / 1000000 AS duration_ms,
    Timestamp
FROM read_otlp_traces('traces/*.jsonl')
WHERE
    ParentSpanId IS NULL  -- Root spans only
    AND Duration > 1000000000  -- Over 1 second
ORDER BY Duration DESC
LIMIT 5;
```

### 2. Drill into a Specific Trace

Take one `TraceId` and see all its spans:

```sql
-- Replace with your TraceId
WITH target_trace AS (
    SELECT '0x5b8aa5a2d2c872e8...' AS trace_id
)
SELECT
    SpanName,
    SpanKind,
    Duration / 1000000 AS duration_ms,
    ParentSpanId,
    StatusCode
FROM read_otlp_traces('traces/*.jsonl')
WHERE TraceId = (SELECT trace_id FROM target_trace)
ORDER BY Timestamp;
```

**Output shows the span hierarchy:**
```
┌─────────────────┬───────────┬──────────────┬──────────────┬────────────┐
│    SpanName     │ SpanKind  │ duration_ms  │ ParentSpanId │ StatusCode │
├─────────────────┼───────────┼──────────────┼──────────────┼────────────┤
│ POST /checkout  │  SERVER   │    1523.4    │     NULL     │    OK      │
│   validate-cart │  INTERNAL │     145.2    │   0x7a3f...  │    OK      │
│   query-db      │  CLIENT   │     892.1    │   0x7a3f...  │    OK      │
│   send-email    │  CLIENT   │     486.1    │   0x7a3f...  │    OK      │
└─────────────────┴───────────┴──────────────┴──────────────┴────────────┘
```

**What this tells you:** The database query (`query-db`) is the bottleneck at 892ms.

### 3. Aggregate Across Multiple Traces

See which operations are consistently slow:

```sql
SELECT
    SpanName,
    COUNT(*) AS occurrence_count,
    AVG(Duration) / 1000000 AS avg_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Duration) / 1000000 AS p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY Duration) / 1000000 AS p99_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE SpanKind = 'CLIENT'  -- External calls
GROUP BY SpanName
HAVING COUNT(*) > 100  -- Only frequent operations
ORDER BY p95_ms DESC;
```

## Advanced Patterns

### Find Spans with Specific Attributes

Query spans that have certain resource or span attributes:

```sql
SELECT
    SpanName,
    ServiceName,
    Attributes['http.method'] AS http_method,
    Attributes['http.status_code'] AS status_code,
    Duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE
    Attributes['http.method'] = 'POST'
    AND Attributes['http.status_code']::INTEGER >= 500
ORDER BY Duration DESC
LIMIT 10;
```

### Trace Dependencies

Identify which services call which:

```sql
SELECT DISTINCT
    parent.ServiceName AS caller_service,
    child.ServiceName AS callee_service,
    COUNT(*) AS call_count
FROM read_otlp_traces('traces/*.jsonl') AS parent
JOIN read_otlp_traces('traces/*.jsonl') AS child
  ON parent.SpanId = child.ParentSpanId
  AND parent.TraceId = child.TraceId
WHERE parent.ServiceName != child.ServiceName
GROUP BY parent.ServiceName, child.ServiceName
ORDER BY call_count DESC;
```

### Time-Series Analysis

Analyze request latency over time:

```sql
SELECT
    DATE_TRUNC('hour', Timestamp) AS hour,
    SpanName,
    AVG(Duration) / 1000000 AS avg_duration_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE
    SpanKind = 'SERVER'
    AND Timestamp >= NOW() - INTERVAL 24 HOURS
GROUP BY hour, SpanName
ORDER BY hour, avg_duration_ms DESC;
```

## Understanding the Output

**Duration Units**: Duration is in nanoseconds. Divide by 1,000,000 for milliseconds.

**SpanKind Values**:
- `SERVER` - Server-side request handling
- `CLIENT` - Client-side (outgoing) requests
- `INTERNAL` - Internal operations
- `PRODUCER` - Message queue publishing
- `CONSUMER` - Message queue consumption

**StatusCode Values**:
- `OK` - Success
- `ERROR` - Failure
- `UNSET` - Status not explicitly set

## Export Results

Save your analysis for later:

```sql
-- Export slow traces to Parquet
COPY (
    SELECT * FROM read_otlp_traces('traces/*.jsonl')
    WHERE Duration > 1000000000
) TO 'slow_traces.parquet' (FORMAT PARQUET);

-- Create a table for dashboard queries
CREATE TABLE traces_hourly_summary AS
SELECT
    DATE_TRUNC('hour', Timestamp) AS hour,
    ServiceName,
    COUNT(*) AS request_count,
    AVG(Duration) / 1000000 AS avg_duration_ms
FROM read_otlp_traces('traces/*.jsonl')
GROUP BY hour, ServiceName;
```

## Next Steps

- **[Filter Logs](filtering-logs.md)** - Correlate traces with log errors
- **[Working with Metrics](working-with-metrics.md)** - Combine traces with metrics
- **[Error Handling](error-handling.md)** - Handle malformed trace data
- **[Traces Schema](../reference/traces-schema.md)** - See all available columns

## Troubleshooting

**No results returned**

Check if your files contain trace data:
```sql
SELECT COUNT(*) FROM read_otlp_traces('traces/*.jsonl');
```

**TraceId looks wrong**

TraceId is a 128-bit integer. Convert to hex for readability:
```sql
SELECT format('{:032x}', TraceId) AS trace_id_hex FROM read_otlp_traces(...);
```

**Attributes are NULL**

Not all spans have attributes. Filter for non-null:
```sql
WHERE Attributes IS NOT NULL AND Attributes['key'] IS NOT NULL
```

---

**Related**: [Get Started](../get-started.md) | [Traces Schema](../reference/traces-schema.md) | [Back to Guides](README.md)
