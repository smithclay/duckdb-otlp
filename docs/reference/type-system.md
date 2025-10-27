# Type System Reference

The DuckDB OTLP extension maps OpenTelemetry data types to native DuckDB types for efficient storage and querying.

## Type Mappings

| OTLP Type | DuckDB Type | Notes |
|-----------|-------------|-------|
| string | `VARCHAR` | UTF-8 text |
| int64 | `BIGINT` | 64-bit signed integer |
| uint64 | `UBIGINT` | 64-bit unsigned integer |
| double | `DOUBLE` | 64-bit floating point |
| bool | `BOOLEAN` | true/false |
| bytes | `BLOB` | Binary data |
| fixed64 (trace/span IDs) | `HUGEINT` | 128-bit integer |
| timestamp (nanoseconds) | `TIMESTAMP` | DuckDB timestamp (microsecond precision) |
| KeyValue[] | `MAP<VARCHAR, VARCHAR>` | String-to-string attributes |
| repeated | `LIST<T>` | Arrays/lists |
| message | `STRUCT<...>` | Nested structures |

## Trace and Span IDs

OpenTelemetry uses 128-bit (16-byte) identifiers for traces and spans. DuckDB stores these as `HUGEINT`.

### Convert to Hex String

```sql
SELECT format('{:032x}', TraceId) AS trace_id_hex
FROM read_otlp_traces('traces.jsonl');
```

### Compare IDs

```sql
-- IDs can be compared directly
SELECT * FROM read_otlp_traces('traces.jsonl')
WHERE TraceId = 123456789012345678901234567890;
```

## Attribute Maps

Resource and span attributes use DuckDB's `MAP<VARCHAR, VARCHAR>` type.

### Access Map Values

```sql
-- Dot notation doesn't work - use bracket syntax
SELECT ResourceAttributes['service.name'] AS service
FROM read_otlp_traces('traces.jsonl');

-- Check if key exists
SELECT * FROM read_otlp_traces('traces.jsonl')
WHERE map_contains(ResourceAttributes, 'service.name');
```

### Convert Map to JSON

```sql
SELECT to_json(ResourceAttributes) AS attributes_json
FROM read_otlp_traces('traces.jsonl');
```

### Filter by Attribute

```sql
SELECT * FROM read_otlp_logs('logs.jsonl')
WHERE ResourceAttributes['deployment.environment'] = 'prod';
```

## Lists

Lists use DuckDB's `LIST<T>` type for repeated fields.

### Histogram Buckets

```sql
SELECT
  MetricName,
  BucketCounts,
  ExplicitBounds
FROM read_otlp_metrics_histogram('metrics.jsonl');
```

### Unnest Lists

```sql
-- Expand list to rows
SELECT
  MetricName,
  unnest(BucketCounts) AS bucket_count,
  unnest(ExplicitBounds) AS bound
FROM read_otlp_metrics_histogram('metrics.jsonl');
```

## Structs

Events and links use `STRUCT` types for nested data.

### Access Struct Fields

```sql
-- Events are LIST<STRUCT>
SELECT
  SpanName,
  unnest(Events) AS event
FROM read_otlp_traces('traces.jsonl');

-- Access struct fields
SELECT
  SpanName,
  unnest(Events).name AS event_name,
  unnest(Events).timestamp AS event_time
FROM read_otlp_traces('traces.jsonl');
```

## Timestamps

OpenTelemetry uses nanosecond-precision timestamps. DuckDB's `TIMESTAMP` type stores microseconds, but the extension preserves nanosecond values.

### Timestamp Comparisons

```sql
SELECT * FROM read_otlp_logs('logs.jsonl')
WHERE Timestamp >= '2024-01-01 00:00:00'
  AND Timestamp < '2024-01-02 00:00:00';
```

### Time Windows

```sql
SELECT
  date_trunc('hour', Timestamp) AS hour,
  COUNT(*) AS span_count
FROM read_otlp_traces('traces.jsonl')
GROUP BY hour;
```

## Durations

Durations are stored as nanoseconds (`BIGINT`).

### Convert to Human-Readable

```sql
SELECT
  SpanName,
  Duration / 1000000000 AS duration_seconds,
  Duration / 1000000 AS duration_milliseconds,
  Duration / 1000 AS duration_microseconds
FROM read_otlp_traces('traces.jsonl');
```

## NULL Handling

Fields may be NULL when:
- Not present in the OTLP data
- Parse error occurred (with `on_error := 'nullify'`)
- Optional field was omitted

```sql
-- Filter out nulls
SELECT * FROM read_otlp_traces('traces.jsonl')
WHERE ParentSpanId IS NOT NULL;

-- Count nulls
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN ParentSpanId IS NULL THEN 1 ELSE 0 END) AS null_count
FROM read_otlp_traces('traces.jsonl');
```

## Type Conversions

### CAST Operations

```sql
-- Convert to string
SELECT CAST(TraceId AS VARCHAR) FROM read_otlp_traces('traces.jsonl');

-- Convert timestamp
SELECT CAST(Timestamp AS DATE) FROM read_otlp_logs('logs.jsonl');
```

### JSON Export

```sql
-- Export entire row as JSON
SELECT to_json(row(*)) AS trace_json
FROM read_otlp_traces('traces.jsonl');
```

## ClickHouse Compatibility

See [ClickHouse Compatibility](../clickhouse-compatibility.md#type-mappings) for type mappings between ClickHouse and DuckDB.

## See Also

- [Schema Reference](schemas.md) - Complete column layouts
- [API Reference](api.md) - Table function signatures
- [DuckDB Types](https://duckdb.org/docs/sql/data_types/overview) - DuckDB type system
