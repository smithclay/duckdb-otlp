# ClickHouse Compatibility

The DuckDB OTLP extension uses schemas that mirror the [OpenTelemetry ClickHouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter). This enables seamless data interchange between ClickHouse and DuckDB for OpenTelemetry workloads.

## Schema Alignment

The extension emits the same column layouts as the ClickHouse exporter:

| Signal | Columns | Schema Source |
|--------|---------|---------------|
| **Traces** | 22 | [ClickHouse Traces Schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md#traces-table-schema) |
| **Logs** | 15 | [ClickHouse Logs Schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md#logs-table-schema) |
| **Metrics** | 27 (union) | [ClickHouse Metrics Schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md#metrics-table-schema) |

## Type Mappings

DuckDB types are chosen to match ClickHouse semantics where possible:

| ClickHouse Type | DuckDB Type | Notes |
|-----------------|-------------|-------|
| `String` | `VARCHAR` | UTF-8 text |
| `DateTime64(9)` | `TIMESTAMP` | Nanosecond precision |
| `UInt64` | `UBIGINT` | Unsigned 64-bit integer |
| `Int64` | `BIGINT` | Signed 64-bit integer |
| `Map(String, String)` | `MAP<VARCHAR, VARCHAR>` | Key-value attributes |
| `Array(...)` | `LIST<...>` | Repeated fields |
| `FixedString(16)` | `HUGEINT` | 128-bit trace/span IDs |
| `Nested(...)` | `STRUCT<...>` | Nested structures (events, links) |

### Trace/Span IDs

ClickHouse stores trace and span identifiers as `FixedString(16)` (16-byte binary). DuckDB uses `HUGEINT` (128-bit integer) for efficient storage and arithmetic operations.

**Converting to hex string:**
```sql
SELECT format('{:032x}', TraceId) AS trace_id_hex
FROM read_otlp_traces('traces.jsonl');
```

### Attributes

Both systems store resource and span attributes as string-to-string maps:

**ClickHouse:**
```sql
SELECT ResourceAttributes['service.name'] AS service
FROM otel_traces;
```

**DuckDB (this extension):**
```sql
SELECT ResourceAttributes['service.name'] AS service
FROM read_otlp_traces('traces.jsonl');
```

To convert to JSON:
```sql
SELECT to_json(ResourceAttributes) AS attributes_json
FROM read_otlp_traces('traces.jsonl');
```

## Migration Patterns

### Exporting DuckDB Data to ClickHouse

Convert DuckDB results to Parquet, then use ClickHouse's Parquet import:

```sql
-- In DuckDB
COPY (
  SELECT * FROM read_otlp_traces('traces.jsonl')
) TO 'traces.parquet' (FORMAT PARQUET);
```

```sql
-- In ClickHouse
INSERT INTO otel_traces
SELECT * FROM file('traces.parquet', Parquet);
```

### Querying ClickHouse Exports in DuckDB

If you have ClickHouse data exported to OTLP JSON or protobuf files:

```sql
-- Read ClickHouse file exports directly
SELECT * FROM read_otlp_traces('clickhouse-export/*.jsonl');
```

## Differences and Limitations

### Computed Fields

The extension computes certain derived fields that ClickHouse may store differently:

- **Duration** - Calculated as `EndTimestamp - Timestamp` (nanoseconds)
- **ServiceName** - Extracted from `ResourceAttributes['service.name']`

### Union Schema for Metrics

ClickHouse typically uses separate tables for each metric type:
- `otel_metrics_gauge`
- `otel_metrics_sum`
- `otel_metrics_histogram`
- `otel_metrics_exponential_histogram`
- `otel_metrics_summary`

This extension uses a single union-schema table function (`read_otlp_metrics`) with a `MetricType` discriminator. You can replicate ClickHouse's table structure:

```sql
CREATE TABLE metrics_gauge AS
SELECT * FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'gauge';

CREATE TABLE metrics_sum AS
SELECT * FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'sum';

-- etc...
```

Alternatively, use the typed helper functions:
```sql
CREATE TABLE metrics_gauge AS
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');
```

### Timestamp Precision

Both systems support nanosecond-precision timestamps. DuckDB's `TIMESTAMP` type stores microseconds, but the extension preserves full nanosecond values in the `Timestamp` columns.

## Querying Both Systems

You can use the same SQL patterns across both systems for common queries:

**Find slow traces:**
```sql
-- ClickHouse
SELECT TraceId, SpanName, Duration / 1000000000 AS duration_sec
FROM otel_traces
WHERE Duration > 5000000000
ORDER BY Duration DESC;

-- DuckDB (this extension)
SELECT TraceId, SpanName, Duration / 1000000000 AS duration_sec
FROM read_otlp_traces('traces.jsonl')
WHERE Duration > 5000000000
ORDER BY Duration DESC;
```

**Filter logs by severity:**
```sql
-- ClickHouse
SELECT Timestamp, ServiceName, Body
FROM otel_logs
WHERE SeverityText = 'ERROR';

-- DuckDB (this extension)
SELECT Timestamp, ServiceName, Body
FROM read_otlp_logs('logs.jsonl')
WHERE SeverityText = 'ERROR';
```

## Benefits of Compatibility

- **Interoperability** - Move data between ClickHouse and DuckDB without schema changes
- **Familiar patterns** - Use the same column names and query structure
- **Migration paths** - Start with file-based analysis in DuckDB, scale to ClickHouse later
- **Unified tooling** - OpenTelemetry Collector exporters work with both systems

## See Also

- [Schema Reference](reference/schemas.md) - Complete column layouts
- [API Reference](reference/api.md) - Table function signatures
- [OpenTelemetry ClickHouse Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
