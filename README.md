# duckspan

Query OpenTelemetry data with SQL. Attach OTLP streams as DuckDB databases, read OTLP files with strongly-typed schemas.

## Quick Start

```sql
-- Load extension
LOAD duckspan;

-- Attach live OTLP stream (starts gRPC receiver on port 4317)
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);

-- Query accumulated telemetry with strongly-typed columns
SELECT TraceId, SpanName, ServiceName, Duration FROM live.otel_traces;
SELECT MetricName, MetricType, Value FROM live.otel_metrics WHERE MetricType = 'gauge';
SELECT SeverityText, Body, ServiceName FROM live.otel_logs;

-- Stop receiver
DETACH live;

-- Read OTLP files (JSON or protobuf, auto-detected)
SELECT * FROM read_otlp_traces('traces.jsonl');
SELECT * FROM read_otlp_metrics('s3://bucket/metrics/*.pb');
SELECT * FROM read_otlp_logs('https://example.com/logs.jsonl');
```

## Features

**Live OTLP Streams**
- `ATTACH` creates a gRPC receiver on specified port
- Auto-creates ClickHouse-compatible tables:
  - `{name}.otel_traces` - 22 strongly-typed columns
  - `{name}.otel_logs` - 15 strongly-typed columns
  - `{name}.otel_metrics` - 27 columns with union schema (MetricType discriminator)
- OpenTelemetry SDKs send data → DuckDB accumulates it
- `DETACH` stops receiver and removes database

**File Reading**
- Specialized table functions for each signal type:
  - `read_otlp_traces(filepath)` - returns 22 columns
  - `read_otlp_logs(filepath)` - returns 15 columns
  - `read_otlp_metrics(filepath)` - returns 27 columns (union schema)
- Supports JSON (`.json`, `.jsonl`) and protobuf (`.pb`) formats (auto-detected)
- Works with local files, S3, HTTP, Azure, GCS
- Same schema as attached tables

**Strongly-Typed Schemas**
- All signals use strongly-typed columns (no JSON extraction required)
- Traces: TraceId, SpanId, SpanName, ServiceName, Duration, Attributes, etc.
- Logs: Timestamp, ServiceName, SeverityText, Body, etc.
- Metrics: MetricType discriminator, type-specific union columns
- Compatible with OpenTelemetry ClickHouse exporter schema

## Usage

### ATTACH/DETACH Lifecycle

```sql
-- Start receiver on port 4317
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);

-- Different port
ATTACH 'otlp:0.0.0.0:4318' AS metrics (TYPE otlp);

-- Multiple simultaneous streams
ATTACH 'otlp:localhost:4317' AS traces1 (TYPE otlp);
ATTACH 'otlp:localhost:4318' AS traces2 (TYPE otlp);

-- Cleanup
DETACH traces1;
DETACH traces2;
```

### Querying Traces

```sql
-- Direct column access (no JSON extraction needed!)
SELECT
    ServiceName,
    SpanName,
    TraceId,
    Duration / 1000000 as duration_ms,
    Timestamp
FROM live.otel_traces
WHERE Timestamp > NOW() - INTERVAL 1 HOUR;

-- Find slow requests (duration > 1 second)
SELECT
    ServiceName,
    SpanName,
    Duration / 1000000 as duration_ms,
    SpanKind,
    StatusCode
FROM live.otel_traces
WHERE Duration > 1000000000  -- 1 second in nanoseconds
ORDER BY Duration DESC
LIMIT 10;

-- Error rate by service
SELECT
    ServiceName,
    COUNT(*) as total_spans,
    SUM(CASE WHEN StatusCode != 'STATUS_CODE_UNSET' THEN 1 ELSE 0 END) as errors,
    (errors::FLOAT / total_spans * 100)::INT as error_pct
FROM live.otel_traces
WHERE Timestamp > NOW() - INTERVAL 15 MINUTES
GROUP BY ServiceName;

-- Trace spans by parent-child relationships
SELECT
    TraceId,
    SpanId,
    ParentSpanId,
    SpanName,
    Duration / 1000000 as duration_ms
FROM live.otel_traces
WHERE TraceId = '5B8EFFF798038103D269B633813FC60C'
ORDER BY Timestamp;
```

### Querying Logs

```sql
-- Filter logs by severity
SELECT
    Timestamp,
    ServiceName,
    SeverityText,
    Body
FROM live.otel_logs
WHERE SeverityText IN ('ERROR', 'FATAL')
ORDER BY Timestamp DESC;

-- Count logs by service and severity
SELECT
    ServiceName,
    SeverityText,
    COUNT(*) as log_count
FROM live.otel_logs
WHERE Timestamp > NOW() - INTERVAL 1 HOUR
GROUP BY ServiceName, SeverityText
ORDER BY log_count DESC;

-- Correlate logs with traces
SELECT
    l.Timestamp,
    l.ServiceName,
    l.Body,
    t.SpanName
FROM live.otel_logs l
LEFT JOIN live.otel_traces t ON l.TraceId = t.TraceId
WHERE l.SeverityText = 'ERROR'
LIMIT 100;
```

### Querying Metrics

```sql
-- Query gauge metrics (filter by MetricType)
SELECT
    Timestamp,
    ServiceName,
    MetricName,
    Value
FROM live.otel_metrics
WHERE MetricType = 'gauge' AND MetricName = 'system.memory.usage'
ORDER BY Timestamp DESC;

-- Query sum metrics (counters)
SELECT
    ServiceName,
    MetricName,
    Value,
    IsMonotonic,
    AggregationTemporality
FROM live.otel_metrics
WHERE MetricType = 'sum' AND MetricName = 'http.server.requests';

-- Query histogram metrics
SELECT
    MetricName,
    Count,
    Sum,
    Min,
    Max,
    Sum / Count as avg_value
FROM live.otel_metrics
WHERE MetricType = 'histogram' AND MetricName = 'http.server.duration'
ORDER BY Timestamp DESC
LIMIT 10;

-- Union query across all metric types
SELECT
    Timestamp,
    ServiceName,
    MetricName,
    MetricType,
    Value,  -- Populated for gauge/sum only
    Count,  -- Populated for histogram/summary
    Sum     -- Populated for histogram/summary
FROM read_otlp_metrics('metrics.jsonl')
WHERE ServiceName = 'api-server'
ORDER BY Timestamp;
```

### Reading Files

```sql
-- Local traces file
SELECT COUNT(*) FROM read_otlp_traces('test/data/traces.jsonl');

-- S3 logs (with DuckDB's S3 support)
SELECT * FROM read_otlp_logs('s3://bucket/logs/*.jsonl');

-- HTTP metrics
SELECT * FROM read_otlp_metrics('https://example.com/metrics.jsonl');

-- Protobuf (auto-detected)
SELECT * FROM read_otlp_traces('traces.pb');

-- Verify traces schema (22 columns)
SELECT
    TraceId,
    SpanId,
    SpanName,
    ServiceName,
    Duration,
    StatusCode
FROM read_otlp_traces('traces.jsonl')
LIMIT 1;

-- Verify metrics schema (27 columns with union)
SELECT
    MetricName,
    MetricType,
    Value,
    Count,
    Sum
FROM read_otlp_metrics('metrics.jsonl')
LIMIT 1;
```

### Persisting Stream Data

```sql
-- Attach and accumulate data
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);

-- Let telemetry flow in for a while...

-- Save to permanent tables
CREATE TABLE archive_traces AS SELECT * FROM live.otel_traces;
CREATE TABLE archive_metrics AS SELECT * FROM live.otel_metrics;
CREATE TABLE archive_logs AS SELECT * FROM live.otel_logs;

-- Stop receiver
DETACH live;

-- Query archived data
SELECT * FROM archive_traces WHERE Timestamp > NOW() - INTERVAL 1 DAY;
```

## Data Model

### Traces Schema (22 columns)

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp` | TIMESTAMP_NS | Span start time |
| `TraceId` | VARCHAR | Hex-encoded trace ID |
| `SpanId` | VARCHAR | Hex-encoded span ID |
| `ParentSpanId` | VARCHAR | Hex-encoded parent span ID |
| `TraceState` | VARCHAR | W3C trace state |
| `SpanName` | VARCHAR | Operation name |
| `SpanKind` | VARCHAR | Span kind (SERVER, CLIENT, etc.) |
| `ServiceName` | VARCHAR | Service name (extracted from resource) |
| `ResourceAttributes` | MAP(VARCHAR, VARCHAR) | Resource attributes |
| `ScopeName` | VARCHAR | Instrumentation scope name |
| `ScopeVersion` | VARCHAR | Instrumentation scope version |
| `SpanAttributes` | MAP(VARCHAR, VARCHAR) | Span attributes |
| `Duration` | BIGINT | Span duration in nanoseconds |
| `StatusCode` | VARCHAR | Span status code |
| `StatusMessage` | VARCHAR | Span status message |
| `EventsTimestamp` | LIST(TIMESTAMP_NS) | Event timestamps |
| `EventsName` | LIST(VARCHAR) | Event names |
| `EventsAttributes` | LIST(MAP) | Event attributes |
| `LinksTraceId` | LIST(VARCHAR) | Linked trace IDs |
| `LinksSpanId` | LIST(VARCHAR) | Linked span IDs |
| `LinksTraceState` | LIST(VARCHAR) | Linked trace states |
| `LinksAttributes` | LIST(MAP) | Link attributes |

### Logs Schema (15 columns)

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp` | TIMESTAMP_NS | Log record timestamp |
| `TraceId` | VARCHAR | Hex-encoded trace ID (for correlation) |
| `SpanId` | VARCHAR | Hex-encoded span ID (for correlation) |
| `TraceFlags` | UINTEGER | W3C trace flags |
| `SeverityText` | VARCHAR | Severity text (INFO, WARN, ERROR, etc.) |
| `SeverityNumber` | INTEGER | Severity number |
| `ServiceName` | VARCHAR | Service name (extracted from resource) |
| `Body` | VARCHAR | Log message body |
| `ResourceSchemaUrl` | VARCHAR | Resource schema URL |
| `ResourceAttributes` | MAP(VARCHAR, VARCHAR) | Resource attributes |
| `ScopeSchemaUrl` | VARCHAR | Scope schema URL |
| `ScopeName` | VARCHAR | Instrumentation scope name |
| `ScopeVersion` | VARCHAR | Instrumentation scope version |
| `ScopeAttributes` | MAP(VARCHAR, VARCHAR) | Scope attributes |
| `LogAttributes` | MAP(VARCHAR, VARCHAR) | Log record attributes |

### Metrics Union Schema (27 columns)

All metric types share a base schema with type-specific union columns:

**Base Columns (9):**
- `Timestamp` (TIMESTAMP_NS)
- `ServiceName` (VARCHAR)
- `MetricName` (VARCHAR)
- `MetricDescription` (VARCHAR)
- `MetricUnit` (VARCHAR)
- `ResourceAttributes` (MAP)
- `ScopeName` (VARCHAR)
- `ScopeVersion` (VARCHAR)
- `Attributes` (MAP)

**Discriminator:**
- `MetricType` (VARCHAR) - one of: `'gauge'`, `'sum'`, `'histogram'`, `'exponential_histogram'`, `'summary'`

**Union Columns (type-specific, NULL for other types):**
- `Value` (DOUBLE) - gauge, sum
- `AggregationTemporality` (INTEGER) - sum, histogram, exponential_histogram
- `IsMonotonic` (BOOLEAN) - sum
- `Count` (UBIGINT) - histogram, exponential_histogram, summary
- `Sum` (DOUBLE) - histogram, exponential_histogram, summary
- `Min` (DOUBLE) - histogram, exponential_histogram
- `Max` (DOUBLE) - histogram, exponential_histogram
- `BucketCounts` (LIST(UBIGINT)) - histogram
- `ExplicitBounds` (LIST(DOUBLE)) - histogram
- `Scale` (INTEGER) - exponential_histogram
- `ZeroCount` (UBIGINT) - exponential_histogram
- `PositiveOffset` (INTEGER) - exponential_histogram
- `PositiveBucketCounts` (LIST(UBIGINT)) - exponential_histogram
- `NegativeOffset` (INTEGER) - exponential_histogram
- `NegativeBucketCounts` (LIST(UBIGINT)) - exponential_histogram
- `QuantileValues` (LIST(DOUBLE)) - summary
- `QuantileQuantiles` (LIST(DOUBLE)) - summary

## Building

### Prerequisites

Install VCPKG for dependency management:

```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build

Use ninja for performance:

```bash
GEN=ninja make
```

Builds:
- `./build/release/duckdb` - DuckDB shell with extension loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/duckspan/duckspan.duckdb_extension` - Loadable extension

### Testing

```bash
# SQL tests
make test

# OTLP integration test (requires uv)
uv run test/python/test_otlp_export.py

# Or via make
make test-otlp-export
```

## Development

### Pre-commit Hooks

Uses `uv` for Python tooling:

```bash
# Install hooks
uvx --from pre-commit pre-commit install

# Run manually
uvx --from pre-commit pre-commit run --all-files
```

Hooks:
- clang-format (C++ formatting)
- black (Python formatting)
- cmake-format/cmake-lint

### Code Formatting

```bash
# Check format
make format-check

# Auto-fix
make format-fix
```

## Architecture

- **Extension Type**: Storage extension registered for `TYPE otlp`
- **ATTACH Handler**: Parses connection string, starts gRPC server, creates strongly-typed tables
- **gRPC Receiver**: Implements OTLP collector service endpoints (traces/metrics/logs)
- **Table Functions**: `read_otlp_traces()`, `read_otlp_logs()`, `read_otlp_metrics()` with format auto-detection
- **Schema**: ClickHouse-compatible strongly-typed columns (no JSON extraction required)

### Dependencies (via VCPKG)
- gRPC (OTLP receiver)
- Protobuf (wire format)
- OpenSSL (gRPC dependency)

## Limitations

**Data Retention**: Attached streams accumulate data in memory indefinitely. Manual cleanup required:

```sql
-- Option 1: Periodic DELETE
DELETE FROM live.otel_traces WHERE Timestamp < NOW() - INTERVAL 1 HOUR;

-- Option 2: Periodic persist-and-detach
CREATE TABLE archive AS SELECT * FROM live.otel_traces;
DETACH live;
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);
```

Future versions may add automatic TTL or persistent backing.

## References

- [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/)
- [OpenTelemetry ClickHouse Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
- [Extension Template](https://github.com/duckdb/extension-template)
