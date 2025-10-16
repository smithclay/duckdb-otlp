# duckspan

Query OpenTelemetry data with SQL. Attach OTLP streams as DuckDB databases, read OTLP files with strongly-typed schemas. Compatible with [Clickhouse OpenTelemetry tables](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md).

## Quick Start

Everything is packaged inside of a duckdb extension. Below are sql statements run inside of a local duckdb process:

```sql
-- Load extension
LOAD duckspan;

-- Attach live OTLP stream (start gRPC receiver on port 4317, buffer in memory)
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);

-- Query accumulated telemetry with strongly-typed columns
SELECT TraceId, SpanName, ServiceName, Duration FROM live.otel_traces;
SELECT MetricName, Value FROM live.otel_metrics_gauge;
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
- Auto-creates ClickHouse-compatible tables (7 tables total):
  - `otel_traces` - 22 strongly-typed columns
  - `otel_logs` - 15 strongly-typed columns
  - `otel_metrics_gauge` - 10 columns for gauge metrics
  - `otel_metrics_sum` - 12 columns for sum/counter metrics
  - `otel_metrics_histogram` - 15 columns for histogram metrics
  - `otel_metrics_exp_histogram` - 19 columns for exponential histogram metrics
  - `otel_metrics_summary` - 13 columns for summary metrics
- OpenTelemetry SDKs send data → DuckDB accumulates it
- `DETACH` stops receiver and removes database

**File Reading**
- Specialized table functions for each signal type
- Supports JSON (`.json`, `.jsonl`) and protobuf (`.pb`) formats (auto-detected)
- Works with local files, S3, HTTP, Azure, GCS
- Same schema as attached tables

**Strongly-Typed Schemas**
- All signals use strongly-typed columns (no JSON extraction required)
- Direct column access: `ServiceName`, `TraceId`, `Duration`, `Value`, etc.
- Compatible with OpenTelemetry ClickHouse exporter schema

## Usage

### ATTACH/DETACH

```sql
-- Start receiver on port 4317
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);

-- Multiple simultaneous streams
ATTACH 'otlp:localhost:4318' AS app2 (TYPE otlp);

-- Cleanup
DETACH live;
DETACH app2;
```

### Query Examples

**Traces** - Find slow requests:
```sql
SELECT
    ServiceName,
    SpanName,
    Duration / 1000000 as duration_ms,
    StatusCode
FROM live.otel_traces
WHERE Duration > 1000000000  -- 1 second in nanoseconds
ORDER BY Duration DESC LIMIT 10;
```

**Logs** - Filter by severity:
```sql
SELECT
    Timestamp,
    ServiceName,
    SeverityText,
    Body
FROM live.otel_logs
WHERE SeverityText IN ('ERROR', 'FATAL')
ORDER BY Timestamp DESC;
```

**Metrics** - Query gauges:
```sql
SELECT
    Timestamp,
    ServiceName,
    MetricName,
    Value
FROM live.otel_metrics_gauge
WHERE MetricName = 'system.memory.usage'
ORDER BY Timestamp DESC;
```

### Persisting Data

```sql
-- Archive live data to permanent tables
CREATE TABLE archive_traces AS SELECT * FROM live.otel_traces;
CREATE TABLE archive_logs AS SELECT * FROM live.otel_logs;
CREATE TABLE archive_metrics AS SELECT * FROM live.otel_metrics_gauge;

-- Export to files
COPY (SELECT * FROM live.otel_traces) TO 'traces.parquet';
COPY (SELECT * FROM live.otel_logs) TO 'logs.csv';
```

## Schemas

All tables use strongly-typed columns compatible with the OpenTelemetry ClickHouse exporter schema:

**Traces Table** - 22 columns including:
- `TraceId`, `SpanId`, `ParentSpanId` - Trace identifiers
- `SpanName`, `SpanKind` - Span metadata
- `ServiceName` - Extracted from resource attributes
- `Duration` - Calculated from start/end timestamps
- `StatusCode`, `StatusMessage` - Span status
- `ResourceAttributes`, `Attributes` - Key-value maps
- `Events`, `Links` - Nested structured data

**Logs Table** - 15 columns including:
- `Timestamp`, `ObservedTimestamp` - Temporal data
- `SeverityText`, `SeverityNumber` - Log level
- `Body` - Log message content
- `ServiceName` - Extracted from resource attributes
- `TraceId`, `SpanId` - Trace correlation
- `ResourceAttributes`, `Attributes` - Key-value maps

**Metrics Tables** - 5 separate tables by type:
- `otel_metrics_gauge` - 10 columns for gauge metrics
- `otel_metrics_sum` - 12 columns for sum/counter metrics
- `otel_metrics_histogram` - 15 columns for histogram metrics
- `otel_metrics_exp_histogram` - 19 columns for exponential histograms
- `otel_metrics_summary` - 13 columns for summary metrics

All metric tables share common base columns: `Timestamp`, `ServiceName`, `MetricName`, `MetricDescription`, `MetricUnit`, `ResourceAttributes`, `ScopeName`, `ScopeVersion`, `Attributes`, plus type-specific fields like `Value`, `Count`, `Sum`, `BucketCounts`, etc.

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
make test-otlp-export
```

## Development

### Pre-commit Hooks

```bash
# Install hooks
uvx --from pre-commit pre-commit install

# Run manually
uvx --from pre-commit pre-commit run --all-files
```

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
