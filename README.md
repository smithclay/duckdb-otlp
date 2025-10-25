# duckspan

Query OpenTelemetry data with SQL using strongly-typed OTLP file readers. Compatible with [Clickhouse OpenTelemetry tables](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md).

## Quick Start

Everything is packaged inside of a duckdb extension. Below are sql statements run inside of a local duckdb process:

```sql
-- Load extension
LOAD duckspan;

-- Read OTLP files (JSON or protobuf, auto-detected)
SELECT * FROM read_otlp_traces('traces.jsonl');
SELECT * FROM read_otlp_metrics('s3://bucket/metrics/*.pb');
SELECT * FROM read_otlp_logs('https://example.com/logs.jsonl');

-- Persist telemetry
CREATE TABLE traces AS
SELECT TraceId, SpanName, ServiceName, Duration
FROM read_otlp_traces('traces.jsonl');
```

## Features

**File Reading**
- Specialized table functions for each signal type
- Supports JSON (`.json`, `.jsonl`) and protobuf (`.pb`) formats (auto-detected)
- Works with local files, S3, HTTP, Azure, GCS
- Consistent schema across traces, logs, and metrics table functions
- Helper table functions: `read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}()` for direct access to specific metric shapes
- Named parameters: `on_error` (fail | skip | nullify) with stats via `read_otlp_scan_stats()`

**Strongly-Typed Schemas**
- All signals use strongly-typed columns (no JSON extraction required)
- Direct column access: `ServiceName`, `TraceId`, `Duration`, `Value`, etc.
- Compatible with OpenTelemetry ClickHouse exporter schema

## Platform Support

**Native Builds** (desktop/server)
- JSON and protobuf file format support
- Table functions: `read_otlp_traces`, `read_otlp_logs`, `read_otlp_metrics`
- Helper table functions: `read_otlp_options()`, `read_otlp_scan_stats()`
- Scalar helper: `duckspan('name')`

**WASM Builds** (browser/WebAssembly)
- Same feature set as native builds (JSON + protobuf file formats)
- Ship the same table functions: `read_otlp_traces`, `read_otlp_logs`, `read_otlp_metrics`
- Helper table functions available (`read_otlp_options()`, `read_otlp_scan_stats()`)

## Usage

### Query Examples

**Traces** - Find slow requests from an input file:
```sql
WITH traces AS (
    SELECT * FROM read_otlp_traces('traces.jsonl')
)
SELECT
    ServiceName,
    SpanName,
    Duration / 1000000 as duration_ms,
    StatusCode
FROM traces
WHERE Duration > 1000000000  -- 1 second in nanoseconds
ORDER BY Duration DESC LIMIT 10;
```

**read_otlp options** - Discover named parameters:
```sql
SELECT * FROM read_otlp_options();
```

**read_otlp on_error** - Skip malformed rows but keep the scan running:
```sql
SELECT *
FROM read_otlp_traces('s3://otel-bucket/traces-*.jsonl', on_error='skip');
```

**read_otlp scan stats** - Inspect parse failures from the last scan in this connection:
```sql
SELECT *
FROM read_otlp_scan_stats();
```

**Logs** - Filter by severity while reading from S3:
```sql
SELECT
    Timestamp,
    ServiceName,
    SeverityText,
    Body
FROM read_otlp_logs('s3://otel-bucket/logs-*.jsonl')
WHERE SeverityText IN ('ERROR', 'FATAL')
ORDER BY Timestamp DESC;
```

**Metrics** - Query gauges and aggregate CPU usage:
```sql
WITH metrics AS (
    SELECT *
    FROM read_otlp_metrics('metrics.pb')
    WHERE MetricType = 'gauge'
)
SELECT ServiceName, MetricName, AVG(Value) AS avg_value
FROM metrics
WHERE MetricName LIKE 'system.cpu%'
GROUP BY ServiceName, MetricName
ORDER BY avg_value DESC;
```

**Metrics Helpers** - Retrieve typed metric tables without manual filtering:
```sql
SELECT *
FROM read_otlp_metrics_gauge('metrics.jsonl')
WHERE MetricName = 'system.memory.usage';

SELECT Timestamp, Count, BucketCounts
FROM read_otlp_metrics_histogram('metrics.jsonl')
WHERE ServiceName = 'api';
```

See `docs/metrics_helpers.md` for more cookbook-style examples.

### Persisting Data

```sql
CREATE TABLE traces AS
SELECT TraceId, SpanName, ServiceName, Duration
FROM read_otlp_traces('traces.jsonl');

CREATE TABLE logs AS
SELECT Timestamp, ServiceName, SeverityText, Body
FROM read_otlp_logs('logs.jsonl');

CREATE TABLE metrics_gauge AS
SELECT Timestamp, ServiceName, MetricName, Value
FROM read_otlp_metrics('metrics.pb')
WHERE MetricType = 'gauge';
```

### Transferring Metrics Between Union and Typed Schemas

`read_otlp_metrics()` returns a union schema (27 columns) with a `MetricType` discriminator column containing all metric types. You can project that union into typed tables or views:

```sql
CREATE TABLE archive_sum AS
SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
       ResourceAttributes, ScopeName, ScopeVersion, Attributes,
       Value, AggregationTemporality, IsMonotonic
FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'sum';

CREATE TABLE archive_histogram AS
SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
       ResourceAttributes, ScopeName, ScopeVersion, Attributes,
       Count, Sum, BucketCounts, ExplicitBounds, Min, Max
FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'histogram';
```

## Schemas

All table functions use strongly-typed columns compatible with the [OpenTelemetry ClickHouse exporter schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter):

- **Traces** (`read_otlp_traces()`) - 22 columns with trace identifiers, span metadata, resource attributes, events, and links
- **Logs** (`read_otlp_logs()`) - 15 columns with timestamps, severity levels, log bodies, and trace correlation
- **Metrics** (`read_otlp_metrics()`) - Union schema with 27 columns covering all 5 metric types (gauge, sum, histogram, exponential histogram, summary)
- **Typed Metrics Helpers** - Dedicated functions (`read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}()`) that return only relevant columns for each metric type

For detailed schema information including all column names, types, and descriptions, see [docs/SCHEMA.md](docs/SCHEMA.md).

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

- **Extension Type**: Table functions that stream OTLP files into DuckDB
- **Parsing Pipeline**: Format detector → JSON or protobuf parser → typed row builders
- **Table Functions**: `read_otlp_traces()`, `read_otlp_logs()`, `read_otlp_metrics()` with format auto-detection
- **Schema**: ClickHouse-compatible strongly-typed columns (no JSON extraction required)

### Dependencies (via VCPKG)
- Protobuf (binary OTLP parsing, optional for JSON-only environments)

## Limitations

- File readers operate in batch mode; there is no live gRPC ingestion
- Protobuf support depends on native builds with the protobuf runtime available
- Large protobuf blobs are materialized per file; chunked streaming beyond DuckDB's scan size is not implemented yet

## References

- [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/)
- [OpenTelemetry ClickHouse Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
- [Extension Template](https://github.com/duckdb/extension-template)
