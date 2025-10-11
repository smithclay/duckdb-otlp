# duckspan

Query OpenTelemetry data with SQL. Attach OTLP streams as DuckDB databases, read OTLP files.

## Quick Start

```sql
-- Load extension
LOAD duckspan;

-- Attach live OTLP stream (starts gRPC receiver on port 4317)
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);

-- Query accumulated telemetry
SELECT * FROM live.traces;
SELECT * FROM live.metrics;
SELECT * FROM live.logs;

-- Stop receiver
DETACH live;

-- Read OTLP files (JSON or protobuf, auto-detected)
SELECT * FROM read_otlp('traces.jsonl');
SELECT * FROM read_otlp('s3://bucket/traces/*.pb');
```

## Features

**Live OTLP Streams**
- `ATTACH` creates a gRPC receiver on specified port
- Auto-creates tables: `{name}.traces`, `{name}.metrics`, `{name}.logs`
- OpenTelemetry SDKs send data → DuckDB accumulates it
- `DETACH` stops receiver and removes database

**File Reading**
- `read_otlp(filepath)` reads OTLP JSON/protobuf files
- Supports JSON and protobuf formats (auto-detected)
- Works with local files, S3, HTTP, Azure, GCS
- Same schema as attached tables

**Unified Schema**
- All signal types use: `(timestamp TIMESTAMP, resource JSON, data JSON)`
- Query with DuckDB's JSON functions
- `resource` = service/host metadata
- `data` = signal-specific payload

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
-- Extract service name and span details
SELECT
    json_extract(resource, '$.service.name') as service,
    json_extract(data, '$.name') as span_name,
    json_extract(data, '$.traceId') as trace_id,
    timestamp
FROM live.traces
WHERE timestamp > NOW() - INTERVAL 1 HOUR;

-- Find slow requests (duration > 1 second)
SELECT
    json_extract(resource, '$.service.name') as service,
    json_extract(data, '$.name') as operation,
    (json_extract(data, '$.endTimeUnixNano')::BIGINT -
     json_extract(data, '$.startTimeUnixNano')::BIGINT) / 1000000 as duration_ms
FROM live.traces
WHERE duration_ms > 1000
ORDER BY duration_ms DESC;

-- Error rate by service
SELECT
    json_extract(resource, '$.service.name') as service,
    COUNT(*) as total_spans,
    SUM(CASE WHEN json_extract(data, '$.status.code') != 0 THEN 1 ELSE 0 END) as errors,
    (errors::FLOAT / total_spans * 100)::INT as error_pct
FROM live.traces
WHERE timestamp > NOW() - INTERVAL 15 MINUTES
GROUP BY service;
```

### Reading Files

```sql
-- Local files
SELECT COUNT(*) FROM read_otlp('test/data/traces.jsonl');

-- S3 (with DuckDB's S3 support)
SELECT * FROM read_otlp('s3://bucket/telemetry/*.jsonl');

-- HTTP
SELECT * FROM read_otlp('https://example.com/traces.jsonl');

-- Protobuf (auto-detected)
SELECT * FROM read_otlp('traces.pb');

-- Verify schema
SELECT
    typeof(timestamp) as ts_type,
    typeof(resource) as resource_type,
    typeof(data) as data_type
FROM read_otlp('traces.jsonl')
LIMIT 1;
```

### Persisting Stream Data

```sql
-- Attach and accumulate data
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);

-- Let telemetry flow in for a while...

-- Save to permanent tables
CREATE TABLE archive_traces AS SELECT * FROM live.traces;
CREATE TABLE archive_metrics AS SELECT * FROM live.metrics;

-- Stop receiver
DETACH live;

-- Query archived data
SELECT * FROM archive_traces WHERE timestamp > NOW() - INTERVAL 1 DAY;
```

## Data Model

All signal types (traces, metrics, logs) use the same schema:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | TIMESTAMP | When the event occurred |
| `resource` | JSON | Service/host metadata |
| `data` | JSON | Signal-specific payload |

**Example Row:**
```json
{
  "timestamp": "2024-01-15 10:30:00",
  "resource": {
    "service.name": "api-server",
    "host.name": "prod-01"
  },
  "data": {
    "traceId": "5B8EFFF798038103D269B633813FC60C",
    "spanId": "EEE19B7EC3C1B174",
    "name": "GET /users",
    "startTimeUnixNano": "1640000000000000000",
    "endTimeUnixNano": "1640000000100000000",
    "attributes": {
      "http.method": "GET",
      "http.url": "/users",
      "http.status_code": 200
    },
    "status": {"code": 0}
  }
}
```

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
- **ATTACH Handler**: Parses connection string, starts gRPC server, creates tables
- **gRPC Receiver**: Implements OTLP collector service endpoints (traces/metrics/logs)
- **Table Functions**: `read_otlp()` streams OTLP files with format auto-detection
- **Data Conversion**: Protobuf → JSON for SQL queryability

### Dependencies (via VCPKG)
- gRPC (OTLP receiver)
- Protobuf (wire format)
- OpenSSL (gRPC dependency)

## Limitations

**Data Retention**: Attached streams accumulate data in memory indefinitely. Manual cleanup required:

```sql
-- Option 1: Periodic DELETE
DELETE FROM live.traces WHERE timestamp < NOW() - INTERVAL 1 HOUR;

-- Option 2: Periodic persist-and-detach
CREATE TABLE archive AS SELECT * FROM live.traces;
DETACH live;
ATTACH 'otlp:localhost:4317' AS live (TYPE otlp);
```

Future versions may add automatic TTL or persistent backing.

## References

- [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
- [Extension Template](https://github.com/duckdb/extension-template)
