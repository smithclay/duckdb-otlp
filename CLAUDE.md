# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build System

- **Always use `GEN=ninja`** when running make commands for caching and performance: `GEN=ninja make`
- **VCPKG is required** for dependency management. Set environment variable before building:
  ```bash
  export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
  ```
- Use `uv` for Python tooling (formatting, testing, dependency management)

## Common Commands

### Building
```bash
# Build extension with ninja
GEN=ninja make

# Build debug version
GEN=ninja make debug

# Build outputs:
# - ./build/release/duckdb - DuckDB shell with extension loaded
# - ./build/release/test/unittest - Test runner
# - ./build/release/extension/duckspan/duckspan.duckdb_extension - Loadable extension
```

### Testing
```bash
# Run SQL logic tests
make test

# Run debug tests
make test_debug

# Run specific test file
build/release/test/unittest "test/sql/attach_detach.test"

# Run OTLP integration test (Python + gRPC)
uv run test/python/test_otlp_export.py
# or
make test-otlp-export
```

### Code Quality
```bash
# Check C++ and Python formatting
make format-check

# Auto-fix formatting
make format-fix

# Run clang-tidy checks
GEN=ninja make tidy-check

# Pre-commit hooks (uses uv)
uvx --from pre-commit pre-commit run --all-files
```

## Architecture

### Extension Type
This is a **DuckDB Storage Extension** registered for `TYPE otlp`. It implements two main features:
1. **Live OTLP streams** via `ATTACH 'otlp:host:port' AS name (TYPE otlp)`
2. **File reading** via `read_otlp('file.jsonl')` table function

### Core Components

**Storage Extension (`otlp_storage_extension.cpp/hpp`)**
- Implements DuckDB's `StorageExtension` interface
- Handles `ATTACH` lifecycle: parses connection string → starts gRPC receiver → creates virtual catalog

**Custom Catalog (`otlp_catalog.cpp/hpp`)**
- Implements DuckDB's `Catalog` interface for virtual tables
- Auto-creates seven tables per attached database following OpenTelemetry ClickHouse exporter schema:
  - `otel_traces` - trace spans with 22 strongly-typed columns
  - `otel_logs` - log records with 15 strongly-typed columns
  - `otel_metrics_gauge` - gauge metrics with 10 columns
  - `otel_metrics_sum` - sum metrics with 12 columns
  - `otel_metrics_histogram` - histogram metrics with 15 columns
  - `otel_metrics_exp_histogram` - exponential histogram metrics with 19 columns
  - `otel_metrics_summary` - summary metrics with 13 columns
- Backed by thread-safe ring buffers (in-memory FIFO storage)

**Ring Buffer (`ring_buffer.cpp/hpp`)**
- Thread-safe circular buffer with shared-read/exclusive-write semantics
- Stores rows as `vector<Value>` with strongly-typed columns (no JSON)
- Each table type has its own schema with specific column definitions
- FIFO eviction when capacity reached
- Used as backing storage for virtual tables

**gRPC Receiver (`otlp_receiver.cpp/hpp`)**
- Implements OpenTelemetry Collector Protocol gRPC endpoints
- Three service endpoints: `/v1/traces`, `/v1/metrics`, `/v1/logs`
- Parses incoming protobuf messages and extracts strongly-typed columns
- Routes metrics to appropriate buffer based on type (gauge, sum, histogram, exponential histogram, summary)
- Converts trace/span IDs to hex, calculates durations, extracts attributes as MAPs
- Runs on background thread, lifecycle tied to ATTACH/DETACH

**Table Function (`read_otlp.cpp/hpp`)**
- Implements DuckDB table function for reading OTLP files
- Auto-detects format: JSON (newline-delimited) or protobuf
- Supports all DuckDB file systems (local, S3, HTTP, Azure, GCS)
- Streams data without loading entire file into memory

**Format Detection (`format_detector.cpp/hpp`)**
- Sniffs first bytes to determine JSON vs protobuf format
- JSON: looks for `{` or whitespace before `{`
- Protobuf: checks for valid protobuf wire-format field tags

**Parsers**
- `json_parser.cpp/hpp` - Parses OTLP JSON format
- `protobuf_parser.cpp/hpp` - Parses OTLP protobuf format using generated stubs

### Data Flow

**ATTACH Flow:**
```
User: ATTACH 'otlp:localhost:4317' AS live (TYPE otlp)
  ↓
OTLPStorageExtension::Attach()
  ↓ Parses connection string (host:port)
  ↓ Creates OTLPStorageInfo with 7 ring buffers (1 traces, 1 logs, 5 metrics)
  ↓ Starts gRPC receiver on background thread
  ↓
Returns OTLPCatalog with virtual tables
  ↓
User: SELECT * FROM live.otel_traces
  ↓
OTLPScan reads from ring buffer → returns strongly-typed rows
```

**File Reading Flow:**
```
User: SELECT * FROM read_otlp('traces.jsonl')
  ↓
FormatDetector::DetectFormat() - sniff first bytes
  ↓
If JSON: JSONParser::Parse() - streaming JSON parsing
If protobuf: ProtobufParser::Parse() - protobuf deserialization
  ↓
Yields rows: (timestamp, resource JSON, data JSON)

Note: File reading still uses legacy (timestamp, resource, data) schema.
      Future work will update to strongly-typed columns matching ATTACH schema.
```

### Generated Code

The `src/generated/` directory contains protobuf and gRPC stubs generated from OpenTelemetry `.proto` files:
- Message definitions: `*.pb.h`, `*.pb.cc`
- Service definitions: `*.grpc.pb.h`, `*.grpc.pb.cc`

**Do not edit these files directly.** They are excluded from formatting and linting (see Makefile overrides for `format-check`, `format-fix`, `tidy-check`).

## Key Design Decisions

### ClickHouse-Compatible Schema (v2)
The extension follows the OpenTelemetry ClickHouse exporter schema with 7 separate tables and strongly-typed columns:

**Traces Table (`otel_traces` - 22 columns):**
- Direct column access: `trace_id`, `span_id`, `parent_span_id`, `trace_state`
- Computed fields: `duration` (calculated from start/end times)
- Extracted metadata: `service_name` (from resource attributes)
- Converted enums: `span_kind` (string), `status_code` (string)
- Nested data: `resource_attributes` (MAP), `attributes` (MAP), `events` (LIST), `links` (LIST)

**Logs Table (`otel_logs` - 15 columns):**
- Direct fields: `timestamp`, `observed_timestamp`, `body`, `severity_text`, `severity_number`
- Trace correlation: `trace_id`, `span_id`, `trace_flags`
- Extracted metadata: `service_name`
- Nested data: `resource_attributes` (MAP), `attributes` (MAP)

**Metrics Tables (5 types with 10-19 columns each):**
All metric tables share 9 base columns plus type-specific fields:
- Common base: `timestamp`, `service_name`, `metric_name`, `metric_description`, `metric_unit`, `resource_attributes`, `scope_name`, `scope_version`, `attributes`
- Type-specific fields:
  - **Gauge** (10 columns): `value` (double)
  - **Sum** (12 columns): `value`, `aggregation_temporality` (int), `is_monotonic` (bool)
  - **Histogram** (15 columns): `count`, `sum`, `bucket_counts` (list), `explicit_bounds` (list), `min`, `max`
  - **Exponential Histogram** (19 columns): `count`, `sum`, `scale`, `zero_count`, `positive_offset`, `positive_bucket_counts`, `negative_offset`, `negative_bucket_counts`, `min`, `max`
  - **Summary** (13 columns): `count`, `sum`, `quantile_values` (list), `quantile_quantiles` (list)

This design enables:
- Direct SQL access to fields without JSON extraction (`SELECT * FROM otel_metrics_gauge`)
- No NULL columns - each table has only the columns it needs
- Efficient filtering and aggregation on typed columns
- Compatibility with ClickHouse-based OTLP tools and queries
- Automatic metrics routing based on protobuf message type

### In-Memory Storage
Attached databases store data in ring buffers (memory-only). There is no persistent storage. Users must:
- Periodically `DELETE` old data
- Or `CREATE TABLE archive AS SELECT * FROM live.otel_traces` to persist

### Thread Safety
- Ring buffers use `std::shared_mutex` for concurrent reads, exclusive writes
- gRPC receiver runs on background thread, inserts are thread-safe
- Catalog operations are read-only after initialization

## Dependencies

Managed via VCPKG:
- **gRPC** - OTLP receiver implementation
- **Protobuf** - Wire format parsing
- **OpenSSL** - Required by gRPC

Python dependencies (via `uv`):
- `black` - Python formatting
- `clang-format` / `clang-tidy` - C++ formatting/linting
- `pre-commit` - Git hooks

## File Organization

```
src/
├── include/           # Public headers
├── generated/         # Protobuf/gRPC generated code (DO NOT EDIT)
├── duckspan_extension.cpp   # Extension entry point
├── otlp_storage_extension.cpp   # Storage extension implementation
├── otlp_catalog.cpp   # Virtual catalog
├── otlp_receiver.cpp  # gRPC server
├── ring_buffer.cpp    # Thread-safe buffer
├── read_otlp.cpp      # Table function
├── *_parser.cpp       # Format parsers
└── format_detector.cpp

test/
├── sql/               # SQLLogicTests (primary test format)
├── python/            # Integration tests (OTLP export)
└── data/              # Test data (OTLP JSON/protobuf files)
```

## Testing Notes

- **SQL tests** are the primary test format (SQLLogicTests in `test/sql/`)
- Tests run against DuckDB built with extension statically linked
- Integration test (`test_otlp_export.py`) verifies end-to-end OTLP gRPC flow
- Test data in `test/data/` includes both JSON and protobuf OTLP files

## Known Limitations

- No persistent storage - data is memory-only
- Ring buffers have fixed capacity, old data evicted when full
- No TTL or automatic cleanup
- gRPC receiver binds to single port (one ATTACH per port)
