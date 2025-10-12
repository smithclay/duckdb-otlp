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
- Auto-creates three tables per attached database: `traces`, `metrics`, `logs`
- Backed by thread-safe ring buffers (in-memory FIFO storage)

**Ring Buffer (`ring_buffer.cpp/hpp`)**
- Thread-safe circular buffer with shared-read/exclusive-write semantics
- Stores rows as `(timestamp TIMESTAMP, resource JSON, data JSON)`
- FIFO eviction when capacity reached
- Used as backing storage for virtual tables

**gRPC Receiver (`otlp_receiver.cpp/hpp`)**
- Implements OpenTelemetry Collector Protocol gRPC endpoints
- Three service endpoints: `/v1/traces`, `/v1/metrics`, `/v1/logs`
- Converts incoming protobuf → JSON → inserts into ring buffers
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
  ↓ Creates OTLPStorageInfo with 3 ring buffers
  ↓ Starts gRPC receiver on background thread
  ↓
Returns OTLPCatalog with virtual tables
  ↓
User: SELECT * FROM live.traces
  ↓
OTLPScan reads from ring buffer → returns rows
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
```

### Generated Code

The `src/generated/` directory contains protobuf and gRPC stubs generated from OpenTelemetry `.proto` files:
- Message definitions: `*.pb.h`, `*.pb.cc`
- Service definitions: `*.grpc.pb.h`, `*.grpc.pb.cc`

**Do not edit these files directly.** They are excluded from formatting and linting (see Makefile overrides for `format-check`, `format-fix`, `tidy-check`).

## Key Design Decisions

### Unified Schema
All three signal types (traces, metrics, logs) use the same schema:
- `timestamp TIMESTAMP` - Event timestamp
- `resource JSON` - Service/host metadata
- `data JSON` - Signal-specific payload

This simplifies the implementation and allows flexible querying with DuckDB's JSON functions.

### In-Memory Storage
Attached databases store data in ring buffers (memory-only). There is no persistent storage. Users must:
- Periodically `DELETE` old data
- Or `CREATE TABLE archive AS SELECT * FROM live.traces` to persist

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
