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
# - ./build/release/extension/otlp/otlp.duckdb_extension - Loadable extension
```

### Building for WebAssembly
```bash
# Build WASM with exception handling support (recommended for demo)
make wasm_eh

# Build MVP WASM (minimal features)
make wasm_mvp

# Build WASM with threads support
make wasm_threads

# Build output:
# - ./build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm
```

**Note**: WASM builds currently support JSON format only. Protobuf parsing requires native builds.

### Testing
```bash
# Run SQL logic tests
make test

# Run debug tests
make test_debug

# Run specific SQLLogicTest file
build/release/test/unittest "test/sql/read_otlp_protobuf.test"
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
Duckspan now ships purely as a **table-function extension**. It exposes strongly-typed readers for OTLP telemetry files (`read_otlp_traces`, `read_otlp_logs`, `read_otlp_metrics`), with optional helpers for scan statistics and option discovery.

### Core Components

- **Format Detector (`format_detector.cpp/hpp`)**: sniffs the first bytes of a file/stream to decide between JSON and protobuf payloads.
- **JSON Parser (`json_parser.cpp/hpp`)**: streaming newline-delimited JSON parser that emits typed rows.
- **Protobuf Parser (`parsers/protobuf_parser.cpp/hpp`)**: wraps generated OTLP protobuf stubs and uses shared row-builder helpers to produce DuckDB vectors.
- **Row Builders (`receiver/row_builders*.cpp`)**: convert OTLP message structures into vectors that match the ClickHouse-compatible schemas.
- **Schema Definitions (`src/schema/*.hpp`)**: centralized column layouts for traces, logs, and metrics (including the union schema used by `read_otlp_metrics`).
- **Metrics helper table functions**: `read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}` call the union reader internally and project typed schemas for each metric shape.

### Data Flow

```
User: SELECT * FROM read_otlp_metrics('metrics.pb')
  ↓
FormatDetector::DetectFormat()
  ↓
Choose JSON parser or protobuf parser
  ↓
Parsers populate typed row builders
  ↓
DuckDB emits DataChunks with strongly-typed columns
```

### Generated Code

The `src/generated/` directory contains protobuf message stubs generated from OpenTelemetry `.proto` files (`*.pb.h`, `*.pb.cc`). They provide the message types consumed by `parsers/protobuf_parser.cpp`. **Do not edit these files directly.** They are excluded from formatting and linting.

## Key Design Decisions

### ClickHouse-Compatible Schema
The table functions continue to emit the OpenTelemetry ClickHouse exporter schema:

- **Traces**: 22 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration.
- **Logs**: 15 columns with severity, body, resource/scope maps, and trace correlation fields.
- **Metrics**: `read_otlp_metrics` returns a union schema (27 columns) containing a `MetricType` discriminator plus all type-specific payloads (gauge, sum, histogram, exponential histogram, summary). Helpers in `schema/otlp_metrics_schemas.hpp` define the typed projections.

### Union Table Strategy
Instead of creating separate DuckDB tables, metric data stays in a single union-shaped table function result. Users can split this into typed archive tables with simple `CREATE TABLE AS SELECT ... WHERE MetricType = 'gauge'` patterns (see `test/sql/schema_bridge.test`).

## Dependencies

Managed via VCPKG:
- **Protobuf** - Wire format parsing for binary OTLP files (optional for JSON-only builds)

Python dependencies (via `uv`):
- `black` - Python formatting
- `clang-format` / `clang-tidy` - C++ formatting/linting
- `pre-commit` - Git hooks

## File Organization

```
src/
├── include/           # Public headers (forwarding to implementation dirs)
├── storage/           # Extension entry point registration
├── function/          # Table function implementations (`read_otlp_*`)
├── parsers/           # JSON/protobuf parsers and format detector
├── receiver/          # Shared row builders and OTLP helpers
├── schema/            # Column layout helpers
├── generated/         # Protobuf message stubs (DO NOT EDIT)
└── wasm/              # Stubs for JSON-only builds

test/
├── sql/               # SQLLogicTests (primary test format)
└── data/              # Test data (OTLP JSON/protobuf files)

demo/
├── index.html         # Browser-based demo application
├── app.js             # DuckDB-WASM integration code
├── style.css          # Demo styling
├── otlp.duckdb_extension.wasm  # WASM build of extension
└── samples/           # Sample OTLP JSONL files for testing
```

## Testing Notes

- SQLLogicTests under `test/sql/` cover JSON parsing, protobuf parsing, option handling, and schema projections.
- All tests run against DuckDB with the extension statically linked (`make test`).
- Test data in `test/data/` includes representative OTLP JSON and protobuf fixtures used by the table functions.

## Known Limitations

- Live OTLP ingestion via gRPC has been removed; only file-based workloads are supported.
- **WASM builds support JSON format only**. Protobuf parsing is only available in native builds.
- Protobuf parsing requires linking against the protobuf runtime (available in native builds).
- The metrics table function emits a union schema; consumers must project out the desired metric shapes manually when creating persistent tables.
