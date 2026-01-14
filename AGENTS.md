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

**Note**: WASM builds support both JSON and Protobuf formats.

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
The extension ships as a **table-function extension** using a Rust backend (`otlp2records`) via FFI. It exposes:
- `read_otlp_traces` - 25 columns for trace spans
- `read_otlp_logs` - 15 columns for log records
- `read_otlp_metrics_gauge` - 16 columns for gauge metrics
- `read_otlp_metrics_sum` - 18 columns for sum/counter metrics
- `read_otlp_metrics_histogram` - 22 columns for standard histogram metrics
- `read_otlp_metrics_exp_histogram` - 27 columns for exponential histogram metrics

Column names use `snake_case` (e.g., `trace_id`, `span_name`, `service_name`).

### Core Components

- **Rust Backend (`external/otlp2records`)**: Rust library that parses OTLP JSON and protobuf, returns Arrow arrays via C Data Interface
- **FFI Bridge (`src/function/read_otlp.cpp`)**: Converts Arrow arrays from Rust to DuckDB DataChunks
- **Format Detection**: Automatic detection of JSON vs protobuf formats

### Data Flow

```
User: SELECT * FROM read_otlp_metrics_gauge('metrics.pb')
  ↓
Rust: otlp2records parses file
  ↓
Arrow C Data Interface
  ↓
DuckDB: Convert Arrow to DataChunks
```

### Generated Code

The `src/generated/` directory contains protobuf message stubs generated from OpenTelemetry `.proto` files (`*.pb.h`, `*.pb.cc`). They provide the message types consumed by `parsers/protobuf_parser.cpp`. **Do not edit these files directly.** They are excluded from formatting and linting.

## Key Design Decisions

### Schema Design
The table functions emit schemas inspired by the OpenTelemetry ClickHouse exporter, with all column names in `snake_case`:

- **Traces**: 25 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration
- **Logs**: 15 columns with severity, body, resource/scope maps, and trace correlation fields
- **Metrics (gauge)**: 16 columns with timestamp, service info, metric metadata, and value
- **Metrics (sum)**: 18 columns (gauge columns plus aggregation_temporality and is_monotonic)

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
└── wasm/              # WASM-specific build configuration

test/
├── sql/               # SQLLogicTests (primary test format)
└── data/              # Test data (OTLP JSON/protobuf files)

demo/
├── index.html         # Browser-based demo application
├── app.js             # DuckDB-WASM integration code
├── style.css          # Demo styling
├── otlp.duckdb_extension.wasm  # WASM build of extension
└── samples/           # Sample OTLP files (JSON and Protobuf)
```

## Testing Notes

- SQLLogicTests under `test/sql/` cover JSON parsing, protobuf parsing, option handling, and schema projections.
- All tests run against DuckDB with the extension statically linked (`make test`).
- Test data in `test/data/` includes representative OTLP JSON and protobuf fixtures used by the table functions.

## Known Limitations

- Live OTLP ingestion via gRPC has been removed; only file-based workloads are supported
- Summary metrics are not yet supported
- The union metrics function (`read_otlp_metrics`) is not yet implemented
