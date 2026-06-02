# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

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

### Building the native server daemon
```bash
# Build/relink only the native duckdb-otlp-server target after CMake is configured
cmake --build build/release --target duckdb_otlp_server

# Build output:
# - ./build/release/extension/otlp/duckdb_otlp_server
```

The daemon is a static DuckDB executable that embeds the OTLP extension and opens/serves DuckDB directly. After the normal release build directory exists, incremental changes to `src/server/*` should relink quickly. A fresh daemon build still has to compile the static DuckDB/extension dependency graph.

### Docker server image
```bash
# Build the foreground-service image
docker buildx build --platform linux/arm64 --load \
  -t duckdb-otlp:daemon \
  -f docker/duckdb-otlp-server/Dockerfile .

# Short local DuckLake e2e benchmark
python3 scripts/benchmark_catalog_ingest.py \
  --scenario local-ducklake \
  --image duckdb-otlp:daemon \
  --platform linux/arm64 \
  --duration 3 \
  --rate 500 \
  --batch-size 100 \
  --startup-timeout 120 \
  --output-dir output/catalog-benchmarks-daemon
```

The Docker image uses `/usr/local/bin/duckdb-otlp-server` as the foreground `ENTRYPOINT`. It no longer relies on a shell/FIFO controller for normal startup/shutdown. Benchmark/admin SQL goes through Quack when `DUCKDB_QUACK_ENABLED=1`.

CI image publishing is split into two phases in `.github/workflows/MainDistributionPipeline.yml`: `daemon-linux` exports Linux `amd64`/`arm64` daemon binaries with the `daemon-export` Dockerfile target, then `docker-image` packages those artifacts with `docker/duckdb-otlp-server/Dockerfile.runtime`. Do not make the runtime Dockerfile compile DuckDB; it should only copy `docker-bin/$TARGETARCH/duckdb-otlp-server` into the image.

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
The extension ships table functions, live ingest functions, and a native daemon. The file-reading table functions use a Rust backend (`otlp2records`) via FFI. They expose:
- `read_otlp_traces` - 25 columns for trace spans
- `read_otlp_logs` - 15 columns for log records
- `read_otlp_metrics_gauge` - 16 columns for gauge metrics
- `read_otlp_metrics_sum` - 18 columns for sum/counter metrics
- `read_otlp_metrics_histogram` - 22 columns for standard histogram metrics
- `read_otlp_metrics_exp_histogram` - 27 columns for exponential histogram metrics

Column names use `snake_case` (e.g., `trace_id`, `span_name`, `service_name`).

### Core Components

- **Rust Backend (`external/otlp2records`)**: Rust library that parses OTLP JSON, NDJSON, and protobuf and returns Arrow arrays via the C Data Interface
- **FFI Bridge (`src/function/read_otlp.cpp`)**: Table function implementations that drive the Rust backend over FFI
- **Arrow conversion (`src/otlp_arrow.cpp`)**: Converts the Arrow arrays returned by Rust into DuckDB DataChunks
- **Live ingest server (`src/otlp_server.cpp`, `src/otlp_server_http.cpp`)**: Native OTLP/HTTP ingest implementation for `/v1/logs`, `/v1/traces`, and `/v1/metrics`
- **Buffered storage (`src/otlp_storage.cpp`)**: Per-signal buffering and serialized background seal/group-commit path
- **Start/stop SQL functions (`src/otlp_start_stop.cpp`)**: `otlp_serve`, `otlp_stop`, `otlp_flush`, and `otlp_server_list`
- **Native daemon (`src/server/`)**: `duckdb-otlp-server` binary that embeds DuckDB, loads the static OTLP extension, executes mode setup, starts `otlp_serve` and optional `quack_serve`, handles SIGTERM/SIGINT, then calls `quack_stop`/`otlp_stop`
- **Format Detection**: Automatic detection of JSON/NDJSON vs protobuf formats (handled by the Rust backend)

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

## Key Design Decisions

### Schema Design
The table functions emit schemas inspired by the OpenTelemetry ClickHouse exporter, with all column names in `snake_case`:

- **Traces**: 25 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration
- **Logs**: 15 columns with severity, body, resource/scope maps, and trace correlation fields
- **Metrics (gauge)**: 16 columns with timestamp, service info, metric metadata, and value
- **Metrics (sum)**: 18 columns (gauge columns plus aggregation_temporality and is_monotonic)

## Dependencies

Managed via VCPKG (see `vcpkg.json`):
- **zlib** - gzip/deflate decompression of incoming OTLP request bodies on the ingest server

OTLP protobuf/JSON wire parsing is handled entirely in the Rust backend (`prost`), not via a C++/VCPKG protobuf dependency.

Python dependencies (via `uv`):
- `black` - Python formatting
- `clang-format` / `clang-tidy` - C++ formatting/linting
- `pre-commit` - Git hooks

## File Organization

```
src/
├── server/
│   ├── main.cpp               # Native `duckdb-otlp-server` process entry point
│   └── server_config.cpp      # Environment/mode config and generated setup SQL
├── storage/
│   └── otlp_extension.cpp     # Extension entry point + registration
├── function/
│   └── read_otlp.cpp          # FFI bridge / `read_otlp_*` table functions
├── otlp_arrow.cpp             # Arrow → DuckDB DataChunk conversion
├── otlp_server.cpp            # HTTP OTLP ingest server
├── otlp_storage.cpp           # Buffered group-commit / seal storage
├── otlp_start_stop.cpp        # `otlp_serve` / `otlp_stop` / `otlp_flush` functions
├── otlp_uri.cpp               # URI parsing/validation
└── include/                   # Public headers (otlp_*.hpp)

test/
├── sql/               # SQLLogicTests (primary test format)
└── data/              # Test data (OTLP JSON/protobuf files)

site/
├── src/content/docs/  # Astro/Starlight documentation pages
└── public/wasm-demo/  # Browser demo, WASM extension, and sample OTLP files

docker/
└── duckdb-otlp-server/ # Native daemon Docker image assets

scripts/
└── benchmark_catalog_ingest.py # Disposable Docker e2e catalog ingest benchmark
```

## Documentation

Follow the Diátaxis documentation framework and keep docs lean:

- **Tutorials**: `README.md`, `site/src/content/docs/get-started.md`, and focused quickstarts such as `site/src/content/docs/quickstart/serve.md`.
- **How-to guides**: task-oriented docs under `site/src/content/docs/guides/` (for example traces, logs, metrics, Parquet, lakehouse ingest, errors). Do not add a separate "cookbook" section.
- **Reference**: exact API, schema, server contract, and operational limits under `site/src/content/docs/reference/`.
- **Explanation**: architecture and design context in `site/src/content/docs/architecture.md`.

Prefer one canonical page per topic and link to it instead of duplicating examples. Since this is an early-stage project, do not add backwards-compatibility redirect pages or migration stubs unless explicitly requested.

## Testing Notes

- SQLLogicTests under `test/sql/` cover JSON parsing, protobuf parsing, option handling, and schema projections.
- All tests run against DuckDB with the extension statically linked (`make test`).
- Test data in `test/data/` includes representative OTLP JSON and protobuf fixtures used by the table functions.
- The Docker benchmark harness starts the daemon image, sends OTLP/HTTP log batches, flushes via Quack, and queries row counts/server metrics over Quack. It intentionally avoids the old FIFO controller path.

## Known Limitations

- Live OTLP ingestion is supported over **HTTP** (`otlp_serve` / `otlp_stop` / `otlp_server_list` / `otlp_flush`), not gRPC. The HTTP server (`src/otlp_server.cpp`) accepts OTLP/JSON, OTLP/NDJSON, and OTLP/protobuf POSTs to `/v1/logs`, `/v1/traces`, `/v1/metrics`.
  - **Catalog targeting**: `otlp_serve(uri, catalog := '<attached_db>')` streams into an attached catalog. Set it to a DuckLake catalog to land data as Parquet in a lakehouse; empty = the default (in-memory/file) catalog.
  - **Buffered group-commit ("seal")**: ingest is buffered in memory (per-signal `ColumnDataCollection` with per-signal locking) and a single background sealer thread group-commits on internal size/age triggers or an explicit `otlp_flush`. One seal = one transaction (for DuckLake: one Parquet file per signal + one snapshot), so a single serialized writer avoids DuckLake's optimistic-concurrency conflicts and tiny-file churn. The configurable httplib worker pool only parses/converts/buffers concurrently.
  - **Durability**: ingest is buffered in memory and durability is the seal. A POST returns **`202 Accepted`** (`{"status":"buffered",...}`) once rows are parsed and buffered in memory, but not yet durable; they commit at the next seal. **`otlp_stop` and `otlp_flush` seal remaining rows before returning; a plain database/connection close does NOT** — buffered-but-un-sealed rows can be lost, so callers must `otlp_stop`/`otlp_flush` before closing the database. Backpressure: `max_buffered_bytes` (default 512 MiB) bounds cumulative *admitted request-body bytes*, not decoded buffer heap — each request reserves `max(body_size, 1024)` input bytes against this budget (the decoded columnar size differs from the encoded/compressed input size). A request whose admission would exceed the budget is rejected with **`503`**.
  - **`otlp_flush(uri)`** forces a synchronous seal. `otlp_server_list` exposes buffer/seal metrics (`buffered_rows`, `last_seal_age_ms`, `seals_total`, `seal_failures_total`, `seal_last_error`, `catalog_name`). Verify the ingest/seal path with `test/manual/otlp_serve_concurrency.py` (set `OTLP_DUCKLAKE_DIR` for the DuckLake path).
  - **Daemon SQL access**: the daemon does not expose an attached DuckDB shell. Enable Quack (`DUCKDB_QUACK_ENABLED=1` and `DUCKDB_QUACK_TOKEN=...`) when external SQL/admin access is required. Quack grants full SQL read/write access to the daemon's DuckDB connection, so treat it as an administrative endpoint.
  - Not available on the wasm build.
- Summary metrics are not yet supported
- The union metrics function (`read_otlp_metrics`) is not yet implemented
