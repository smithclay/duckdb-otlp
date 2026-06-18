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

There is a single `docker/duckdb-otlp-server/Dockerfile` with named stages: `builder` (compiles the static daemon), `daemon-export` (exports just the binary), `deps` (throwaway: primes the extension cache and stages `libz`), `runtime-base` (the runnable image minus the binary), and two leaf stages — `runtime-source` (default; copies the binary from `builder`, used by `make docker-image-local`) and `runtime-prebuilt` (copies `docker-bin/$TARGETARCH/duckdb-otlp-server`, used by CI). `runtime-prebuilt` does not depend on `builder`, so packaging never recompiles DuckDB — keep it that way.

The final image is `gcr.io/distroless/cc-debian12:nonroot` (~48MB base): glibc + NSS (DNS) + libstdc++ + openssl + CA certs, no shell, no package manager, no bundled DuckDB CLI, runs as nonroot (uid 65532). The daemon and all six runtime extensions link only glibc/libstdc++/libgcc/libz; distroless/cc ships everything except `libz`, so the `deps` stage stages `libz.so.1` into `/opt/duckdb-otlp/lib` (`LD_LIBRARY_PATH`). Because there is no shell/curl, the container `HEALTHCHECK` is the daemon's own `duckdb-otlp-server healthcheck` subcommand. It probes the daemon's actual transport, derived from the listen URI: `otlp:` (HTTP) → GET `/readyz`; `otap:` (gRPC/HTTP2, no `/readyz`) → a TCP-connect probe. Quack is also probed when enabled. All probes target the configured bind host (loopback for a wildcard bind).

CI in `.github/workflows/MainDistributionPipeline.yml`: `daemon-compile` is a cheap amd64-only `daemon-export` build that runs the daemon config tests and gates PRs and feature-branch pushes (no publish). The publish path runs only on `main`/tags/`workflow_dispatch`: `daemon-linux` exports `amd64`/`arm64` binaries (`daemon-export` target), `docker-smoke` runs the benchmark e2e on the packaged amd64 image, and `docker-image` publishes with `--target runtime-prebuilt`.

Extension offline-cache coupling: the `deps` stage pre-`INSTALL`s ducklake/iceberg/httpfs/aws/postgres/quack with a throwaway DuckDB CLI into `HOME=/duckdb-home` (copied into the final image, owned by the nonroot user; the CLI itself is not copied). The daemon reuses that cache only because it runs with the same `HOME` and is pinned to the same `DUCKDB_VERSION`. If those diverge, the daemon's startup `INSTALL`/`LOAD` re-downloads (or fails when offline). The `otlp` extension is statically embedded in the daemon and is intentionally not installed. Bind-mounted `/data` must be writable by uid 65532; named/anonymous volumes inherit the image's nonroot ownership automatically.

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

**Note**: WASM builds support OTLP JSON and Protobuf, plus OTAP reads (`read_otap_*`) limited to uncompressed/LZ4 — the `otap-zstd` feature is native-only, so Zstandard OTAP is not decodable on WASM.

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
- `read_otlp_traces` - 24 columns for trace spans
- `read_otlp_logs` - 18 columns for log records
- `read_otlp_metrics_gauge` - 17 columns for gauge metrics
- `read_otlp_metrics_sum` - 19 columns for sum/counter metrics
- `read_otlp_metrics_histogram` - 22 columns for standard histogram metrics
- `read_otlp_metrics_exp_histogram` - 27 columns for exponential histogram metrics

Column names use `snake_case` (e.g., `trace_id`, `span_name`, `service_name`).

A parallel set of `read_otap_*` functions (same six signals, identical output schemas) decodes the OpenTelemetry Arrow Protocol (canonical `BatchArrowRecords`) instead of OTLP protobuf/JSON. OTAP and OTLP are deliberately separate functions, not a `format` flag, because they are different protocols. Each file is decoded as one self-contained message via the crate's stateful OTAP decoder FFI (`otlp_otap_decoder_new`/`_decode_logs`/`_decode_traces`/`_decode_metrics`/`_decoder_free`). Native builds enable the `otap-zstd` cargo feature so Zstandard streams (the producer default) decode; WASM builds do not (uncompressed/LZ4 only).

### Core Components

- **Rust Backend (`external/otlp2records`)**: Rust library that parses OTLP JSON, NDJSON, and protobuf — and decodes OTAP (`BatchArrowRecords`) via a stateful decoder — returning Arrow arrays via the C Data Interface
- **FFI Bridge (`src/function/read_otlp.cpp`)**: Table function implementations that drive the Rust backend over FFI
- **Arrow conversion (`src/otlp_arrow.cpp`)**: Converts the Arrow arrays returned by Rust into DuckDB DataChunks
- **Live ingest server (`src/otlp_server.cpp`, `src/otlp_server_http.cpp`)**: Native OTLP/HTTP ingest implementation for `/v1/logs`, `/v1/traces`, and `/v1/metrics`
- **gRPC transport bridge (`src/otlp_server_grpc.cpp`)**: Bridges the embedded tonic gRPC server (in `otlp2records`) into the same buffering/seal core as the HTTP path via a per-batch C callback. Two disjoint gRPC service families, selected via the `service_flags` FFI arg: OTLP/gRPC unary `Export` for `otlp_serve(transport := 'grpc')`, OTAP/Arrow streaming for `otap_serve`
- **Buffered storage (`src/otlp_storage.cpp`)**: Per-signal buffering and serialized background seal/group-commit path
- **Start/stop SQL functions (`src/otlp_start_stop.cpp`)**: `otlp_serve` (HTTP), `otap_serve` (gRPC), `otlp_stop`, `otlp_flush`, and `otlp_server_list`
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

- **Traces**: 24 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration
- **Logs**: 18 columns with severity, body, resource/scope maps, and trace correlation fields
- **Metrics (gauge)**: 17 columns with timestamp, service info, metric metadata, and value
- **Metrics (sum)**: 19 columns (gauge columns plus aggregation_temporality and is_monotonic)

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
├── otlp_server.cpp            # Shared ingest core (admission, buffering, seal)
├── otlp_server_http.cpp       # HTTP transport (cpp-httplib)
├── otlp_server_grpc.cpp       # gRPC transport bridge (tonic, via otlp2records FFI)
├── otlp_storage.cpp           # Buffered group-commit / seal storage
├── otlp_start_stop.cpp        # `otlp_serve` / `otap_serve` / `otlp_stop` / `otlp_flush`
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
- The Docker benchmark harness (`scripts/benchmark_catalog_ingest.py`) starts the daemon image, sends OTLP/HTTP log batches, flushes via Quack, and queries row counts/server metrics over Quack. Because the image is distroless (no in-container shell/`duckdb`), it publishes the Quack port and runs Quack queries from a **host `duckdb` CLI** — so the harness now requires `duckdb` on `PATH` (the `docker-smoke` CI job installs the pinned v1.5.3 CLI). It intentionally avoids the old FIFO controller path.

## Known Limitations

- Live ingestion is exposed by two scheme-bound serve functions (no URI mixing — each rejects the other's scheme): **`otlp_serve`** on the `otlp:` scheme (default port 4318) and **`otap_serve`** on the `otap:` scheme (default port 4317). `otlp_stop` / `otlp_flush` / `otlp_server_list` are transport-agnostic — they dispatch by the scheme-aware canonical listen URI, so `otap:host:4317` and `otlp:host:4318` are distinct servers. The transport is **not** encoded in the scheme: `otlp_serve` picks it via the `transport := 'http'|'grpc'` named param (default `http`); `otap_serve` is always gRPC.
  - **OTLP/HTTP** (`otlp_serve` default, `src/otlp_server.cpp`): OTLP/JSON, OTLP/NDJSON, and OTLP/protobuf POSTs to `/v1/logs`, `/v1/traces`, `/v1/metrics`.
  - **OTLP/gRPC** (`otlp_serve(transport := 'grpc')`, `src/otlp_server_grpc.cpp`): standard **OTLP/gRPC** unary `Export` (`{Logs,Trace,Metrics}Service`) for all three signals.
  - **OTAP/Arrow** (`otap_serve`, `src/otlp_server_grpc.cpp` + the embedded tonic server in `otlp2records`): canonical **OTAP/Arrow** bidirectional streaming (`Arrow{Logs,Traces,Metrics}Service`, `stream BatchArrowRecords` → `stream BatchStatus`), all six signals. OTAP streams keep one stateful decoder per stream (cross-message Arrow dictionary reuse). Metric shapes are buffered per-shape, so a backpressure nack partway through a metrics message can leave earlier shapes buffered.
  - The two gRPC service sets are **disjoint** — the host selects which to register per listener via the `service_flags` arg to `otlp_grpc_server_start` (`OTLP_GRPC_SERVICE_OTLP_UNARY` vs `OTLP_GRPC_SERVICE_OTAP_ARROW`), so calling the wrong family returns `UNIMPLEMENTED`. The gRPC stack (tokio + tonic) is statically linked into the otlp2records archive — no new shared libraries — and is native-only (absent from the WASM build, like the HTTP server).
  - **Catalog targeting**: `otlp_serve(uri, catalog := '<attached_db>')` streams into an attached catalog. Set it to a DuckLake catalog to land data as Parquet in a lakehouse; empty = the default (in-memory/file) catalog.
  - **Buffered group-commit ("seal")**: ingest is buffered in memory (per-signal `ColumnDataCollection` with per-signal locking) and a single background sealer thread group-commits on internal size/age triggers or an explicit `otlp_flush`. One seal = one transaction (for DuckLake: one Parquet file per signal + one snapshot), so a single serialized writer avoids DuckLake's optimistic-concurrency conflicts and tiny-file churn. The configurable httplib worker pool only parses/converts/buffers concurrently.
  - **Durability**: ingest is buffered in memory and durability is the seal. A POST returns **`202 Accepted`** (`{"status":"buffered",...}`) once rows are parsed and buffered in memory, but not yet durable; they commit at the next seal. **`otlp_stop` and `otlp_flush` seal remaining rows before returning; a plain database/connection close does NOT** — buffered-but-un-sealed rows can be lost, so callers must `otlp_stop`/`otlp_flush` before closing the database. Backpressure: `max_buffered_bytes` (default 512 MiB) bounds cumulative *admitted request-body bytes*, not decoded buffer heap — each request reserves `max(body_size, 1024)` input bytes against this budget (the decoded columnar size differs from the encoded/compressed input size). A request whose admission would exceed the budget is rejected with **`503`**.
  - **`otlp_flush(uri)`** forces a synchronous seal. `otlp_server_list` exposes buffer/seal metrics (`buffered_rows`, `last_seal_age_ms`, `seals_total`, `seal_failures_total`, `seal_last_error`, `catalog_name`). Verify the ingest/seal path with `test/manual/otlp_serve_concurrency.py` (set `OTLP_DUCKLAKE_DIR` for the DuckLake path).
  - **Daemon SQL access**: the daemon does not expose an attached DuckDB shell. Enable Quack (`DUCKDB_QUACK_ENABLED=1` and `DUCKDB_QUACK_TOKEN=...`) when external SQL/admin access is required. Quack grants full SQL read/write access to the daemon's DuckDB connection, so treat it as an administrative endpoint.
  - Not available on the wasm build.
- Summary metrics are not yet supported
- The union metrics function (`read_otlp_metrics`) is not yet implemented
- **OTAP reads (`read_otap_*`)** decode canonical `BatchArrowRecords` files into the same flattened schemas as `read_otlp_*`, with these constraints:
  - **One self-contained message per file.** Each file is decoded with one stateful decoder as a single `BatchArrowRecords`. A file holding several concatenated messages, or a "reuse" message that omits its schema/dictionaries and depends on a prior message in the same decoder session, is not supported and surfaces an `OTAP decode error`.
  - **Envelopes are per-signal.** Canonical OTAP carries one signal family (logs *or* traces *or* metrics) per message, so use the reader that matches the file. A metrics envelope can hold several metric *shapes* at once — one file feeds `read_otap_metrics_gauge`/`_sum`/`_histogram`/`_exp_histogram`, each of which extracts its shape (the rest are released), exactly like `read_otlp_metrics_*`. Summary data points are counted as skipped.
  - **Wrong/foreign payloads are a hard error, never silent.** Calling a reader on a file of a different signal (or an envelope mixing incompatible payloads) throws `OTAP decode error … Parse failed` rather than returning partial or mis-typed rows.
  - **Compression:** native builds decode uncompressed, LZ4, and Zstandard (the producer default, via `otap-zstd`); WASM decodes uncompressed/LZ4 only.
  - **File reads only** — OTAP is not accepted by the live HTTP ingest server.
