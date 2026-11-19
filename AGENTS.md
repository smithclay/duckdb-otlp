# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Build System

- **Always use `GEN=ninja`** when running make commands for caching and performance: `GEN=ninja make`
- **VCPKG is required** for dependency management. Set environment variable before building:
  ```bash
  export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
  ```
- Use `uv` for Python tooling (formatting, testing, dependency management)
- **DuckDB is pinned to `v2.0.0`.** The `duckdb` submodule is checked out at the tag, the
  extension CI (and `pages.yml`) passes `duckdb_version: v2.0.0`, and the daemon/Docker version
  (`DAEMON_DUCKDB_VERSION`, the Dockerfile's `DUCKDB_VERSION` ARG, the Makefile's WASM
  `VERSION_FIELD`) is `v2.0.0`, so runtime `INSTALL`s resolve to published binaries. Move them
  together. `extension-ci-tools` and the reusable-workflow refs stay on `v2.0-cyanoptera`, the
  community build's own. One gap remains: neither the community `gcs` extension nor the patched
  build (`smithclay/duckdb-gcs`) has a 2.0 build, so the image skips both (see the offline-cache
  note below) and `gcp-ducklake` fails at startup. `description.yml` carries `variegata:` (the
  last v1.5.6-compatible commit) for the v1.5 line; `ref_next` no longer triggers community builds.
- **Statically linked extensions must be rooted explicitly (DuckDB 2.0).** A linked extension
  registers itself only when the program calls `link_extension_libraries`, and that function reads
  the link set DuckDB resolves *after* its extension loop, which is where this `CMakeLists.txt` runs.
  An executable that embeds DuckDB (the daemon, the C++ harnesses) therefore calls
  `otlp_link_duckdb_extensions(<target>)`, which defers the call to the end of configure. A direct
  call links nothing and still builds; the first symptom is a runtime
  `Copy Function "parquet" is not in the catalog`. Separately, loading an extension
  config no longer links it into the shell/`unittest`: `extension_config.cmake` asks for that with
  `duckdb_extension_statically_link(otlp)`.
- **Windows (`windows_amd64`, MSVC) builds the extension only.** `CMakeLists.txt` gates the
  native daemon (`duckdb_otlp_server`) and the C++ seal harness (`otlp_seal_harness`)
  on `NOT WIN32`, because `make release` builds the default ALL target
  and the daemon only ships as the Linux container image. Windows-specific build details:
  `src/otlp_server.cpp` selects Winsock over the POSIX socket headers under `#ifdef _WIN32`,
  the extension targets get `/bigobj` (cpp-httplib is a large header), and the Rust archive is
  built with `RUSTFLAGS=-C target-feature=+crt-static` so it matches DuckDB's static CRT.
  `windows_amd64_mingw` remains excluded (untested `x86_64-pc-windows-gnu` Rust toolchain).

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
# - ./build/release/extension/otlp/duckdb-otlp  (CMake target: duckdb_otlp_server)
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

There is a single `docker/duckdb-otlp-server/Dockerfile` with named stages: `builder` (compiles the static daemon), `daemon-export` (exports just the binary), `deps` (throwaway: primes the extension cache), `zlib` (throwaway: stages Wolfi's `libz`), `runtime-base` (the runnable image minus the binary), and two leaf stages — `runtime-source` (default; copies the binary from `builder`, used by `make docker-image-local`) and `runtime-prebuilt` (copies `docker-bin/$TARGETARCH/duckdb-otlp-server`, used by CI). `runtime-prebuilt` does not depend on `builder`, so packaging never recompiles DuckDB — keep it that way.

The final image is `cgr.dev/chainguard/glibc-dynamic` (Wolfi), pinned by digest: glibc + libstdc++ + libgcc + CA certs, no shell, no package manager, no bundled DuckDB CLI, runs as nonroot (uid 65532). The daemon and the runtime extensions link only glibc/libstdc++/libgcc/libz — no `libssl` (the extensions carry their TLS statically) — so the base ships everything except `libz`, which the throwaway `zlib` stage takes from Wolfi (`apk add zlib`) and copies to `/usr/lib`. It replaced `gcr.io/distroless/cc-debian12` because that base's scanner reports (~49 CVEs, a third in Debian's `libssl3`) were all in libraries the image either never loads or that Debian patches slowly; glibc-dynamic scans clean. Do not add packages to the runtime stage to satisfy a scanner — check `readelf -d` NEEDED first. Because there is no shell/curl, the container `HEALTHCHECK` is the daemon's own `duckdb-otlp doctor` subcommand (`healthcheck` remains an accepted spelling, so existing probes keep working). It reports one line per check and runs them all rather than stopping at the first failure, and Docker keeps that output in the container's health log. It probes every configured listener: HTTP → GET `/readyz`; OTLP/gRPC and OTAP/Arrow (HTTP2, no `/readyz`) → a TCP-connect probe; Quack → a TCP-connect probe too, because it serves TLS on a non-loopback bind and the daemon carries no TLS client. `DUCKDB_OTLP_TRANSPORTS=http|grpc|http,grpc` selects standard OTLP listeners; `OTEL_HTTP_ADDR` and `OTEL_GRPC_ADDR` select their separate bind addresses. The default remains HTTP. An existing single `otap:` listen URI still selects OTAP/Arrow. Quack is also probed when enabled. All probes target the configured bind host (loopback for a wildcard bind).

CI in `.github/workflows/MainDistributionPipeline.yml`: `daemon-compile` is a cheap amd64-only `daemon-export` build that runs daemon configuration and real HTTP/gRPC transport tests and gates PRs and feature-branch pushes (no publish). The publish path runs only on `main`/tags/`workflow_dispatch`: `daemon-linux` exports `amd64`/`arm64` binaries (`daemon-export` target), `daemon-macos` builds `darwin-arm64`/`darwin-amd64` natively, `docker-smoke` runs the benchmark e2e on the packaged amd64 image, and `docker-image` publishes with `--target runtime-prebuilt`.

`daemon-macos` cannot reuse the Docker path (a Linux container cannot produce a Darwin binary), so it runs `make server-release` on the arm64 `macos-15` runner for both architectures, with `OSX_BUILD_ARCH` driving `CMAKE_OSX_ARCHITECTURES` and — through `cmake/FindOtlp2Records.cmake` — the Rust target. That is the same cross-compile `extension-ci-tools` uses for `osx_amd64`. It sets no vcpkg toolchain: `vcpkg.json` declares zlib for Windows only, so on macOS `find_package(ZLIB)` resolves to the SDK's libz. It pins `OVERRIDE_GIT_DESCRIBE` to the workflow's `DAEMON_DUCKDB_VERSION` for the same reason the Dockerfile does — an unpinned `git describe` version has no published extension build for the daemon's runtime `INSTALL` to fetch — so keep that variable in sync with the Dockerfile's `DUCKDB_VERSION` ARG.

On a `v*` tag, `daemon-release` attaches those four binaries to the GitHub release as `duckdb-otlp-<tag>-<platform>.tar.gz` plus `SHA256SUMS`. It recompiles nothing (it downloads the artifacts the build jobs uploaded) and gates on `docker-smoke`, so a binary that failed the e2e never reaches a release. The Darwin artifacts are deliberately named `duckdb-otlp-darwin-*` rather than `duckdb-otlp-server-*`: the latter is the glob `docker-image` downloads, which must only match the Linux binaries. `daemon-release` creates the release when the tag was pushed without one, which in turn triggers `pages.yml` to attach the extension repository.

A tag gets one pipeline run, so a transient failure there loses the release outright — v0.8.0 sat tagged but unreleased after a GitHub "Internal server error" kept the extension build matrices from ever being created. `retry-release.yml` re-runs a failed `v*` tag pipeline once (`workflow_run`, first attempt only, all jobs because a run-level error leaves no failed jobs to select). A failure that survives the retry is real. Before tagging, check that the release commit's `main` run is green, and after tagging, confirm a release actually appeared (`gh release view <tag>`).

Extension offline-cache coupling: the `deps` stage runs `INSTALL` for ducklake/iceberg/avro/httpfs/aws/postgres/quack (and community gcs, best effort: skipped with a warning while it has no build for the pinned version) in a throwaway DuckDB CLI with `HOME=/duckdb-home`, and that directory is copied into the final image, owned by the nonroot user. avro is primed because iceberg autoloads it. The daemon reuses that cache only because it runs with the same `HOME` and is pinned to the same `DUCKDB_VERSION`. If those diverge, the daemon's startup `INSTALL`/`LOAD` re-downloads (or fails when offline). The `otlp` extension is statically embedded in the daemon and is intentionally not installed. Bind-mounted `/data` must be writable by uid 65532; named/anonymous volumes inherit the image's nonroot ownership automatically.

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
- **Arrow import (`src/otlp_arrow.cpp`)**: A thin wrapper over DuckDB's stock Arrow importer (`ArrowTableFunction::ArrowToDuckDB`). Every otlp2records call requests `OTLP_ARROW_PROFILE_DUCKDB` (the `*_with_profile` FFI entry points), which emits hex `Utf8` ids, an `Int64` nanosecond duration and signed counters, so the stock mapping already yields the extension's column types and there is no hand-written converter. The one remaining narrowing is the ingest server's: its buffers and destination tables hold `TIMESTAMP` (µs, truncated), where the importer yields `TIMESTAMP_NS` (`OtlpServer::BufferColumnType`). The importer needs a `ClientContext`, which it consults only for Arrow extension types; `OtlpArrowTable::ForSignal` rejects any such column, which is what lets the server's ingest threads share one context (`arrow_con`)
- **Live ingest server (`src/otlp_server.cpp`)**: `OtlpServer` is one pipeline (buffers, admission cap, single sealer/writer, catalog maintenance) fed by N `OtlpListener`s. A listener is a bound socket plus a transport and owns no buffers or counters
- **HTTP listener (`src/otlp_server_http.cpp`)**: `OtlpHttpListener`, native OTLP/HTTP ingest for `/v1/logs`, `/v1/traces`, and `/v1/metrics`
- **gRPC listener (`src/otlp_server_grpc.cpp`)**: `OtlpGrpcListener` bridges the embedded tonic gRPC server (in `otlp2records`) into the same server as the HTTP path via a per-batch C callback. Two disjoint gRPC service families, selected via the `service_flags` FFI arg: OTLP/gRPC unary `Export` for `otlp_serve(transport := 'grpc')`, OTAP/Arrow streaming for `otap_serve`
- **Buffered storage (`src/otlp_storage.cpp`)**: Per-signal buffering and serialized background seal/group-commit path
- **Start/stop SQL functions (`src/otlp_start_stop.cpp`)**: `otlp_serve` (HTTP), `otap_serve` (gRPC), `otlp_stop`, `otlp_flush`, and `otlp_server_list`
- **Native CLI / daemon (`src/server/`)**: the `duckdb-otlp` binary, a receiver only: `serve` (the default), `validate` (serve with `DRY_RUN`, printing the generated SQL), `doctor` (probe every configured listener; also spelled `healthcheck`), `version`, `help`. Reading files and querying belong to the `otlp` extension in the `duckdb` CLI. `cli.cpp` holds one flag table whose rows carry the set of commands that accept each flag, so a misdirected flag (`doctor --init-sql`) is an error naming where it belongs. Flag values are collected as `(name, value)` pairs and layered over the process environment in an `EnvSource` (`env_source.cpp`) that `ServerConfig::FromEnv(env)` reads: that is what makes `flag > env > default` hold through one config-resolution path, without mutating the process environment (so a `--token` is never published through `getenv()`). `main.cpp`'s `RunServe` embeds DuckDB, loads the static OTLP extension, runs the mode preset and the operator's `--init-sql`, starts one `otlp_serve` call covering every configured transport and optional `quack_serve`, handles SIGTERM/SIGINT, then calls `quack_stop`/`otlp_stop`
- **Mode presets (`src/server/modes/*.sql`)**: each mode is a SQL file, compiled into the daemon by `CMakeLists.txt` as the `MODE_PRESETS` table and expanded by `PresetExpander` in `server_config.cpp`. The syntax is documented at the top of `local-ducklake.sql`: `${NAME}` / `${NAME:-default}` substitute settings (escaped for a single-quoted literal), `getenv('NAME')` becomes `getvariable('env_NAME')` bound from the environment so secrets never appear in printed SQL, and `-- @set` / `-- @if` / `-- @end` directives assign variables and keep optional lines. The daemon reads back `CATALOG`, `SCHEMA`, `DATA_PATH`, `CATALOG_PATH`, `PARQUET_EXPORT_PATH`, `CREDENTIALS` and `ALLOW_UNSIGNED_EXTENSIONS`. Adding a mode is adding a file (plus its minimal settings in `MODE_FIXTURES`, `test/server/test_server_config.py`).
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

### Function metadata is part of the registration

`duckdb_functions()` is the only description of this extension a SQL client can reach — the README and the community catalog's `extended_description` are not available over a connection. So no function is registered through the bare `loader.RegisterFunction(fn)` overload, which has nowhere to put documentation. Every one goes through `OtlpDocumented()` (`src/otlp_function_docs.cpp`), which wraps the function in the matching `Create*FunctionInfo`, attaches a `FunctionDescription` per overload, and sets `on_conflict = ALTER_ON_CONFLICT` — the bare overloads set that internally and `CreateInfo` defaults to `ERROR_ON_CONFLICT`, so the info form has to restore it. `test/sql/function_metadata.test` fails if a new function arrives without a description, an example, or with `colN` parameter names.

Two constraints shape what can be written there. `duckdb_functions()` picks which description belongs to which overload by matching `parameter_types` against the overload's **positional** arguments, so a set with several overloads needs one description each (`otlp_stop()` and `otlp_stop(uri)` say different things). And as of DuckDB 2.0 a `FunctionDescription`'s `parameter_names` are only a *fallback*: `duckdb_functions()` takes the names off the function's own signature whenever their count matches the rendered parameter list, and falls back to the description only when it does not (`GetParameterNames` in `duckdb_functions.cpp`; in v1.5.5 the description won outright). Since DuckDB 2.0 every function, scalar and table alike, carries a `FunctionSignature`, and `duckdb_functions()` renders the names in it (the `TableFunctionExtractor` walks `GetSignature().GetParameters()` and then the typed `**kwargs` options), so in practice the description's `parameter_names` are never consulted. What a client sees is therefore decided by how the signature is built. A function constructed from a bare `vector<LogicalType>` gets positional-only parameters named `col0`, `col1`, … (`FunctionSignature::AddParameter(type)`), which is why every `read_ot*` reader and every control-plane table function still reports `colN`. To give one a real name without letting callers pass it by name, declare it with `AddPositionalOnly(name, type)` (`otlp_uri_parser` names its scalar argument through a named `FunctionParameter`). Their `parameter_names` are kept anyway, to document the argument. Optional named options are declared as a typed `**kwargs` schema (`GetSignature().WithTypedKwargs("options", ...)`, the mechanism `read_csv` uses): an option the call leaves out is not passed, so the bind keeps its own defaults, and `duckdb_functions()` lists the options by name in declaration order. `otlp_serve`/`otap_serve` are built this way (`BuildServeFunctionSet` in `src/otlp_start_stop.cpp`) and leave `parameter_names` empty. (`TableFunction::named_parameters` and `arguments` no longer exist; in v1.5 the named parameters lived in an `unordered_map`, rendered as `colN`, and their order could not be reproduced.)

Examples are run, not guessed: a token shorter than `otlp_limits::MIN_TOKEN_LENGTH` is rejected at bind, and `read_otlp_metrics`/`read_otlp_metrics_summary` exist only to raise, which is why those two carry a description but no example.

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
│   ├── main.cpp               # `duckdb-otlp` entry point, serve loop, doctor
│   ├── cli.cpp                # argv parsing, per-command flag table, help
│   ├── env_source.cpp         # Process environment + command-line overlay (one config source)
│   ├── server_util.cpp        # Shared query/env/filesystem helpers
│   ├── server_config.cpp      # Environment config, listener resolution, preset expansion
│   └── modes/*.sql            # One SQL preset per --mode, compiled into the binary
├── storage/
│   └── otlp_extension.cpp     # Extension entry point + registration
├── function/
│   └── read_otlp.cpp          # FFI bridge / `read_otlp_*` table functions
├── otlp_arrow.cpp             # Arrow import via DuckDB's stock importer
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
├── public/install.sh  # `curl ... | sh` installer for the duckdb-otlp CLI
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

The CLI installer lives at `site/public/install.sh` rather than in `scripts/`, because that path *is* its URL: Astro copies `site/public/` into the Pages artifact verbatim, so `https://smithclay.github.io/duckdb-otlp/install.sh` and the file in this repository cannot drift. It installs the release tarballs that `daemon-release` attaches (`duckdb-otlp-<tag>-<platform>.tar.gz` + `SHA256SUMS`), so a change to that naming is a change to the installer. It follows the CLI's own conventions — `flag > env > default`, exit `2` for a bad command line, messaging on stderr — and refuses to install anything whose checksum does not match.

Prefer one canonical page per topic and link to it instead of duplicating examples. Since this is an early-stage project, do not add backwards-compatibility redirect pages or migration stubs unless explicitly requested.

## Testing Notes

- SQLLogicTests under `test/sql/` cover JSON parsing, protobuf parsing, option handling, and schema projections.
- All tests run against DuckDB with the extension statically linked (`make test`).
- Test data in `test/data/` includes representative OTLP JSON and protobuf fixtures used by the table functions.
- `test/pages/` holds the published-site tests (`python3 -m unittest discover -s test/pages`, run by `site-check.yml`): the extension-repository packaging, and `install.sh` driven against a fake release served over loopback.
- The Docker benchmark harness (`scripts/benchmark_catalog_ingest.py`) starts the daemon image, sends OTLP/HTTP log batches, flushes via Quack, and queries row counts/server metrics over Quack. Because the image is distroless (no in-container shell/`duckdb`), it publishes the Quack port and runs Quack queries from a **host `duckdb` CLI** — so the harness now requires `duckdb` on `PATH`. It intentionally avoids the old FIFO controller path.

## Known Limitations

- Live ingestion is exposed by two scheme-bound serve functions (no URI mixing — each rejects the other's scheme): **`otlp_serve`** on the `otlp:` scheme (default port 4318) and **`otap_serve`** on the `otap:` scheme (default port 4317). `otlp_stop` / `otlp_flush` / `otlp_server_list` are transport-agnostic — they dispatch by the scheme-aware canonical listen URI, so `otap:host:4317` and `otlp:host:4318` are distinct servers. The transport is **not** encoded in the scheme: `otlp_serve` picks it via the `transport := 'http'|'grpc'` named param (default `http`); `otap_serve` is always gRPC.
  - **One server, N listeners**: `otlp_serve` accepts a `VARCHAR[]` of listen URIs with a parallel `transport` list (or one scalar transport for all) and starts one server fed by every listener. The server owns the buffers, the admission cap (`max_buffered_bytes`), the sealer, and the maintenance schedule; listeners share them. Any listener's URI names the whole server for `otlp_stop`/`otlp_flush`. `otlp_server_list` returns one row per listener with shared counters and a `transport` column. The registry allows one server per catalog target (catalog + schema, or Parquet export root), so a second `otlp_serve`/`otap_serve` for the same target fails. The daemon's `DUCKDB_OTLP_TRANSPORTS=http,grpc` maps to one call.
  - **OTLP/HTTP** (`otlp_serve` default, `src/otlp_server.cpp`): OTLP/JSON, OTLP/NDJSON, and OTLP/protobuf POSTs to `/v1/logs`, `/v1/traces`, `/v1/metrics`.
  - **OTLP/gRPC** (`otlp_serve(transport := 'grpc')`, `src/otlp_server_grpc.cpp`): standard **OTLP/gRPC** unary `Export` (`{Logs,Trace,Metrics}Service`) for all three signals.
  - **OTAP/Arrow** (`otap_serve`, `src/otlp_server_grpc.cpp` + the embedded tonic server in `otlp2records`): canonical **OTAP/Arrow** bidirectional streaming (`Arrow{Logs,Traces,Metrics}Service`, `stream BatchArrowRecords` → `stream BatchStatus`), all six signals. OTAP streams keep one stateful decoder per stream (cross-message Arrow dictionary reuse). Metric shapes are buffered per-shape, so a backpressure nack partway through a metrics message can leave earlier shapes buffered.
  - The two gRPC service sets are **disjoint** — the host selects which to register per listener via the `service_flags` arg to `otlp_grpc_server_start` (`OTLP_GRPC_SERVICE_OTLP_UNARY` vs `OTLP_GRPC_SERVICE_OTAP_ARROW`), so calling the wrong family returns `UNIMPLEMENTED`. The gRPC stack (tokio + tonic) is statically linked into the otlp2records archive — no new shared libraries — and is native-only (absent from the WASM build, like the HTTP server).
  - **Catalog targeting**: `otlp_serve(uri, catalog := '<attached_db>')` streams into an attached catalog. Set it to a DuckLake catalog to land data as Parquet in a lakehouse; empty = the default (in-memory/file) catalog.
  - **A seal is one SQL statement per signal**: `BuildSinks` (`src/otlp_server.cpp`) fixes, at startup, the statement each signal's sealed rows are written with — `INSERT INTO <catalog>.<schema>.<table> BY NAME SELECT *[, promoted attrs] FROM sealed` for a catalog, a partitioned `COPY (SELECT *, year, month, day FROM sealed) TO <root>/<table>` for Parquet. `ExecuteOverSealed` binds the swapped-out `ColumnDataCollection` as the relation `sealed` by injecting a non-materialized CTE over a `ColumnDataRef` into the parsed statement (the mechanism DuckDB's own `Appender` flushes through), so there is no temp table and no per-row copy. `SealOnce` runs one loop over the sinks; the only destination-specific behaviour is failure: a catalog seal is one transaction (rollback, re-buffer all), a Parquet COPY is not (re-buffer only the signals that had not exported — at-least-once). `BY NAME` is also why a table wider than the signal schema (promoted columns from an earlier run, operator additions) needs no special path: the extra columns are NULL-filled.
  - **Buffered group-commit ("seal")**: ingest is buffered in memory (per-signal `ColumnDataCollection` with per-signal locking) and a single background sealer thread group-commits on internal size/age triggers or an explicit `otlp_flush`. One seal = one transaction (for DuckLake: one Parquet file per signal + one snapshot), so a single serialized writer avoids DuckLake's optimistic-concurrency conflicts and tiny-file churn. The configurable httplib worker pool only parses/converts/buffers concurrently.
  - **Durability**: ingest is buffered in memory and durability is the seal. A POST returns **`202 Accepted`** (`{"status":"buffered",...}`) once rows are parsed and buffered in memory, but not yet durable; they commit at the next seal. **`otlp_stop` and `otlp_flush` seal remaining rows before returning; a plain database/connection close does NOT** — buffered-but-un-sealed rows can be lost, so callers must `otlp_stop`/`otlp_flush` before closing the database. Backpressure: `max_buffered_bytes` (default 512 MiB) bounds cumulative *admitted request-body bytes*, not decoded buffer heap — each request reserves `max(body_size, 1024)` input bytes against this budget (the decoded columnar size differs from the encoded/compressed input size). A request whose admission would exceed the budget is rejected with **`503`**.
  - **Catalog maintenance** runs `CHECKPOINT <catalog>` from the sealer thread at most once per `maintenance_interval_ms` (default 5 min), and only after rows were sealed since the last attempt (`catalog_maintenance_due`, which starts true). There is no adaptive scheduling: during a long checkpoint, seals wait and ingest buffers against `max_buffered_bytes`. Tests shorten the interval through the config field or the `maintenance_interval_ms` named parameter.
  - **Catalog maintenance contention**: several processes can share one DuckLake catalog, so their `CHECKPOINT`s can race. `IsContendedCatalogMaintenanceError` (`src/otlp_server.cpp`) classifies the loser (`could not serialize access`, `concurrent delete/update`) as contention: it bumps `maintenance_contended_total` and leaves `maintenance_failures_total`/`maintenance_last_error` alone. After each successful DuckLake `CHECKPOINT`, `SweepOrphanedFiles` calls `ducklake_delete_orphaned_files(older_than => now() - maintenance_retention_ms)` to remove untracked files left by a losing checkpoint; sweep errors are logged, never counted as checkpoint failures.
  - **`otlp_stop()` with no argument** stops and seals **every** server on the instance, returning one row per server, and leaves the registry usable (it does not mark teardown). The daemon's shutdown uses this form deliberately: `otlp_stop('<its own uri>')` never reached a server started out-of-band — over Quack, or by an `--init-sql` script — so that server fell through to `~OtlpStorageExtensionInfo`, where `OtlpServer::db_ptr` has already expired, `SealOnce` is a silent no-op and its buffered rows are dropped while the daemon still exits 0. DuckDB offers no pre-teardown hook (`~DatabaseInstance` resets `db_manager`/`buffer_manager` before `config.storage_extensions` dies, and `ExtensionCallback` has only per-connection and per-extension-load hooks), so a graceful stop while the database is alive is the *only* place those rows can be committed. Regression test: `test_init_sql_adds_a_parquet_destination_next_to_the_ducklake_catalog` in `test/server/test_server_transports.py`.
  - **`otlp_flush(uri)`** forces a synchronous seal. `otlp_server_list` exposes buffer/seal metrics (`buffered_rows`, `last_seal_age_ms`, `seals_total`, `seal_failures_total`, `seal_last_error`, `catalog_name`). Verify the ingest/seal path with `test/manual/otlp_serve_concurrency.py` (set `OTLP_DUCKLAKE_DIR` for the DuckLake path).
  - **Exit codes and streams**: `0` success, `1` the work failed, `2` the command line was wrong (every usage error is raised in `ParseCli`, so the split is one the parser can make honestly). Data on stdout, messaging on stderr. `main()` sets `std::cout << std::unitbuf`: std::cout is fully buffered when stdout is not a TTY, so a redirected log held the entire `serve` banner until the process exited — `docker logs` on a healthy container showed a raw listener table and nothing else. `doctor --json` is the machine-readable form.
  - **Bind-host precedence**: a `--host` flag beats everything; between environment variables the more specific wins, so a host inside `OTEL_HTTP_ADDR`/`OTEL_GRPC_ADDR` beats `DUCKDB_OTLP_HOST`. That ordering exists because the container image sets `DUCKDB_OTLP_HOST=0.0.0.0`, and an operator narrowing the bind with `OTEL_HTTP_ADDR=127.0.0.1:4318` must not be silently widened back to the wildcard. `EnvSource::IsOverride` is what distinguishes the flag from the variable. `MakeListener` is the single place that brackets an IPv6 literal before recombining host and port.
  - **CLI defaults vs container defaults**: the C++ defaults are laptop-shaped — loopback bind, `$XDG_DATA_HOME/duckdb-otlp` data dir, `DUCKDB_MODE=local-ducklake`, and both OTLP/HTTP (4318) and OTLP/gRPC (4317) enabled. The container's `/data`, `0.0.0.0`, http-only defaults live in `docker/duckdb-otlp-server/Dockerfile`'s `ENV` block, deliberately, so there is no "am I in a container" detection at runtime. Changing a default means deciding which of the two it belongs to.
  - **No built-in token**: there is no default token any more. With no token configured and every listener on loopback, auth is disabled automatically (with a printed notice); a non-loopback bind and no token is a hard startup error. `--no-auth` opts into unauthenticated traffic anywhere.
  - **Standard OTLP env vars**: `OTEL_EXPORTER_OTLP_PROTOCOL` (only when no port is explicit), and `OTEL_EXPORTER_OTLP_HEADERS` (`Authorization=Bearer`) are honored. `OTEL_EXPORTER_OTLP_ENDPOINT` is deliberately **not read** (it usually points at the user's real collector, so reading it as a bind address is a footgun); the per-signal endpoint and TLS variables are **rejected at startup** rather than ignored, because silently dropping them would misrepresent the deployment.
  - **AWS credentials use a multi-source chain**: every AWS preset (`aws-ducklake`, `parquet` with `S3_BUCKET`, `s3-tables`) creates a `credential_chain` secret with `CHAIN 'env;config;instance'`, plus `PROFILE` when `AWS_PROFILE` is set. A single-source chain is a trap in both directions — `CHAIN env` cannot see an EC2/ECS instance role, `CHAIN instance` cannot see the standard AWS variables — and this mode set has been bitten by each. `CREDENTIALS` must keep naming the sources the chain actually contains.
  - **Presets do not validate values, only presence**: a malformed `DUCKLAKE_DATA_PATH` fails when the SQL runs. `parquet` loads the S3 filesystem and credential chain when the resolved `PARQUET_EXPORT_PATH` starts with `s3://` (`-- @if PARQUET_EXPORT_PATH^=s3://`), whether it was set directly or composed from `S3_BUCKET`/`S3_PREFIX`. The R2 modes' default DuckLake `DATA_PATH` keeps its trailing slash (`s3://bucket/prefix/`), because DuckLake treats it as a directory prefix and dropping it would repoint an existing deployment at a different prefix than the one in its catalog.
  - **A preset's extensions are its own `INSTALL`/`LOAD` lines**: `otlp` is statically embedded and must never be `INSTALL`ed (that would fetch a different published build); a community extension needs `FROM community`. `test_every_mode_renders_with_its_minimal_settings` checks both per mode. The image's offline extension cache (`docker/duckdb-otlp-server/Dockerfile`) cannot share the presets — `deps` must not depend on `builder` — so `test_the_image_primes_every_extension_some_mode_needs` reads every preset's `INSTALL`s and asserts the image covers them; without it, a mode gaining an extension silently sends the container to the network at startup, or fails outright when offline. Quack is loaded by `StartQuackSql` with httpfs first: every Quack server draws a random HMAC key from DuckDB's encryption module (and TLS needs it too), which is writable only once httpfs is loaded, so without it `quack_serve` fails with `read-only crypto module loaded`. It is `CORE` — `INSTALL quack FROM community` 404s.
  - **One name per setting, one error per run**: each preset reads one canonical name per concept: the cloud's own convention where one exists (`AWS_*`, `PG*` libpq names), `R2_*` for R2, `S3_*` for S3, `DUCKDB_OTLP_*` only for this daemon's own settings. `PresetExpander` collects every missing setting while expanding and throws once naming all of them, deduplicated by variable; a missing setting inside a default (`${R2_ENDPOINT:-${CLOUDFLARE_ACCOUNT_ID}...}`) names what could replace it. Reporting one per run made configuring `r2-data-catalog` take six runs.
  - **`parquet` mode has no catalog**: a signal is readable through `read_parquet('<root>/<table>/**/*.parquet', hive_partitioning=false, union_by_name=true)` (`ParquetDatasetSelect`, `otlp_sql_util.hpp`). `hive_partitioning=false` because the COPY sets `WRITE_PARTITION_COLUMNS false` (year/month/day live only in the path). The seal (`EnsureParquetView`) creates that view in the control database once files exist, but only when `parquet_views` is on, which the daemon passes only with Quack enabled: binding it reads every archived footer.
  - **`--mode none`**: attaches nothing, installs nothing, sets no catalog — the destination is entirely `--init-sql` plus `--catalog`. Every mode contributes only setup SQL and a catalog name (see `BootSql`), both already expressible that way, but without an opt-out an operator's script was layered on top of an unwanted `local-ducklake` ATTACH. The presets are a hand-picked subset of (3 catalog kinds × 2 catalog stores × 4 storage backends); `none` is how an uncovered combination (S3 data files with a Postgres catalog, say) is reached without adding a ninth.
  - **Credentials may come from DuckDB's secret store**: an R2 preset wraps its `CREATE OR REPLACE SECRET` in `-- @if R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY`, so it is emitted *only* when a key pair is in the environment. With none set it emits nothing, so a `CREATE PERSISTENT SECRET` in `$HOME/.duckdb/stored_secrets` (`--secret-dir` / `DUCKDB_OTLP_SECRET_DIR` relocates it) is picked up automatically — emitting the secret unconditionally would shadow the stored one with an empty credential. Half a key pair is a hard error (the `@if` requires every name once any is set). The startup banner names the resolved source (`Credentials: ...`). `secret_directory` is emitted first in `BootSql`, before anything can initialize the secret manager. The container's `HOME=/duckdb-home` is where a mounted secret directory lands.
  - **Custom DuckDB setup (`--init-sql` / `DUCKDB_OTLP_INIT_SQL`)**: a SQL script run after mode setup and before ingest starts — the escape hatch for DuckDB configuration the modes do not model (extra `ATTACH`, `SET`, secrets, views). Accepted by `serve` and `validate` (the `SERVE_CMDS` mask in `cli.cpp`); **not** by `doctor`, because the container `HEALTHCHECK` runs it on every probe and a liveness check must not execute operator SQL. Resolved to *contents* in `ServerConfig::FromEnv` (via `ReadSqlFile`) rather than kept as a path, so `validate` both rejects an unreadable script and prints it inside the generated boot SQL; `doctor` never calls `FromEnv`, so the probe never reads it. Failures are fatal. It runs before the default-catalog switch, so a catalog the script attaches can still be named by `--catalog`.
  - **Second destination is a second server, never a tee**: one server has exactly one ingest target (`otlp_serve` rejects `parquet_export_path` combined with a catalog, and each signal has exactly one sink statement), so fanning out to both a catalog and a Parquet directory means a second `otlp_serve` on its own port from the `--init-sql` script. That is fan-out **by port** — the exporter picks the destination — and there is deliberately no way to mirror one stream into two targets, because one target has one writer is what keeps a single sealer per catalog.
  - **Daemon SQL access**: the daemon does not expose an attached DuckDB shell. Enable Quack (`DUCKDB_QUACK_ENABLED=1` and `DUCKDB_QUACK_TOKEN=...`) when external SQL/admin access is required. Quack grants full SQL read/write access to the daemon's DuckDB connection, so treat it as an administrative endpoint. As of DuckDB 2.0, `quack_serve` serves TLS on every non-loopback bind (the container's `0.0.0.0`), with a self-signed certificate unless `DUCKDB_QUACK_SSL_CERT_FILE`/`DUCKDB_QUACK_SSL_KEY_FILE` supply one. The daemon does not pass `disable_ssl`. Quack keeps that pair under `<home_directory>/.duckdb/extension_data/quack`, and `$HOME` is part of the image, so `StartQuackSql` points the connection-local `home_directory` setting at the data directory (the `/data` volume) around `quack_serve` only, after `INSTALL`/`LOAD quack` so the extension cache still resolves against `$HOME`. The fingerprint therefore survives a redeploy that keeps the volume; it used to change with every new container and break every client's pin. Quack also always loads httpfs first (`QuackExtensions`): every Quack server draws a random HMAC key from DuckDB's encryption module (and TLS needs it too), which is writable only once httpfs is loaded, so without it `quack_serve` fails on any bind with `read-only crypto module loaded`. The image primes httpfs and DuckDB autoloads it, so only hosts without httpfs installed hit that. Clients pin the certificate with `SSL_FINGERPRINT` (ATTACH) / `ssl_fingerprint` (`quack_query`); without it a client tries plain HTTP and gets `Unsupported protocol error`. The fingerprint is printed after startup (`PrintQuackEndpoint`, from `quack_server_list()`), and `scripts/smoke_test.py` and the benchmark harness read it from that banner line.
  - Not available on the wasm build.
- Summary metrics are not yet supported
- The union metrics function (`read_otlp_metrics`) is not yet implemented
- **OTAP reads (`read_otap_*`)** decode canonical `BatchArrowRecords` files into the same flattened schemas as `read_otlp_*`, with these constraints:
  - **One self-contained message per file.** Each file is decoded with one stateful decoder as a single `BatchArrowRecords`. A file holding several concatenated messages, or a "reuse" message that omits its schema/dictionaries and depends on a prior message in the same decoder session, is not supported and surfaces an `OTAP decode error`.
  - **Envelopes are per-signal.** Canonical OTAP carries one signal family (logs *or* traces *or* metrics) per message, so use the reader that matches the file. A metrics envelope can hold several metric *shapes* at once — one file feeds `read_otap_metrics_gauge`/`_sum`/`_histogram`/`_exp_histogram`, each of which extracts its shape (the rest are released), exactly like `read_otlp_metrics_*`. Summary data points are counted as skipped.
  - **Wrong/foreign payloads are a hard error, never silent.** Calling a reader on a file of a different signal (or an envelope mixing incompatible payloads) throws `OTAP decode error … Parse failed` rather than returning partial or mis-typed rows.
  - **Compression:** native builds decode uncompressed, LZ4, and Zstandard (the producer default, via `otap-zstd`); WASM decodes uncompressed/LZ4 only.
  - **File reads only** — OTAP is not accepted by the live HTTP ingest server.
