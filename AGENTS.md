# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Build System

- **Always use `GEN=ninja`** when running make commands for caching and performance: `GEN=ninja make`
- **VCPKG is required** for dependency management. Set environment variable before building:
  ```bash
  export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
  ```
- Use `uv` for Python tooling (formatting, testing, dependency management)
- **Windows (`windows_amd64`, MSVC) builds the extension only.** `CMakeLists.txt` gates the
  native daemon (`duckdb_otlp_server`) and the C++ harnesses (`otlp_seal_harness`,
  `otlp_arrow_harness`) on `NOT WIN32`, because `make release` builds the default ALL target
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

There is a single `docker/duckdb-otlp-server/Dockerfile` with named stages: `builder` (compiles the static daemon), `daemon-export` (exports just the binary), `deps` (throwaway: primes the extension cache and stages `libz`), `runtime-base` (the runnable image minus the binary), and two leaf stages — `runtime-source` (default; copies the binary from `builder`, used by `make docker-image-local`) and `runtime-prebuilt` (copies `docker-bin/$TARGETARCH/duckdb-otlp-server`, used by CI). `runtime-prebuilt` does not depend on `builder`, so packaging never recompiles DuckDB — keep it that way.

The final image is `gcr.io/distroless/cc-debian12:nonroot` (~48MB base): glibc + NSS (DNS) + libstdc++ + openssl + CA certs, no shell, no package manager, no bundled DuckDB CLI, runs as nonroot (uid 65532). The daemon and the runtime extensions link only glibc/libstdc++/libgcc/libz; distroless/cc ships everything except `libz`, so the `deps` stage stages `libz.so.1` into `/opt/duckdb-otlp/lib` (`LD_LIBRARY_PATH`). Because there is no shell/curl, the container `HEALTHCHECK` is the daemon's own `duckdb-otlp doctor` subcommand (`healthcheck` remains an accepted spelling, so existing probes keep working). It reports one line per check and runs them all rather than stopping at the first failure, and Docker keeps that output in the container's health log. It probes every configured listener: HTTP → GET `/readyz`; OTLP/gRPC and OTAP/Arrow (HTTP2, no `/readyz`) → a TCP-connect probe. `DUCKDB_OTLP_TRANSPORTS=http|grpc|http,grpc` selects standard OTLP listeners; `OTEL_HTTP_ADDR` and `OTEL_GRPC_ADDR` select their separate bind addresses. The default remains HTTP. An existing single `otap:` listen URI still selects OTAP/Arrow. Quack is also probed when enabled. All probes target the configured bind host (loopback for a wildcard bind).

CI in `.github/workflows/MainDistributionPipeline.yml`: `daemon-compile` is a cheap amd64-only `daemon-export` build that runs daemon configuration and real HTTP/gRPC transport tests and gates PRs and feature-branch pushes (no publish). The publish path runs only on `main`/tags/`workflow_dispatch`: `daemon-linux` exports `amd64`/`arm64` binaries (`daemon-export` target), `daemon-macos` builds `darwin-arm64`/`darwin-amd64` natively, `docker-smoke` runs the benchmark e2e on the packaged amd64 image, and `docker-image` publishes with `--target runtime-prebuilt`.

`daemon-macos` cannot reuse the Docker path (a Linux container cannot produce a Darwin binary), so it runs `make server-release` on the arm64 `macos-15` runner for both architectures, with `OSX_BUILD_ARCH` driving `CMAKE_OSX_ARCHITECTURES` and — through `cmake/FindOtlp2Records.cmake` — the Rust target. That is the same cross-compile `extension-ci-tools` uses for `osx_amd64`. It sets no vcpkg toolchain: `vcpkg.json` declares zlib for Windows only, so on macOS `find_package(ZLIB)` resolves to the SDK's libz. It pins `OVERRIDE_GIT_DESCRIBE` to the workflow's `DAEMON_DUCKDB_VERSION` for the same reason the Dockerfile does — an unpinned `git describe` version has no published extension build for the daemon's runtime `INSTALL` to fetch — so keep that variable in sync with the Dockerfile's `DUCKDB_VERSION` ARG.

On a `v*` tag, `daemon-release` attaches those four binaries to the GitHub release as `duckdb-otlp-<tag>-<platform>.tar.gz` plus `SHA256SUMS`. It recompiles nothing (it downloads the artifacts the build jobs uploaded) and gates on `docker-smoke`, so a binary that failed the e2e never reaches a release. The Darwin artifacts are deliberately named `duckdb-otlp-darwin-*` rather than `duckdb-otlp-server-*`: the latter is the glob `docker-image` downloads, which must only match the Linux binaries. `daemon-release` creates the release when the tag was pushed without one, which in turn triggers `pages.yml` to attach the extension repository.

Extension offline-cache coupling: the `deps` stage pre-`INSTALL`s ducklake/iceberg/httpfs/aws/postgres/quack and community gcs with a throwaway DuckDB CLI into `HOME=/duckdb-home` (copied into the final image, owned by the nonroot user; the CLI itself is not copied). The daemon reuses that cache only because it runs with the same `HOME` and is pinned to the same `DUCKDB_VERSION`. If those diverge, the daemon's startup `INSTALL`/`LOAD` re-downloads (or fails when offline). The `otlp` extension is statically embedded in the daemon and is intentionally not installed. Bind-mounted `/data` must be writable by uid 65532; named/anonymous volumes inherit the image's nonroot ownership automatically.

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
- **Live ingest server (`src/otlp_server.cpp`)**: `OtlpServer` is one pipeline (buffers, admission cap, single sealer/writer, catalog maintenance) fed by N `OtlpListener`s. A listener is a bound socket plus a transport and owns no buffers or counters
- **HTTP listener (`src/otlp_server_http.cpp`)**: `OtlpHttpListener`, native OTLP/HTTP ingest for `/v1/logs`, `/v1/traces`, and `/v1/metrics`
- **gRPC listener (`src/otlp_server_grpc.cpp`)**: `OtlpGrpcListener` bridges the embedded tonic gRPC server (in `otlp2records`) into the same server as the HTTP path via a per-batch C callback. Two disjoint gRPC service families, selected via the `service_flags` FFI arg: OTLP/gRPC unary `Export` for `otlp_serve(transport := 'grpc')`, OTAP/Arrow streaming for `otap_serve`
- **Buffered storage (`src/otlp_storage.cpp`)**: Per-signal buffering and serialized background seal/group-commit path
- **Start/stop SQL functions (`src/otlp_start_stop.cpp`)**: `otlp_serve` (HTTP), `otap_serve` (gRPC), `otlp_stop`, `otlp_flush`, and `otlp_server_list`
- **Native CLI / daemon (`src/server/`)**: the `duckdb-otlp` binary. `cli.cpp` parses argv and dispatches subcommands; `commands.cpp` implements `convert`/`export`/`query`; `main.cpp`'s `RunServe` embeds DuckDB, loads the static OTLP extension, executes mode setup, runs the operator's `--init-sql` script, starts one `otlp_serve` call covering every configured transport and optional `quack_serve`, handles SIGTERM/SIGINT, then calls `quack_stop`/`otlp_stop`. `cli.cpp` holds one flag table whose rows carry the set of commands that accept each flag, so a misdirected flag (`convert --quack`) is an error naming where it belongs rather than a silent no-op, and `--otap` can mean a listener port for `serve` and an input format for `convert` without a special case. Flag values are collected as `(name, value)` pairs and layered over the process environment in an `EnvSource` (`env_source.cpp`) that `ServerConfig::FromEnv(env)` reads: that is what makes `flag > env > default` hold for every command through one config-resolution path, without any command mutating the process environment (so a `--token` is never published through `getenv()`)
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

Two constraints shape what can be written there. `duckdb_functions()` picks which description belongs to which overload by matching `parameter_types` against the overload's **positional** arguments, so a set with several overloads needs one description each (`otlp_stop()` and `otlp_stop(uri)` say different things). And it renders positional and named parameters as one `parameters` list, where every entry past the end of `parameter_names` becomes `colN` — so for `otlp_serve`/`otap_serve`, naming the one positional argument would replace every named parameter's real name with a placeholder. Their `parameter_names` is deliberately empty; the named parameters' order comes from an `unordered_map` the catalog copies, so it cannot be reproduced at registration time anyway.

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
│   ├── main.cpp               # `duckdb-otlp` entry point + subcommand dispatch + serve loop
│   ├── cli.cpp                # argv parsing, per-command flag table, per-command help
│   ├── env_source.cpp         # Process environment + command-line overlay (one config source)
│   ├── commands.cpp           # `convert` / `export` / `query` subcommands
│   ├── server_util.cpp        # Shared query/env/filesystem helpers for all subcommands
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
- The Docker benchmark harness (`scripts/benchmark_catalog_ingest.py`) starts the daemon image, sends OTLP/HTTP log batches, flushes via Quack, and queries row counts/server metrics over Quack. Because the image is distroless (no in-container shell/`duckdb`), it publishes the Quack port and runs Quack queries from a **host `duckdb` CLI** — so the harness now requires `duckdb` on `PATH` (the `docker-smoke` CI job installs the pinned v1.5.5 CLI). It intentionally avoids the old FIFO controller path.

## Known Limitations

- Live ingestion is exposed by two scheme-bound serve functions (no URI mixing — each rejects the other's scheme): **`otlp_serve`** on the `otlp:` scheme (default port 4318) and **`otap_serve`** on the `otap:` scheme (default port 4317). `otlp_stop` / `otlp_flush` / `otlp_server_list` are transport-agnostic — they dispatch by the scheme-aware canonical listen URI, so `otap:host:4317` and `otlp:host:4318` are distinct servers. The transport is **not** encoded in the scheme: `otlp_serve` picks it via the `transport := 'http'|'grpc'` named param (default `http`); `otap_serve` is always gRPC.
  - **One server, N listeners**: `otlp_serve` accepts a `VARCHAR[]` of listen URIs with a parallel `transport` list (or one scalar transport for all) and starts one server fed by every listener. The server owns the buffers, the admission cap (`max_buffered_bytes`), the sealer, and the maintenance schedule; listeners share them. Any listener's URI names the whole server for `otlp_stop`/`otlp_flush`. `otlp_server_list` returns one row per listener with shared counters and a `transport` column. The registry allows one server per catalog target (catalog + schema, or Parquet export root), so a second `otlp_serve`/`otap_serve` for the same target fails. The daemon's `DUCKDB_OTLP_TRANSPORTS=http,grpc` maps to one call.
  - **OTLP/HTTP** (`otlp_serve` default, `src/otlp_server.cpp`): OTLP/JSON, OTLP/NDJSON, and OTLP/protobuf POSTs to `/v1/logs`, `/v1/traces`, `/v1/metrics`.
  - **OTLP/gRPC** (`otlp_serve(transport := 'grpc')`, `src/otlp_server_grpc.cpp`): standard **OTLP/gRPC** unary `Export` (`{Logs,Trace,Metrics}Service`) for all three signals.
  - **OTAP/Arrow** (`otap_serve`, `src/otlp_server_grpc.cpp` + the embedded tonic server in `otlp2records`): canonical **OTAP/Arrow** bidirectional streaming (`Arrow{Logs,Traces,Metrics}Service`, `stream BatchArrowRecords` → `stream BatchStatus`), all six signals. OTAP streams keep one stateful decoder per stream (cross-message Arrow dictionary reuse). Metric shapes are buffered per-shape, so a backpressure nack partway through a metrics message can leave earlier shapes buffered.
  - The two gRPC service sets are **disjoint** — the host selects which to register per listener via the `service_flags` arg to `otlp_grpc_server_start` (`OTLP_GRPC_SERVICE_OTLP_UNARY` vs `OTLP_GRPC_SERVICE_OTAP_ARROW`), so calling the wrong family returns `UNIMPLEMENTED`. The gRPC stack (tokio + tonic) is statically linked into the otlp2records archive — no new shared libraries — and is native-only (absent from the WASM build, like the HTTP server).
  - **Catalog targeting**: `otlp_serve(uri, catalog := '<attached_db>')` streams into an attached catalog. Set it to a DuckLake catalog to land data as Parquet in a lakehouse; empty = the default (in-memory/file) catalog.
  - **Buffered group-commit ("seal")**: ingest is buffered in memory (per-signal `ColumnDataCollection` with per-signal locking) and a single background sealer thread group-commits on internal size/age triggers or an explicit `otlp_flush`. One seal = one transaction (for DuckLake: one Parquet file per signal + one snapshot), so a single serialized writer avoids DuckLake's optimistic-concurrency conflicts and tiny-file churn. The configurable httplib worker pool only parses/converts/buffers concurrently.
  - **Durability**: ingest is buffered in memory and durability is the seal. A POST returns **`202 Accepted`** (`{"status":"buffered",...}`) once rows are parsed and buffered in memory, but not yet durable; they commit at the next seal. **`otlp_stop` and `otlp_flush` seal remaining rows before returning; a plain database/connection close does NOT** — buffered-but-un-sealed rows can be lost, so callers must `otlp_stop`/`otlp_flush` before closing the database. Backpressure: `max_buffered_bytes` (default 512 MiB) bounds cumulative *admitted request-body bytes*, not decoded buffer heap — each request reserves `max(body_size, 1024)` input bytes against this budget (the decoded columnar size differs from the encoded/compressed input size). A request whose admission would exceed the budget is rejected with **`503`**.
  - **Catalog maintenance contention**: several processes can share one DuckLake catalog, so their `CHECKPOINT`s can race. `IsContendedCatalogMaintenanceError` (`src/otlp_server.cpp`) classifies the loser (`could not serialize access`, `concurrent delete/update`) as contention: it bumps `maintenance_contended_total` and leaves `maintenance_failures_total`/`maintenance_last_error` alone. After each successful DuckLake `CHECKPOINT`, `SweepOrphanedFiles` calls `ducklake_delete_orphaned_files(older_than => now() - maintenance_retention_ms)` to remove untracked files left by a losing checkpoint; sweep errors are logged, never counted as checkpoint failures.
  - **`otlp_stop()` with no argument** stops and seals **every** server on the instance, returning one row per server, and leaves the registry usable (it does not mark teardown). The daemon's shutdown uses this form deliberately: `otlp_stop('<its own uri>')` never reached a server started out-of-band — over Quack, or by an `--init-sql` script — so that server fell through to `~OtlpStorageExtensionInfo`, where `OtlpServer::db_ptr` has already expired, `SealOnce` is a silent no-op and its buffered rows are dropped while the daemon still exits 0. DuckDB offers no pre-teardown hook (`~DatabaseInstance` resets `db_manager`/`buffer_manager` before `config.storage_extensions` dies, and `ExtensionCallback` has only per-connection and per-extension-load hooks), so a graceful stop while the database is alive is the *only* place those rows can be committed. Regression test: `test_init_sql_adds_a_parquet_destination_next_to_the_ducklake_catalog` in `test/server/test_server_transports.py`.
  - **`otlp_flush(uri)`** forces a synchronous seal. `otlp_server_list` exposes buffer/seal metrics (`buffered_rows`, `last_seal_age_ms`, `seals_total`, `seal_failures_total`, `seal_last_error`, `catalog_name`). Verify the ingest/seal path with `test/manual/otlp_serve_concurrency.py` (set `OTLP_DUCKLAKE_DIR` for the DuckLake path).
  - **Exit codes and streams**: `0` success, `1` the work failed, `2` the command line was wrong (every usage error is raised in `ParseCli`, including missing required positionals, so the split is one the parser can make honestly). Data on stdout, messaging on stderr. `main()` sets `std::cout << std::unitbuf`: std::cout is fully buffered when stdout is not a TTY, so a redirected log held the entire `serve` banner until the process exited — `docker logs` on a healthy container showed a raw listener table and nothing else. `doctor --json` is the machine-readable form.
  - **Output paths are created, and a URI is not a path**: `ResolveOutputPath` has always created the parent of an unpartitioned `--to`, but the `--partition-by day` branch did not, and DuckDB's partitioned COPY creates the `year=/month=/day=` levels without the root above them (its directory creation is not recursive) — so exporting into a directory that did not exist yet failed with `Failed to create directory <root>/<table>` in every mode. `PartitionedOutputDirectory` is now shared by the `CreateDirectory` call, the COPY target, and the "Wrote" line, so the three cannot drift. Separately, `CreateDirectory`/`CreateParentDirectory` no-op on a remote path (`IsRemotePath` in `server_util.cpp`, any `<scheme>://`): `std::filesystem` does not reject a URI, it builds a literal `s3:/bucket/prefix` tree under the working directory, so `export --to s3://...` used to litter wherever the command was run.
  - **Output files**: with no `--format`, the extension of `--to` selects it (`FormatFromExtension` in `cli.cpp`), matching what DuckDB's own `COPY ... TO 'x.parquet'` does; an explicit `--format` wins. A directory is spelled with a trailing `/` or already exists as one — deliberately NOT "whenever several signals are written", which made the output shape depend on the data rather than the command. JSON/NDJSON output needs DuckDB's json COPY function, so `json` is linked into the daemon via `CORE_EXTENSIONS` in the Makefile rather than installed at runtime (`convert` must work with nothing set up, and the distroless image has no network).
  - **Fan-out is best effort**: `--signal all` / `--signal metrics` skip and name what is not there, in both `convert` (a reader that rejects the input) and `export` (a table not in the catalog). A signal named on its own stays a hard error in both.
  - **Bind-host precedence**: a `--host` flag beats everything; between environment variables the more specific wins, so a host inside `OTEL_HTTP_ADDR`/`OTEL_GRPC_ADDR` beats `DUCKDB_OTLP_HOST`. That ordering exists because the container image sets `DUCKDB_OTLP_HOST=0.0.0.0`, and an operator narrowing the bind with `OTEL_HTTP_ADDR=127.0.0.1:4318` must not be silently widened back to the wildcard. `EnvSource::IsOverride` is what distinguishes the flag from the variable. `MakeListener` is the single place that brackets an IPv6 literal before recombining host and port.
  - **CLI defaults vs container defaults**: the C++ defaults are laptop-shaped — loopback bind, `$XDG_DATA_HOME/duckdb-otlp` data dir, `DUCKDB_MODE=local-ducklake`, and both OTLP/HTTP (4318) and OTLP/gRPC (4317) enabled. The container's `/data`, `0.0.0.0`, http-only defaults live in `docker/duckdb-otlp-server/Dockerfile`'s `ENV` block, deliberately, so there is no "am I in a container" detection at runtime. Changing a default means deciding which of the two it belongs to.
  - **No built-in token**: there is no default token any more. With no token configured and every listener on loopback, auth is disabled automatically (with a printed notice); a non-loopback bind and no token is a hard startup error. `--no-auth` opts into unauthenticated traffic anywhere.
  - **Standard OTLP env vars**: `OTEL_EXPORTER_OTLP_PROTOCOL` (only when no port is explicit), and `OTEL_EXPORTER_OTLP_HEADERS` (`Authorization=Bearer`) are honored. `OTEL_EXPORTER_OTLP_ENDPOINT` is deliberately **not read** (it usually points at the user's real collector, so reading it as a bind address is a footgun); the per-signal endpoint and TLS variables are **rejected at startup** rather than ignored, because silently dropping them would misrepresent the deployment.
  - **AWS credentials use a multi-source chain**: `BuildCredentialChainSecret` emits `CHAIN 'env;config;instance'` when no `AWS_PROFILE` is set (and `CHAIN config` + `PROFILE` when one is). A single-source chain is a trap in both directions — `CHAIN env` cannot see an EC2/ECS instance role, `CHAIN instance` cannot see the standard AWS variables — and this mode set has been bitten by each: `aws-ducklake` hardcoded `CHAIN instance`, and consolidating it onto the shared builder briefly swapped that for `CHAIN env`, taking instance-role credentials away from exactly the deployments that have nothing else. `AwsCredentialsSource` must keep naming the sources the chain actually contains; it previously claimed "environment, then instance role" while the SQL said `env`.
  - **`parquet` locality is `IsRemotePath`, not `IsS3Path`**: `IsS3Path` answers "does this need the S3 credential set", which is a different question from "is this a local directory". Using it as the locality test sent a `gcss://` export path down the local branch — no filesystem extension loaded, empty setup SQL, `validate` reporting success — and the first seal was the first sign. A remote non-S3 path is now recorded as unusable at configuration time. The R2 modes' default DuckLake `DATA_PATH` keeps its trailing slash (`ObjectStorePath` + `"/"`), because DuckLake treats it as a directory prefix and dropping it would repoint an existing deployment at a different prefix than the one in its catalog.
  - **A mode declares its extensions once**: `ServerConfig::mode_extensions` is a list of `{name, ExtensionSource}` (`BUILT_IN` / `CORE` / `COMMUNITY`), and both the `INSTALL`/`LOAD` prelude (`ExtensionSetupSql`, spliced ahead of each mode's SQL) and the startup banner are derived from it. Each mode used to state the same list twice — once as strings for the banner, once as hand-typed SQL — so they could disagree with no symptom beyond a misleading banner. Provenance is the only thing that varies and it decides everything: `BUILT_IN` (otlp, statically embedded) emits nothing, so an `INSTALL otlp` that would fetch a *different* published build is unrepresentable; `COMMUNITY` emits `FROM community`, without which the install resolves nowhere. `test_generated_sql_installs_exactly_what_the_banner_reports` pins the two renderings together per mode. The image's offline extension cache (`docker/duckdb-otlp-server/Dockerfile`) is a third copy that cannot share the declaration — different language, and `deps` must not depend on `builder` — so `test_the_image_primes_every_extension_some_mode_needs` asserts it covers the union instead; without it, a mode gaining an extension silently sends the container to the network at startup, or fails outright when offline. Quack is declared the same way but kept out of `mode_extensions`, because it is loaded by the serve path only and `query`/`export` must not pull a server extension in: `QUACK_EXTENSION` is declared once and consumed by `StartQuackSql` and by `AllExtensions()` (what the banner prints). It is `CORE` — `INSTALL quack FROM community` 404s while a bare `INSTALL quack` resolves, which is also how the image primes it. Its `LOAD` previously carried no `INSTALL`, so `--quack` failed with `Extension "quack" not found. Install it first` on every host except the container, whose image primes the cache.
  - **One name per setting, one error per run**: storage configuration used to spell the same concept differently per mode (five names for an R2 access key, `NEON_PG*` vs `PG*` for the same Postgres catalog, six for a bucket) — about 70 variable names for ~25 concepts. Each mode now reads one canonical name: the cloud's own convention where one exists (`AWS_*`, `PG*` libpq names), `R2_*` for R2, `S3_*` for S3, `DUCKDB_OTLP_*` only for this daemon's own settings. `ConfigProblems` (`server_config.cpp`) accumulates every problem with a mode's configuration — absent values via `Add`, set-but-unusable ones via `Invalid` — and `ConfigureMode` throws once naming all of them, deduplicated by variable. Malformed values are recorded rather than thrown so they do not short-circuit the rest of the pass, which is what made `DUCKLAKE_DATA_PATH=/local/path` report the path on one run and `AWS_REGION` only on the next — configuring `r2-data-catalog` took six runs to satisfy when each error named a single variable. `MODES` is one table of `{name, configure}` so the supported-mode list in the error cannot drift from the set actually handled.
  - **`parquet` mode reads back through views, defined in one place**: the mode has no catalog, so a signal is readable only through a view over `<root>/<table>/**/*.parquet`. `SealParquet` already creates that view after each successful export (it can only be created once files exist, which is why mode setup cannot do it), so serving and reading on one host was already covered. What was broken is narrower than it looks: `ExistingSignalTables` asked `duckdb_tables()` only, so `export` found **zero** signal tables and refused to export data that was sitting there readable — it now unions `duckdb_views()`. `RegisterParquetExportViews` (`commands.cpp`, from `OpenConfiguredDatabase`) covers the remaining gap: a dataset written by another process or host, whose control database has never been sealed into. Both sites build the SELECT from `ParquetDatasetSelect` (`otlp_sql_util.hpp`) so the glob depth and the two load-bearing `read_parquet` options cannot drift — `hive_partitioning=false` because the COPY sets `WRITE_PARTITION_COLUMNS false` (year/month/day live only in the path, and inferring them appends three columns no catalog mode has), `union_by_name=true` so a schema change still reads. Registration is scoped to the signals the caller could read — the resolved `--signal` list for `export`, the signal names appearing in the SQL for `query` — because each probe is a directory listing, and a remote LIST apiece against an `s3://` root. A read-only database cannot hold a view, so that path falls back to session-scoped `TEMP VIEW`s.
  - **`--mode none`**: attaches nothing, installs nothing, sets no catalog — the destination is entirely `--init-sql` plus `--catalog`. Every mode contributes only setup SQL and a catalog name (see `BootSql`), both already expressible that way, but without an opt-out an operator's script was layered on top of an unwanted `local-ducklake` ATTACH. The eight presets are a hand-picked subset of (3 catalog kinds × 2 catalog stores × 4 storage backends); `none` is how an uncovered combination (S3 data files with a Postgres catalog, say) is reached without adding a ninth.
  - **Credentials may come from DuckDB's secret store**: a mode emits `CREATE OR REPLACE SECRET` for object storage *only* when a key pair is in the environment. With none set it emits nothing, so a `CREATE PERSISTENT SECRET` in `$HOME/.duckdb/stored_secrets` (`--secret-dir` / `DUCKDB_OTLP_SECRET_DIR` relocates it) is picked up automatically — emitting the secret unconditionally would shadow the stored one with an empty credential. Half a key pair is a hard error rather than a fall-through, since silently ignoring the half that IS set hides the typo until the first seal fails. The startup banner names the resolved source (`Credentials: ...`). `secret_directory` is emitted first in `BootSql`, before anything can initialize the secret manager. The container's `HOME=/duckdb-home` is where a mounted secret directory lands.
  - **AWS credentials resolve identically in every AWS mode**: `aws-ducklake` previously hardcoded `CHAIN instance` while `parquet`/`s3-tables` used `BuildCredentialChainSecret` (`CHAIN config` + `PROFILE`, else `CHAIN env`), so `AWS_PROFILE` worked in two modes and was silently ignored in the third. All three now share the builder.
  - **Custom DuckDB setup (`--init-sql` / `DUCKDB_OTLP_INIT_SQL`)**: a SQL script run after mode setup and before ingest starts — the escape hatch for DuckDB configuration the modes do not model (extra `ATTACH`, `SET`, secrets, views). Accepted by `serve`, `validate`, `export`, and `query` (the `INIT_SQL_CMDS` mask in `cli.cpp`); **not** by `doctor`, because the container `HEALTHCHECK` runs it on every probe and a liveness check must not execute operator SQL, nor by `convert`, which opens no catalog. Resolved to *contents* in `ServerConfig::FromEnv` (via `ReadSqlFile`) rather than kept as a path, so `validate` both rejects an unreadable script and prints it inside the generated boot SQL; `doctor` never calls `FromEnv`, so the probe never reads it. Failures are fatal. It runs before `SetDefaultDatabase`, so a catalog the script attaches can still be named by `--catalog`.
  - **Second destination is a second server, never a tee**: one server has exactly one ingest target (`otlp_serve` rejects `parquet_export_path` combined with a catalog, and the seal path is `SealParquet` XOR `SealCatalog`), so fanning out to both a catalog and a Parquet directory means a second `otlp_serve` on its own port from the `--init-sql` script. That is fan-out **by port** — the exporter picks the destination — and there is deliberately no way to mirror one stream into two targets, because one target has one writer is what keeps a single sealer per catalog.
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
