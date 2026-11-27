# AGENTS.md

## Build and release

- Use `GEN=ninja` for native `make` targets and `uv` for Python tooling. Initialize the pinned
  submodules with `git submodule update --init --recursive`. Build prerequisites and release
  procedures are in [CONTRIBUTING.md](CONTRIBUTING.md).
- Use Cargo with Rust 1.90 or later. Windows/MSVC builds the extension only. Keep the static
  Rust CRT and `/bigobj` settings. MinGW is excluded from the build matrix.
- Move the DuckDB `v2.0.0` pins together. Update the `duckdb` gitlink and release-line hint in
  `.gitmodules`, both `duckdb_version` inputs in `MainDistributionPipeline.yml` and `pages.yml`,
  the workflows' `DAEMON_DUCKDB_VERSION` and `DUCKDB_VERSION` respectively, both Dockerfile
  `DUCKDB_VERSION` defaults, and the `duckdb==2.0.0` daemon test pin in CI and `test/README.md`.
  The `extension-ci-tools` submodule, its branch hint, reusable workflow refs, and `ci_tools_version`
  follow the community tools line `v2.0-cyanoptera`. `description.yml` carries the separate
  `variegata` pin for v1.5.6 rebuilds.
- Call `otlp_link_duckdb_extensions(<target>)` for executables that embed DuckDB. The deferred
  call roots the resolved extension set. `Copy Function "parquet" is not in the catalog` is a
  symptom of missing linkage. `extension_config.cmake` explicitly links `otlp` into the shell and
  SQL test runner with `duckdb_extension_statically_link(otlp)`.

```sh
GEN=ninja make release                 # build/release/duckdb and extension/otlp/otlp.duckdb_extension
GEN=ninja make debug
GEN=ninja make server-release          # build/release/extension/otlp/duckdb-otlp
cmake --build build/release --target duckdb_otlp_server  # relink in a configured tree
GEN=ninja make test                    # SQL logic tests and, where built, the C++ seal harness
GEN=ninja make test_debug
build/release/test/unittest 'test/sql/otlp_serve.test'
GEN=ninja make format-check
GEN=ninja make format-fix
GEN=ninja make tidy-check
```

- `wasm_eh` is the published browser target. Follow the Emscripten and Rust instructions in
  [CONTRIBUTING.md](CONTRIBUTING.md). That target uses Make because it invokes `emmake make`.
  WASM includes file readers with uncompressed/LZ4 OTAP support. Zstandard OTAP and live ingest
  require native builds. The checked-in browser demo is built separately.
- Keep `deps` and `runtime-prebuilt` independent of `builder`. `runtime-prebuilt` consumes
  `daemon-export` artifacts. Check `readelf -d` `NEEDED` entries before adding runtime packages.
  The offline cache requires matching `HOME` and DuckDB versions. Bind-mounted `/data` must be
  writable by uid 65532. Keep container defaults in Dockerfile `ENV` and standalone defaults in C++.
- `daemon-linux` feeds `docker-smoke` on every distribution workflow run. `docker-smoke` runs the
  daemon suite and packages the amd64 binary for smoke tests. The publish jobs reuse the
  `duckdb-otlp` binary artifacts. Verify the release commit's `main` run before tagging and confirm
  the release with `gh release view <tag>` afterward.
- See [test/README.md](test/README.md) for suites and CI. Test behavior changes at the normal entry point.

## Extension and ingest invariants

- Register every SQL function through `OtlpDocumented()`. Give each overload its own description,
  put argument names in the signature, and declare named options as typed kwargs. Run the examples
  against the extension. `test/sql/function_metadata.test` checks the catalog metadata.
- `read_otlp_*` and `read_otap_*` expose the same snake_case schemas for logs, traces, and four
  metric shapes. Match the reader family to the wire format. DuckDB's multi-file wrapper handles
  paths, globs, file options, and cross-file parallelism. A single-file reader decodes lazily
  through `otlp2records` and imports Arrow with DuckDB's stock importer.
- Apply every pushed filter exactly in the DuckDB scan. Rust pruning ranges may include extra
  rows but must cover every match. When `file_row_number` is projected, send only the column
  projection to Rust and assign offsets before exact filtering. Regenerate
  `test/sql/read_otlp_filter_pushdown.test` with `scripts/gen_filter_pushdown_test.py` when changing
  pushdown coverage.
- Start native ingest with `otlp_serve` and `otlp:` URIs. A gRPC listener serves OTLP unary exports
  and OTAP/Arrow streams. Every listener on a server shares one target, admission budget, and sealer.
  A second destination requires a second server on its own port. `OtlpServerRegistry` groups listeners
  by server and allows one server per target (catalog and schema, or Parquet export root).
- The Rust server handles authentication, decompression and decoding on every listener. It lends
  one decoded message at a time to `OtlpServer::Ingest`, which reserves once, stages all Arrow
  batches, and buffers their rows together. Rust maps host refusals to wire statuses. A seal swaps
  the live `OtlpBuffers` under one mutex and hands them to the serialized writer.
- Acceptance means buffering. Durability begins at a successful seal. `otlp_flush` and `otlp_stop`
  take an optional listener URI. Omitting it applies the operation to every server. An unknown URI
  is an error. The control functions use a shared run-once scan and keep results in a
  `GlobalTableFunctionState`.
- Shut down the daemon with `otlp_stop()` to drain servers started through `--init-sql` or Quack
  while DuckDB can still commit. Regression coverage is in `test/server/test_server_transports.py`
  (`test_init_sql_adds_a_parquet_destination_next_to_the_ducklake_catalog`).
- See the [serve reference](site/src/reference/serve/index.md) for the SQL and transport contracts
  and the [reader reference](site/src/reference/api/index.md) for file formats and constraints.

## Daemon and presets

- `duckdb-otlp` is the live receiver. Use DuckDB with the extension for file reads and SQL queries.
  Keep `flag > environment > default` through the `EnvSource` overlay. `serve` executes the setup
  and listener startup SQL steps that `validate` prints. `doctor` probes listeners without loading
  a database or reading operator SQL.
- Declare daemon-passable listener tuning once in `OtlpServeTuningOptions()` through the extension's
  option tables. The daemon derives `DUCKDB_OTLP_<OPTION>` from that list. DuckLake compaction and
  retention belong to the three `set_option` calls in the DuckLake presets.
- Add a mode as a SQL file under `src/server/modes/` with minimal settings in `MODE_FIXTURES` in
  `test/server/test_server_config.py`. Follow the syntax at the top of `local-ducklake.sql`. Check
  required settings in the expander and let DuckDB validate SQL values at execution. Give each
  setting one canonical name. Use the provider's conventions (`AWS_*`, libpq `PG*`, `R2_*`, `S3_*`)
  and reserve `DUCKDB_OTLP_*` for daemon settings. Keep missing settings deduplicated in one error.
- Rewrite preset `getenv()` secrets to session variables through `PresetExpander`. Keep credentials
  out of generated SQL and bind them before executing setup. Set `secret_directory` before mode SQL.
  Keep the AWS chain multi-source (`env`, `config`, `instance`) and match `CREDENTIALS` to the
  sources used. Preserve the trailing slash on R2 DuckLake data prefixes and the persistent-secret
  fallback when neither R2 key is set.
- A preset owns its `INSTALL`/`LOAD` lines. The daemon embeds `otlp` statically, so never `INSTALL`
  it in a preset. Use `FROM community` for community extensions. Keep image cache coverage in sync
  with preset dependencies (`test_the_image_primes_every_extension_some_mode_needs`).
- See the [CLI reference](site/src/reference/cli/index.md) for flags, settings, modes, and Quack.

## Code and documentation map

| Path | Responsibility |
| --- | --- |
| `src/otlp_extension.cpp` | Extension entry point and registration |
| `src/otlp_function_docs.cpp` | SQL catalog descriptions, examples, and categories |
| `src/otlp_read.cpp`, `src/otlp_arrow.cpp` | Multi-file readers, decoder FFI, projection and filter pushdown, Arrow import |
| `external/otlp2records` | Rust decoding and embedded OTLP/HTTP, OTLP/gRPC and OTAP/gRPC server |
| `src/otlp_control.cpp`, `src/otlp_registry.cpp`, `src/otlp_uri.cpp` | SQL serve/stop/flush/list, per-database server ownership, URI parsing |
| `src/otlp_server.cpp`, `src/otlp_column_promote.cpp` | Rust listener handles, admission, live buffers, seal and maintenance, attribute promotion |
| `src/include/` | Extension headers, ingest limits, SQL helpers, and logging |
| `src/server/main.cpp`, `src/server/server_util.cpp` | Receiver startup, shutdown, doctor, and shared helpers |
| `src/server/cli.cpp`, `src/server/env_source.cpp` | Command-specific flags and environment overlay |
| `src/server/server_config.cpp`, `src/server/modes/*.sql` | Listener configuration, preset expansion, and SQL steps |
| `test/sql/`, `test/data/`, `test/cpp/`, `test/server/`, `test/pages/` | SQL regressions, fixtures, seal harness, daemon tests, and site tests |
| `docker/duckdb-otlp-server/Dockerfile`, `scripts/` | Runtime image, benchmarks, smoke tests, and site tooling |

- Follow Diátaxis and link to one canonical page per topic. Keep tutorials in the README and
  quickstarts, how-to guides in `site/src/guides/`, contracts in `site/src/reference/`, and design
  explanations in `site/src/architecture/`. Put task-oriented examples under the existing guides.
- Use plain Markdown and the pinned mdBook binary. Update the version and SHA-256 together in
  `.github/actions/setup-mdbook/action.yml`. Keep the site free of npm and preprocessors. Put each
  page at `site/src/<path>/index.md`, list it in `site/src/SUMMARY.md`, and run
  `python3 scripts/check_site_links.py <built-site-dir>`. Skip redirect pages and migration stubs
  unless requested.
- Keep `site/src/install.sh` at its published URL. Its release archive names and `SHA256SUMS`
  must match `daemon-release`. Preserve checksum verification and CLI-style flag precedence,
  exit codes, and stderr output. See [site/README.md](site/README.md) for site build and publishing.
