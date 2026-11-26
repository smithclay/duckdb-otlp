# Testing duckdb-otlp

Run these commands from the repository root. Build the release extension before running the SQL and C++ suites. For the daemon suite, build the receiver with `GEN=ninja make server-release` or point `DUCKDB_OTLP_SERVER_BIN` at an existing binary. See the [build instructions](../CONTRIBUTING.md).

| Suite | Coverage | Local command | CI |
| --- | --- | --- | --- |
| `test/sql` | Reader schemas, decoding, filters, and SQL server controls | `GEN=ninja make test` | `duckdb-stable-build` in the main distribution workflow |
| `test/cpp` | Admission, catalog and Parquet seals, retry, and shutdown | `GEN=ninja make test` | The native extension test jobs that build the seal harness, through `test_release` |
| `test/server` | Daemon CLI, configuration, HTTP and gRPC ingest, and shutdown | `DUCKDB_OTLP_SERVER_BIN="$PWD/build/release/extension/otlp/duckdb-otlp" uv run --with pytest --with grpcio --with opentelemetry-proto --with duckdb==2.0.0 pytest test/server -q` | `daemon-compile` in the main distribution workflow |
| `test/pages` | Published extension repository and CLI installer | `python3 -m unittest discover -s test/pages` | `site` in `site-check.yml` |
