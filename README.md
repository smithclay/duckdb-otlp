# Duckspan

Duckspan (extension name `otlp`) is a DuckDB extension that streams OpenTelemetry Protocol (OTLP) files into strongly-typed tables that mirror the ClickHouse exporter schema. Use it to explore traces, logs, and metrics with familiar SQL.

## Key Features

- Table functions for each OTLP signal: `read_otlp_traces`, `read_otlp_logs`, `read_otlp_metrics`
- Automatic format detection for JSON/JSONL and protobuf exports
- Optional helper scans for metrics (`read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}`)
- Works with DuckDB’s file system layer: local files, object storage, HTTP(s), Azure, and GCS
- `on_error` controls (`fail`, `skip`, `nullify`) and scan diagnostics via `read_otlp_scan_stats`

## Documentation

- [Quickstart](docs/quickstart/README.md) – Collect OTLP telemetry with the OpenTelemetry Collector file exporter and explore it with Duckspan.
- [Cookbook](docs/cookbook/README.md) – End-to-end recipes, including exporting telemetry to Parquet and reloading it with DuckDB.
- [Schema Reference](docs/schema/README.md) – Detailed column layouts for traces, logs, metrics, and helper table functions.
- [Updating Duckspan](docs/UPDATING.md) – Guidance for tracking new DuckDB releases.

## Try It

```sql
-- Load the extension inside DuckDB
LOAD otlp;

-- Inspect telemetry from an OTLP export
SELECT TraceId, SpanName, Duration
FROM read_otlp_traces('test/data/traces.jsonl')
WHERE Duration >= 1000000000
ORDER BY Duration DESC
LIMIT 10;
```

## Table Functions

| Function | Description |
| --- | --- |
| `read_otlp_traces(path, …)` | Streams trace spans with identifiers, scope metadata, attributes, events, and links. |
| `read_otlp_logs(path, …)` | Reads log records with severity, body, resource/scope attributes, and trace correlation IDs. |
| `read_otlp_metrics(path, …)` | Returns the 27-column union schema covering gauge, sum, histogram, exponential histogram, and summary metrics. |
| `read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}(path, …)` | Projects the union schema into typed layouts for each metric shape. |
| `read_otlp_options()` / `read_otlp_scan_stats()` | Discover named parameters and review parser diagnostics for the current connection. |

See the [cookbook](docs/cookbook/README.md) for usage patterns and `CREATE TABLE AS SELECT` examples.

## Build & Test

Duckspan relies on [vcpkg](https://github.com/microsoft/vcpkg) for dependency management. Set the toolchain path before building.

```bash
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Build with Ninja for faster incremental builds:

```bash
GEN=ninja make
```

Produced artifacts:
- `./build/release/duckdb` – DuckDB shell with Duckspan preloaded
- `./build/release/test/unittest` – C++ unit test runner
- `./build/release/extension/duckspan/duckspan.duckdb_extension` – Loadable extension binary

Run the SQL-based regression tests:

```bash
make test
```

## Development Workflow

```bash
# Apply formatting checks
make format-check

# Auto-format C++ and Python sources
make format-fix

# Run clang-tidy
GEN=ninja make tidy-check

# Execute pre-commit hooks with uv
uvx --from pre-commit pre-commit run --all-files
```

## Architecture Highlights

- **Format Detector**: Sniffs the beginning of a stream to choose between JSON and protobuf parsing.
- **Parsers**: Streaming JSON reader and protobuf reader feed shared row builders.
- **Row Builders**: Populate DuckDB vectors to match the ClickHouse-compatible schemas.
- **Schema Helpers**: `schema/` defines column layouts for traces, logs, metrics, and typed metric helpers.

## Limitations

- Focused on file-based telemetry ingestion; live OTLP gRPC streaming is out of scope.
- Protobuf support requires the protobuf runtime for each build target (native and WASM).
- Large protobuf files are processed in chunks; extremely large datasets may require additional staging.

## References

- [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/)
- [OpenTelemetry ClickHouse Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
- [DuckDB Extensions Overview](https://duckdb.org/docs/extensions/overview)
