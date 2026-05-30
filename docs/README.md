# DuckDB OpenTelemetry Extension

Start here for `duckdb-otlp`: installation, file readers, live ingest, the browser demo, and the exact schemas exposed by the extension.

## Start using duckdb-otlp

- **[Get Started](get-started.md)** - install the extension, query bundled OTLP files, and POST one log over HTTP.
- **[WASM Demo](demo.md)** - query OTLP JSON, JSONL, and protobuf samples in your browser.
- **[Live Ingest Quickstart](quickstart/serve.md)** - POST one OTLP/HTTP log record and query it from DuckDB.

## Query and export telemetry

- **[How to analyze traces](guides/analyzing-traces.md)** - find slow spans, errors, trace details, and service dependencies.
- **[How to filter logs](guides/filtering-logs.md)** - query logs by severity, service, time, text, and trace correlation.
- **[How to work with metrics](guides/working-with-metrics.md)** - query gauge, sum, histogram, and exponential histogram metrics.
- **[How to stream to DuckLake](guides/stream-to-ducklake.md)** - land OTLP/HTTP rows in Parquet files tracked by DuckLake.
- **[How to stream to Iceberg](guides/stream-to-iceberg.md)** - land OTLP/HTTP rows in an attached Iceberg REST catalog.
- **[How to export telemetry to Parquet](guides/exporting-to-parquet.md)** - write traces, logs, and metrics to reusable Parquet datasets.
- **[How to handle malformed input](guides/error-handling.md)** - diagnose reader and ingest errors.

## Set up data sources

- **[How to install the extension](setup/installation.md)** - install from community extensions or build locally.
- **[How to configure the OpenTelemetry Collector](setup/collector.md)** - write OTLP file exports for DuckDB analysis.
- **[How to get sample data](setup/sample-data.md)** - use bundled fixtures, download files, or generate your own telemetry.
- **[How to export the OpenTelemetry Demo](setup/otel-demo.md)** - collect real demo telemetry as OTLP files.

## Reference

- **[API Reference](reference/api.md)** - file readers and live ingest functions.
- **[Schema Reference](reference/schemas.md)** - columns for traces, logs, gauges, sums, histograms, and exponential histograms.
- **[Live Ingest Reference](reference/serve.md)** - `otlp_serve`, `otlp_flush`, `otlp_stop`, `otlp_server_list`, HTTP endpoints, auth, buffering, and durability.
- **[Performance Reference](reference/performance.md)** - file formats, projection, file sizes, materialization, and live ingest notes.

## Project internals

- **[Architecture](architecture.md)** - how the file readers, Arrow bridge, schema layer, and HTTP ingest server work.
- **[Contributing](../CONTRIBUTING.md)** - build, test, and contribute to the project.

## Need help?

- [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
