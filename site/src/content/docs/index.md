---
title: "DuckDB OpenTelemetry Extension"
---

Start here for `duckdb-otlp`: installation, file readers, live ingest, the browser demo, and the exact schemas exposed by the extension.

## Start using duckdb-otlp

- **[Get Started](get-started/)** - install the extension, query bundled OTLP files, and POST one log over HTTP.
- **[WASM Demo](demo/)** - query OTLP JSON, JSONL, and protobuf samples in your browser.
- **[Live Ingest Quickstart](quickstart/serve/)** - POST one OTLP/HTTP log record and query it from DuckDB.

## Query and export telemetry

- **[How to analyze traces](guides/analyzing-traces/)** - find slow spans, errors, trace details, and service dependencies.
- **[How to filter logs](guides/filtering-logs/)** - query logs by severity, service, time, text, and trace correlation.
- **[How to work with metrics](guides/working-with-metrics/)** - query gauge, sum, histogram, and exponential histogram metrics.
- **[How to stream to DuckLake](guides/stream-to-ducklake/)** - land OTLP/HTTP rows in Parquet files tracked by DuckLake.
- **[How to stream to Amazon S3 Tables](guides/stream-to-s3-tables/)** - land OTLP/HTTP rows in Amazon S3 Tables as an Iceberg catalog.
- **[How to stream to Cloudflare R2 Data Catalog](guides/stream-to-r2-data-catalog/)** - land OTLP/HTTP rows in Cloudflare R2 Data Catalog as an Iceberg catalog.
- **[How to export telemetry to Parquet](guides/exporting-to-parquet/)** - write traces, logs, and metrics to reusable Parquet datasets.
- **[How to handle malformed input](guides/error-handling/)** - diagnose reader and ingest errors.

## Set up data sources

- **[How to install the extension](setup/installation/)** - install from community extensions or build locally.
- **[How to configure the OpenTelemetry Collector](setup/collector/)** - write OTLP file exports for DuckDB analysis.
- **[How to get sample data](setup/sample-data/)** - use bundled fixtures, download files, or generate your own telemetry.
- **[How to export the OpenTelemetry Demo](setup/otel-demo/)** - collect real demo telemetry as OTLP files.

## Reference

- **[API Reference](reference/api/)** - file readers and live ingest functions.
- **[Schema Reference](reference/schemas/)** - columns for traces, logs, gauges, sums, histograms, and exponential histograms.
- **[Live Ingest Reference](reference/serve/)** - `otlp_serve`, `otlp_flush`, `otlp_stop`, `otlp_server_list`, HTTP endpoints, auth, buffering, and durability.
- **[Performance Reference](reference/performance/)** - file formats, projection, file sizes, materialization, and live ingest notes.

## Project internals

- **[Architecture](architecture/)** - how the file readers, Arrow bridge, schema layer, and HTTP ingest server work.
- **[Contributing](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md)** - build, test, and contribute to the project.

## Need help?

- [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
