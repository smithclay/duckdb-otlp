---
title: "DuckDB OpenTelemetry Extension"
---

Start here for `duckdb-otlp`: installation, file readers, live ingest, the browser demo, and schema details.

## Start using duckdb-otlp

- **[Get Started](get-started/)** - install the extension, query bundled OTLP files, and POST one log over HTTP.
- **[WASM Demo](demo/)** - query OTLP JSON, JSONL, and protobuf samples in your browser.
- **[Live Ingest Quickstart](quickstart/serve/)** - POST one OTLP/HTTP log record and query it from DuckDB.

## Query and export telemetry

- **[How to analyze telemetry](guides/analyze-telemetry/)** - query traces, logs, and metrics with a few practical starting points.
- **[How to stream to local DuckLake](guides/stream-to-local-ducklake/)** - land OTLP/HTTP rows in local Parquet files tracked by DuckLake.
- **[How to stream to remote DuckLake](guides/stream-to-remote-ducklake/)** - land OTLP/HTTP rows in DuckLake with Neon Postgres metadata and R2 data files.
- **[How to stream to Parquet](guides/stream-to-parquet/)** - land OTLP/HTTP rows as partitioned Parquet files on disk or S3.
- **[How to stream to Amazon S3 Tables](guides/stream-to-s3-tables/)** - land OTLP/HTTP rows in Amazon S3 Tables as an Iceberg catalog.
- **[How to stream to Cloudflare R2 Data Catalog](guides/stream-to-r2-data-catalog/)** - land OTLP/HTTP rows in Cloudflare R2 Data Catalog as an Iceberg catalog.
- **[How to export telemetry to Parquet](guides/exporting-to-parquet/)** - write traces, logs, and metrics to reusable Parquet datasets.

## Set up data sources

- **[How to install the extension](setup/installation/)** - install from community extensions or build locally.
- **[How to configure the OpenTelemetry Collector](setup/collector/)** - write OTLP file exports for DuckDB analysis.
- **[How to point the OpenTelemetry Demo at local DuckLake](setup/otel-demo/)** - stream demo telemetry into a local DuckLake container.

## Reference

- **[API Reference](reference/api/)** - file readers and live ingest functions.
- **[Schema Reference](reference/schemas/)** - columns for traces, logs, gauges, sums, histograms, and exponential histograms.
- **[Live Ingest Reference](reference/serve/)** - `otlp_serve`, `otlp_flush`, `otlp_stop`, `otlp_server_list`, HTTP endpoints, auth, buffering, and durability.
- **[Error Reference](reference/error-handling/)** - file reader failures and live-ingest error responses.
- **[Performance Reference](reference/performance/)** - file formats, projection, file sizes, materialization, and live ingest notes.

## Project internals

- **[Architecture](architecture/)** - how the file readers, Arrow bridge, schema layer, and HTTP ingest server work.
- **[Contributing](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md)** - build, test, and contribute to the project.

## Need help?

- [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
