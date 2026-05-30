# Documentation

Use this page to choose the document that matches what you need to do now.

## Learn by doing

- **[Get Started](get-started.md)** - install the extension, query bundled OTLP files, and POST one log over HTTP.
- **[Live Ingest Quickstart](quickstart/serve.md)** - stream OTLP/HTTP exports into a DuckLake lakehouse and query the sealed rows.

## Do a job

- **[How to analyze traces](guides/analyzing-traces.md)** - find slow spans, errors, trace details, and service dependencies.
- **[How to filter logs](guides/filtering-logs.md)** - query logs by severity, service, time, text, and trace correlation.
- **[How to work with metrics](guides/working-with-metrics.md)** - query gauge, sum, histogram, and exponential histogram metrics.
- **[How to export telemetry to Parquet](guides/exporting-to-parquet.md)** - write traces, logs, and metrics to reusable Parquet datasets.
- **[How to build dashboard tables](guides/building-dashboards.md)** - materialize compact rollups for BI and dashboards.
- **[How to handle malformed input](guides/error-handling.md)** - diagnose reader and ingest errors.
- **[How to install the extension](setup/installation.md)** - install from community extensions or build locally.
- **[How to configure the OpenTelemetry Collector](setup/collector.md)** - write OTLP file exports for DuckDB analysis.
- **[How to get sample data](setup/sample-data.md)** - use bundled fixtures, download files, or generate your own telemetry.
- **[How to export the OpenTelemetry Demo](setup/otel-demo.md)** - collect real demo telemetry as OTLP files.

## Look up facts

- **[API Reference](reference/api.md)** - file readers and live ingest functions.
- **[Schema Reference](reference/schemas.md)** - columns for traces, logs, gauges, sums, histograms, and exponential histograms.
- **[Live Ingest Reference](reference/serve.md)** - `otlp_serve`, `otlp_flush`, `otlp_stop`, `otlp_server_list`, HTTP endpoints, auth, buffering, and durability.
- **[Performance Reference](reference/performance.md)** - file formats, projection, file sizes, materialization, and live ingest notes.

## Understand the design

- **[Architecture](architecture.md)** - how the file readers, Arrow bridge, schema layer, and HTTP ingest server work.
- **[Contributing](../CONTRIBUTING.md)** - build, test, and contribute to the project.

## Need help?

- [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
