# Documentation

Welcome to the DuckDB OTLP Extension documentation. Find what you need:

- **[Get Started](get-started.md)** - Install the extension, read sample data, and POST one OTLP log over HTTP
- **[Main README](../README.md)** - Short overview and quickstart
- **[Live Ingest Quickstart](quickstart/serve.md)** - Run the OTLP HTTP server and stream live exports into a DuckLake lakehouse (Parquet + catalog), buffered and group-committed

## Guides

- **[Analyzing Traces](guides/analyzing-traces.md)** - Find slow requests, trace bottlenecks, analyze service dependencies
- **[Filtering Logs](guides/filtering-logs.md)** - Debug production issues by querying log exports
- **[Working with Metrics](guides/working-with-metrics.md)** - Query gauge, sum, histogram, and exponential histogram metrics
- **[How-to Guides](guides/README.md)** - Task-oriented guides for traces, logs, metrics, Parquet, dashboards, and errors

## Reference

- **[API](reference/api.md)** - File readers and live ingest functions
- **[Schemas](reference/schemas.md)** - Complete schema reference for traces, logs, gauges, sums, histograms, and exponential histograms
- **[Live Ingest Server](reference/serve.md)** - `otlp_serve` / `otlp_flush` / `otlp_stop` / `otlp_server_list`, catalog targeting (DuckLake), HTTP endpoints, auth, and the buffered seal/durability model
- **[Performance Tips](reference/performance.md)** - Short guidance for files, Parquet, globs, and live ingest

## Setup

- **[Installation](setup/installation.md)** - Install from community or build from source
- **[OpenTelemetry Collector](setup/collector.md)** - Configure collector to export OTLP files
- **[Sample Data](setup/sample-data.md)** - Download or generate test data
- **[OpenTelemetry Demo Exports](setup/otel-demo.md)** - Optional real demo telemetry

## Advanced Topics

- **[Architecture](architecture.md)** - How the extension works internally
- **[Contributing](../CONTRIBUTING.md)** - Build, test, and contribute to the project

## Need Help?

- **Questions?** Check [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- **Found a bug?** [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
- **Want to contribute?** See [CONTRIBUTING.md](../CONTRIBUTING.md)

---

**Quick Links**: [Main README](../README.md) | [Get Started](get-started.md) | [Live Ingest](quickstart/serve.md) | [GitHub](https://github.com/smithclay/duckdb-otlp)
