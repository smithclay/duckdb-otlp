# Documentation

Welcome to the DuckDB OTLP Extension documentation. Find what you need:

## 🚀 Getting Started

**New to the extension?**

→ **[Get Started in 3 Minutes](get-started.md)** - Install the extension, load sample data, run your first query

**Want to see it in action?**

→ **[Main README](../README.md)** - Quick examples and use cases

## 📚 Guides

Task-based tutorials with real-world examples:

### Getting Telemetry Data

- **[OTel Collector Demo Exports](guides/otel-collector-demo-exports.md)** - Export OTLP files from the OpenTelemetry demo

### Analyzing Data

- **[Analyzing Traces](guides/analyzing-traces.md)** - Find slow requests, trace bottlenecks, analyze service dependencies
- **[Filtering Logs](guides/filtering-logs.md)** - Debug production issues by querying log exports
- **[Working with Metrics](guides/working-with-metrics.md)** - Query gauge, sum, histogram, and summary metrics

### Data Management

- **[Exporting to Parquet](guides/exporting-to-parquet.md)** - Archive telemetry for long-term storage
- **[Building Dashboards](guides/building-dashboards.md)** - Create custom metrics dashboards and visualizations
- **[Error Handling](guides/error-handling.md)** - Handle malformed data with `on_error` and scan diagnostics

## 📖 Reference

Detailed technical documentation:

### API

- **[Table Functions](reference/api.md)** - Function signatures, parameters, return types
- **[Error Handling](reference/error-handling.md)** - `on_error` modes and `read_otlp_scan_stats()`

### Schemas

- **[Traces Schema](reference/traces-schema.md)** - 22 columns for trace spans
- **[Logs Schema](reference/logs-schema.md)** - 15 columns for log records
- **[Metrics Schema](reference/metrics-schema.md)** - Union schema and typed helpers

### Advanced

- **[Type System](reference/type-system.md)** - DuckDB type mappings, conversions, gotchas
- **[Performance Tips](reference/performance.md)** - Optimize queries and scans

## ⚙️ Setup

Installation and configuration:

- **[Installation](setup/installation.md)** - Install from community or build from source
- **[OpenTelemetry Collector](setup/collector.md)** - Configure collector to export OTLP files
- **[Sample Data](setup/sample-data.md)** - Download or generate test data

## 🏗️ Advanced Topics

- **[Architecture](architecture.md)** - How the extension works internally
- **[ClickHouse Compatibility](clickhouse-compatibility.md)** - Schema mapping and migration guide
- **[Contributing](../CONTRIBUTING.md)** - Build, test, and contribute to the project

## 💡 Common Tasks

**I want to...**

- **Get sample OTLP data** → See [OTel Collector Demo Exports](guides/otel-collector-demo-exports.md)
- **Query trace spans** → See [Analyzing Traces](guides/analyzing-traces.md)
- **Filter logs by severity** → See [Filtering Logs](guides/filtering-logs.md)
- **Export to Parquet** → See [Exporting to Parquet](guides/exporting-to-parquet.md)
- **Build metrics tables** → See [Working with Metrics](guides/working-with-metrics.md)
- **Handle parsing errors** → See [Error Handling](guides/error-handling.md)
- **Understand schemas** → See [Reference](reference/)

## ❓ Need Help?

- **Questions?** Check [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- **Found a bug?** [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
- **Want to contribute?** See [CONTRIBUTING.md](../CONTRIBUTING.md)

---

**Quick Links**: [Main README](../README.md) | [Get Started](get-started.md) | [GitHub](https://github.com/smithclay/duckdb-otlp)
