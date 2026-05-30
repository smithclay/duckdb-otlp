# Documentation

Welcome to the DuckDB OTLP Extension documentation. Find what you need:

## 🚀 Getting Started

**New to the extension?**

→ **[Get Started in 3 Minutes](get-started.md)** - Install the extension, load sample data, run your first query

**Want to see it in action?**

→ **[Main README](../README.md)** - Quick examples and use cases

**Want to stream live telemetry in?**

→ **[Live Ingest Quickstart](quickstart/serve.md)** - Run the OTLP HTTP server and POST exports straight into DuckDB tables

## 📚 Guides

Task-based tutorials with real-world examples:

### Getting Telemetry Data

- **[OTel Collector Demo Exports](guides/otel-collector-demo-exports.md)** - Export OTLP files from the OpenTelemetry demo

### Analyzing Data

- **[Analyzing Traces](guides/analyzing-traces.md)** - Find slow requests, trace bottlenecks, analyze service dependencies
- **[Filtering Logs](guides/filtering-logs.md)** - Debug production issues by querying log exports
- **[Working with Metrics](guides/working-with-metrics.md)** - Query gauge, sum, histogram, and summary metrics

### Data Management

- **[Cookbook](guides/cookbook.md)** - Common recipes and patterns

## 📖 Reference

Detailed technical documentation:

### API

- **[Table Functions](reference/api.md)** - Function signatures and parameters
- **[Schemas](reference/schemas.md)** - Complete schema reference for traces (25 columns), logs (15 columns), and metrics (16-18 columns)
- **[Live Ingest Server](reference/serve.md)** - `otlp_serve` / `otlp_stop` / `otlp_server_list`, HTTP endpoints, auth, and concurrency

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

- **Get sample OTLP data** → See [Sample Data](setup/sample-data.md)
- **Query trace spans** → See [Cookbook](guides/cookbook.md)
- **Filter logs by severity** → See [Cookbook](guides/cookbook.md)
- **Export to Parquet** → See [Cookbook](guides/cookbook.md)
- **Build metrics tables** → See [Cookbook](guides/cookbook.md)
- **Understand schemas** → See [Schema Reference](reference/schemas.md)

## ❓ Need Help?

- **Questions?** Check [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- **Found a bug?** [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
- **Want to contribute?** See [CONTRIBUTING.md](../CONTRIBUTING.md)

---

**Quick Links**: [Main README](../README.md) | [Get Started](get-started.md) | [GitHub](https://github.com/smithclay/duckdb-otlp)
