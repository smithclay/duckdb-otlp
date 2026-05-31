---
title: "API Reference"
---

This document provides an overview of the DuckDB OpenTelemetry Extension API. All file readers accept a path or glob pattern and auto-detect OTLP JSON, JSONL, and protobuf. Live ingest is available only in native builds.

## Table Functions

The extension provides the following table functions for reading OTLP data:

### Traces

**`read_otlp_traces(path)`**

Streams trace spans with identifiers, attributes, events, and links. See the [schema reference](../schemas/#traces-read_otlp_traces) for all 25 columns.

**Parameters:**
- `path` (VARCHAR): File path or glob pattern. DuckDB file systems provide local, S3, HTTP(S), Azure, and GCS access.

### Logs

**`read_otlp_logs(path)`**

Reads log records with severity, body, and trace correlation. See the [schema reference](../schemas/#logs-read_otlp_logs) for all 15 columns.

**Parameters:** Same as `read_otlp_traces`

### Metrics

**`read_otlp_metrics_gauge(path)`**

Returns gauge metrics (16 columns). See the [schema reference](../schemas/#gauge-metrics-read_otlp_metrics_gauge) for details.

**`read_otlp_metrics_sum(path)`**

Returns sum/counter metrics (18 columns) with `value`, `aggregation_temporality`, and `is_monotonic`. See the [schema reference](../schemas/#sum-metrics-read_otlp_metrics_sum) for details.

**`read_otlp_metrics_histogram(path)`**

Returns standard histogram metrics (22 columns) with counts, sum, min/max, explicit bucket bounds, and bucket counts. See the [schema reference](../schemas/#histogram-metrics-read_otlp_metrics_histogram) for details.

**`read_otlp_metrics_exp_histogram(path)`**

Returns exponential histogram metrics (27 columns) with scale, zero bucket, positive buckets, negative buckets, and aggregation temporality. See the [schema reference](../schemas/#exponential-histogram-metrics-read_otlp_metrics_exp_histogram) for details.

**Parameters:** Same as `read_otlp_traces`

> **Note**: `read_otlp_metrics(path)` and `read_otlp_metrics_summary(path)` are registered but intentionally unsupported. Use the shape-specific metric readers above.

## Live Ingest

In native builds, the extension can also run an HTTP server that accepts live OTLP/HTTP exports and streams them into the default DuckDB catalog or an attached writable catalog such as DuckLake or an Iceberg REST catalog. Ingest is buffered and committed in batches: a POST returns `202 Accepted`, and rows become durable at the next automatic background commit or on graceful stop. Current native builds commit automatically after about 5 seconds for the oldest buffered row, or when admitted request-body bytes reach about 64 MiB.

- **`otlp_serve([uri], catalog := '<attached_db>', ...)`** - Start the ingest server, target a catalog, and create/validate the target tables.
- **`otlp_flush(uri)`** - Optionally force a synchronous commit when readers need the latest accepted rows immediately.
- **`otlp_stop(uri)`** - Stop the server listening on `uri` (commits remaining rows first).
- **`otlp_server_list()`** - List running servers with live counters, buffer state, and health.

See the [Serve Reference](../serve/) for parameters, catalog targeting, endpoints, auth, and buffered commit behavior. For task-oriented walkthroughs, start with the [Live Ingest Quickstart](../../quickstart/serve/), [Stream to DuckLake](../../guides/stream-to-ducklake/), [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/), or [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/).

## Examples

For task-oriented examples, see the [How-to Guides](../../guides/).

For complete schema details, see the [Schema Reference](../schemas/).
