# API Reference

This document provides an overview of the DuckDB OTLP extension API.

## Table Functions

The extension provides the following table functions for reading OTLP data:

### Traces

**`read_otlp_traces(path)`**

Streams trace spans with identifiers, attributes, events, and links. See the [schema reference](schemas.md#traces-read_otlp_traces) for all 25 columns.

**Parameters:**
- `path` (VARCHAR): File path or glob pattern (supports local, S3, HTTP, Azure, GCS)

### Logs

**`read_otlp_logs(path)`**

Reads log records with severity, body, and trace correlation. See the [schema reference](schemas.md#logs-read_otlp_logs) for all 15 columns.

**Parameters:** Same as `read_otlp_traces`

### Metrics

**`read_otlp_metrics_gauge(path)`**

Returns gauge metrics (16 columns). See the [schema reference](schemas.md#gauge-metrics-read_otlp_metrics_gauge) for details.

**`read_otlp_metrics_sum(path)`**

Returns sum/counter metrics (18 columns) with `value`, `aggregation_temporality`, and `is_monotonic`. See the [schema reference](schemas.md#sum-metrics-read_otlp_metrics_sum) for details.

**Parameters:** Same as `read_otlp_traces`

> **Note**: The following functions are not yet implemented:
> - `read_otlp_metrics(path)` - Union schema for all metric types
> - `read_otlp_metrics_histogram(path)` - Histogram metrics
> - `read_otlp_metrics_exp_histogram(path)` - Exponential histogram metrics
> - `read_otlp_metrics_summary(path)` - Summary metrics

## Examples

For practical examples and copy-paste-ready recipes, see the [Cookbook](../guides/cookbook.md).

For complete schema details, see the [Schema Reference](schemas.md).
