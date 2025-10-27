# API Reference

This document provides an overview of the DuckDB OTLP extension API.

## Table Functions

The extension provides the following table functions for reading OTLP data:

### Traces

**`read_otlp_traces(path, ...)`**

Streams trace spans with identifiers, attributes, events, and links. See the [schema reference](schemas.md#traces-read_otlp_traces) for all 22 columns.

**Parameters:**
- `path` (VARCHAR): File path or glob pattern (supports local, S3, HTTP, Azure, GCS)
- `on_error` (VARCHAR): Error handling mode - `'fail'` (default), `'skip'`, or `'nullify'`
- `max_document_bytes` (BIGINT): Max size per JSON/protobuf document (default: 100MB)

### Logs

**`read_otlp_logs(path, ...)`**

Reads log records with severity, body, and trace correlation. See the [schema reference](schemas.md#logs-read_otlp_logs) for all 15 columns.

**Parameters:** Same as `read_otlp_traces`

### Metrics

**`read_otlp_metrics(path, ...)`**

Queries metrics with a union schema covering all metric types (gauge, sum, histogram, exponential histogram, summary). Returns 27 columns. See the [schema reference](schemas.md#metrics-read_otlp_metrics) for details.

**Parameters:** Same as `read_otlp_traces`

### Metrics Helpers

Typed helpers that project specific metric shapes:

- **`read_otlp_metrics_gauge(path, ...)`** - Returns gauge metrics with `Value`
- **`read_otlp_metrics_sum(path, ...)`** - Returns sum/counter metrics with `Value`, `AggregationTemporality`, `IsMonotonic`
- **`read_otlp_metrics_histogram(path, ...)`** - Returns histogram metrics with `Count`, `Sum`, `BucketCounts`, `ExplicitBounds`, `Min`, `Max`
- **`read_otlp_metrics_exp_histogram(path, ...)`** - Returns exponential histogram metrics
- **`read_otlp_metrics_summary(path, ...)`** - Returns summary metrics with `Count`, `Sum`, `QuantileValues`, `QuantileQuantiles`

All helpers accept the same parameters as `read_otlp_metrics`.

## Diagnostic Functions

**`read_otlp_options()`**

Returns a table of all available configuration options with their default values and descriptions.

```sql
SELECT * FROM read_otlp_options();
```

**`read_otlp_scan_stats()`**

Returns scan statistics for the current connection, including:
- Number of records scanned
- Parse errors encountered
- Format detection results
- Skipped/nullified rows

```sql
SELECT * FROM read_otlp_scan_stats();
```

## Configuration Options

### Error Handling

Control how the extension handles malformed or invalid OTLP data:

```sql
-- Default: fail on parse errors
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Skip invalid records and continue processing
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'skip');

-- Emit NULL rows for invalid records (preserves row count)
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'nullify');
```

### Size Limits

Individual JSON/Protobuf documents are limited to **100 MB by default** to prevent memory exhaustion:

```sql
-- Use default 100MB limit
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Override for larger documents (value in bytes)
SELECT * FROM read_otlp_traces('huge_traces.jsonl', max_document_bytes := 500000000);

-- Combine with error handling
SELECT * FROM read_otlp_metrics('metrics.pb',
                                max_document_bytes := 200000000,
                                on_error := 'skip');
```

## Examples

For practical examples and copy-paste-ready recipes, see the [Cookbook](../guides/cookbook.md).

For complete schema details, see the [Schema Reference](schemas.md).
