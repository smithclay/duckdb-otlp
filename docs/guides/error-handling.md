# How to Handle Malformed Input

The file readers fail fast on malformed OTLP input. Use the error message to decide whether the problem is file discovery, payload shape, file size, unsupported metric shape, or live-ingest admission.

```sql
SELECT * FROM read_otlp_traces('traces.jsonl');
```

Use this behavior when converting telemetry exports so bad data is visible immediately.

## Common Errors

**No files matched the path or glob**

```sql
SELECT * FROM read_otlp_logs('missing/*.jsonl');
-- No files found that match the pattern
```

Check the DuckDB working directory or use an absolute path.

**Malformed OTLP input**

```sql
SELECT * FROM read_otlp_traces('malformed.jsonl');
-- OTLP parse error on malformed.jsonl: ...
```

Validate that the payload shape matches the reader:

- `read_otlp_traces` expects `resourceSpans`.
- `read_otlp_logs` expects `resourceLogs`.
- `read_otlp_metrics_*` expects `resourceMetrics`.

**Unsupported metric shape**

`read_otlp_metrics()` and `read_otlp_metrics_summary()` are intentionally unsupported. Use a typed metric reader:

```sql
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');
SELECT * FROM read_otlp_metrics_sum('metrics.jsonl');
SELECT * FROM read_otlp_metrics_histogram('metrics.jsonl');
SELECT * FROM read_otlp_metrics_exp_histogram('metrics.jsonl');
```

**File is too large**

Individual file reads are limited to 100 MB to prevent memory exhaustion. Split large exports with the OpenTelemetry Collector file exporter rotation settings, or query smaller files through globs.

## Live Ingest Errors

For `otlp_serve`, HTTP errors are returned as JSON. Common cases are:

- `401` for missing or invalid tokens.
- `413` for bodies larger than `max_body_bytes`.
- `415` for unsupported content types or encodings.
- `503` when `max_buffered_bytes` backpressure rejects a request.

See the [Live Ingest Reference](../reference/serve.md#responses-and-status-codes) for the complete contract.

## See Also

- [API Reference](../reference/api.md)
- [Live Ingest Reference](../reference/serve.md)
- [Schema Reference](../reference/schemas.md)
