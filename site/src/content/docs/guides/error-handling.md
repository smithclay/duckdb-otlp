---
title: "How to Handle Malformed Input"
---

The file readers fail fast on malformed OTLP input. Use the error message to identify file discovery problems, payload shape mismatches, file size limits, unsupported metric shapes, or live-ingest admission failures.

```sql
SELECT * FROM read_otlp_traces('traces.jsonl');
```

Use this behavior when you convert telemetry exports. It surfaces bad data during the query instead of hiding it in downstream results.

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

Check that the payload shape matches the reader:

- `read_otlp_traces` expects `resourceSpans`.
- `read_otlp_logs` expects `resourceLogs`.
- `read_otlp_metrics_*` expects `resourceMetrics`.

**Unsupported metric shape**

The extension raises an unsupported-function error for `read_otlp_metrics()` and `read_otlp_metrics_summary()`. Use a typed metric reader:

```sql
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');
SELECT * FROM read_otlp_metrics_sum('metrics.jsonl');
SELECT * FROM read_otlp_metrics_histogram('metrics.jsonl');
SELECT * FROM read_otlp_metrics_exp_histogram('metrics.jsonl');
```

**File is too large**

The readers limit each file to 100 MB to avoid memory exhaustion. Split large exports with the OpenTelemetry Collector file exporter rotation settings, or query smaller files through globs.

## Live Ingest Errors

For `otlp_serve`, the server returns HTTP errors as JSON. Common cases:

- `401` for missing or invalid tokens.
- `413` for bodies larger than `max_body_bytes`.
- `415` for unsupported content types or encodings.
- `503` when `max_buffered_bytes` backpressure rejects a request.

See the [Live Ingest Reference](../../reference/serve/#responses-and-status-codes) for the complete contract.

## Related

- [API Reference](../../reference/api/)
- [Live Ingest Reference](../../reference/serve/)
- [Schema Reference](../../reference/schemas/)
