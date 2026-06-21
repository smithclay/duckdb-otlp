---
title: "Error Reference"
description: "Errors returned for file discovery, malformed payloads, unsupported metric shapes, file limits, and live-ingest admission failures."
---

The file readers fail fast on malformed OTLP input. Errors identify file discovery problems, payload shape mismatches, file size limits, and unsupported metric shapes. Live ingest reports authentication, request, and backpressure failures through HTTP or gRPC status codes.

## File reader errors

### No files matched the path or glob

```sql
SELECT * FROM read_otlp_logs('missing/*.jsonl');
-- No files found that match the pattern
```

The path is resolved from DuckDB's working directory. An absolute path avoids working-directory ambiguity.

### Malformed OTLP input

```sql
SELECT * FROM read_otlp_traces('malformed.jsonl');
-- OTLP parse error on malformed.jsonl: ...
```

The top-level payload field must match the reader:

| Reader | Required payload field |
| --- | --- |
| `read_otlp_traces` | `resourceSpans` |
| `read_otlp_logs` | `resourceLogs` |
| `read_otlp_metrics_*` | `resourceMetrics` |

### Unsupported metric shape

`read_otlp_metrics()` and `read_otlp_metrics_summary()` raise an unsupported-function error. The supported typed metric readers are:

```sql
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');
SELECT * FROM read_otlp_metrics_sum('metrics.jsonl');
SELECT * FROM read_otlp_metrics_histogram('metrics.jsonl');
SELECT * FROM read_otlp_metrics_exp_histogram('metrics.jsonl');
```

### File is too large

Each file is limited to 100 MB to bound memory use. Larger exports must be split, for example with the OpenTelemetry Collector file exporter's rotation settings, and can then be queried through a glob.

## Live ingest errors

`otlp_serve` returns HTTP errors as JSON. Common responses are:

| Status | Condition |
| --- | --- |
| `401 Unauthorized` | The bearer token is missing or invalid. |
| `413 Content Too Large` | The request body exceeds `max_body_bytes`. |
| `415 Unsupported Media Type` | The content type or content encoding is unsupported. |
| `503 Service Unavailable` | `max_buffered_bytes` backpressure rejects the request. |

The [Live Ingest Reference](../serve/#responses-and-status-codes) defines the complete HTTP and gRPC error contract.

## Related

- [API Reference](../api/)
- [Live Ingest Reference](../serve/)
- [Schema Reference](../schemas/)
