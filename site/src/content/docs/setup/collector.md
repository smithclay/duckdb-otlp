---
title: "How to Configure the OpenTelemetry Collector"
---

Use the OpenTelemetry Collector when you want a standard pipeline that receives OTLP from applications and writes OTLP files for DuckDB analysis.

For direct HTTP ingestion into DuckDB, DuckLake, Amazon S3 Tables, or Cloudflare R2 Data Catalog, see the [Live Ingest Quickstart](../../quickstart/serve/), [Stream to DuckLake](../../guides/stream-to-ducklake/), [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/), and [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/).

## File Exporter Configuration

Create `collector.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:

exporters:
  file/otel-json:
    path: ./otel-export/telemetry.jsonl
    rotation:
      max_megabytes: 50
      max_days: 1
    encoding: json

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/otel-json]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/otel-json]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [file/otel-json]
```

Run it:

```bash
otelcol-contrib --config collector.yaml
```

The collector listens on the standard OTLP ports: `4317` for gRPC and `4318` for HTTP. It writes JSONL files under `./otel-export/`.

To generate protobuf files, switch the exporter to `encoding: proto`.

## Query Exported Files

```sql
INSTALL otlp FROM community;
LOAD otlp;

SELECT trace_id, span_name, duration
FROM read_otlp_traces('otel-export/*.jsonl')
ORDER BY timestamp DESC
LIMIT 20;

SELECT timestamp, severity_text, body
FROM read_otlp_logs('otel-export/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL');

SELECT timestamp, metric_name, value
FROM read_otlp_metrics_gauge('otel-export/*.jsonl');
```

## Live Ingest Alternative

If you do not need collector-side processing, the extension can receive OTLP/HTTP directly:

```sql
SELECT listen_url
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

Point an OTLP/HTTP exporter at `http://localhost:4318` and set `Authorization: Bearer dev-token-123456`. See [Live Ingest Reference](../../reference/serve/) for endpoints, content types, auth, buffering, and durability.

## Next Steps

- [Get Started](../../get-started/)
- [Live Ingest Quickstart](../../quickstart/serve/)
- [Stream to DuckLake](../../guides/stream-to-ducklake/)
- [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/)
- [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/)
- [How to Get Sample Data](../sample-data/)
- [Schema Reference](../../reference/schemas/)
