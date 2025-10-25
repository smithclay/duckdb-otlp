# Duckspan Quickstart

This quickstart walks through collecting OpenTelemetry data with the OpenTelemetry Collector file exporter and exploring the resulting OTLP files with Duckspan in DuckDB.

## Prerequisites

- DuckDB 0.10+ with the Duckspan (`otlp`) extension built or installed.
- The OpenTelemetry Collector (either `otelcol` or `otelcol-contrib`).
- An application that can send OTLP data to the collector (any OpenTelemetry SDK, demo app, or `otel-cli`).

## 1. Configure the collector

Create `collector.yaml` with a file exporter that writes OTLP payloads. JSON encoding keeps the files human-readable and works with Duckspan out of the box.

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

Notes:
- The collector creates the `otel-export/` directory and rotates files when they reach the configured size or age.
- Switch `encoding: proto` to emit protobuf (`.pb`) files; Duckspan auto-detects both formats.

## 2. Run the collector

Start the collector with the configuration above:

```bash
otelcol-contrib --config collector.yaml
```

Leave the process running while you generate telemetry. Any OTLP-capable client pointed at `localhost:4317` (gRPC) or `localhost:4318` (HTTP) will now produce files under `./otel-export/`.

## 3. Generate sample telemetry

Use any OpenTelemetry-enabled service or a demo client. For example, with [`otel-cli`](https://github.com/equinix-labs/otel-cli):

```bash
otel-cli span \
  --service "checkout" \
  --name "place-order" \
  --tp-endpoint http://localhost:4318/v1/traces \
  --start  \
  --end
```

Repeat for logs and metrics if desired. The collector writes combined JSONL files such as `telemetry.jsonl`, or rotated variants like `telemetry-00001.jsonl`.

## 4. Explore the export with Duckspan

Launch DuckDB and load the extension:

```sql
LOAD otlp;
```

Query traces, logs, and metrics directly from the exported files:

```sql
-- Inspect the latest traces
SELECT TraceId, SpanName, Duration
FROM read_otlp_traces('otel-export/telemetry.jsonl')
ORDER BY Timestamp DESC
LIMIT 20;

-- Filter logs by severity
SELECT Timestamp, SeverityText, Body
FROM read_otlp_logs('otel-export/telemetry.jsonl')
WHERE SeverityText IN ('ERROR', 'FATAL');

-- Work with metrics using the helper scans
SELECT Timestamp, MetricName, Value
FROM read_otlp_metrics_gauge('otel-export/telemetry.jsonl')
WHERE MetricName LIKE 'http.server.duration%';
```

Duckspan automatically loads multiple files via globbing:

```sql
SELECT COUNT(*) FROM read_otlp_traces('otel-export/*.jsonl');
```

## 5. Next steps

- Convert the export to Parquet and reload it later—see the [cookbook](../cookbook/README.md#export-telemetry-to-parquet) for a worked example.
- Review the [schema reference](../schema/README.md) to understand every column emitted by the table functions.
- Automate builds and testing with the commands documented in the [repository README](../../README.md).
