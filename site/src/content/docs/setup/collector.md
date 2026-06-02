---
title: "How to Configure the OpenTelemetry Collector"
---

Use the OpenTelemetry Collector to route telemetry from applications to `duckdb-otlp`. You can forward OTLP/HTTP to the server image, or write OTLP files for later DuckDB analysis.

## Forward to the Server Image

Start a local DuckLake-backed server:

```bash
cat > .env <<'EOF'
DUCKDB_MODE=local-ducklake
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKLAKE_NAME=lake
DUCKLAKE_CATALOG_PATH=/data/ducklake/catalog.duckdb
DUCKLAKE_DATA_PATH=/data/ducklake/storage

DUCKDB_QUACK_ENABLED=1
DUCKDB_QUACK_ADDR=0.0.0.0:9494
DUCKDB_QUACK_TOKEN=dev-quack-token-123456
EOF

mkdir -p data

docker run --rm --name duckdb-otlp \
  --env-file .env \
  -p 4318:4318 \
  -p 9494:9494 \
  -v "$(pwd)/data:/data" \
  ghcr.io/smithclay/duckdb-otlp:latest
```

In another terminal, create `collector-to-duckdb.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:

exporters:
  otlp_http/duckdb:
    endpoint: http://localhost:4318
    headers:
      Authorization: Bearer dev-token-123456

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp_http/duckdb]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp_http/duckdb]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp_http/duckdb]
```

Run the collector:

```bash
otelcol-contrib --config collector-to-duckdb.yaml
```

Point applications at the collector's OTLP endpoints:

- OTLP/gRPC: `http://localhost:4317`
- OTLP/HTTP: `http://localhost:4318`

The collector forwards traces, logs, and metrics to `duckdb-otlp`, which writes them into DuckLake tables.

If the collector itself runs in Docker, use `http://host.docker.internal:4318` for the exporter endpoint. On Linux, add `host.docker.internal` with Docker's `host-gateway` support.

## Query Forwarded Telemetry

Flush buffered telemetry and query through the running `duckdb-otlp` container:

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

FROM quack_query(
  'quack:localhost:9494',
  'SELECT * FROM otlp_flush(''otlp:0.0.0.0:4318'')',
  token = 'dev-quack-token-123456'
);

FROM quack_query(
  'quack:localhost:9494',
  $$
  SELECT service_name, name, count(*) AS spans
  FROM lake.main.otlp_traces
  GROUP BY service_name, name
  ORDER BY spans DESC
  LIMIT 20
  $$,
  token = 'dev-quack-token-123456'
);
SQL
```

The server image is distroless and has no shell or DuckDB CLI, so run inspection SQL from a host DuckDB process through Quack instead of `docker exec ... sh -c`.

## Write OTLP Files Instead

Use the file exporter when you want files that DuckDB can query without a running server.

Create `collector-to-files.yaml`:

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
otelcol-contrib --config collector-to-files.yaml
```

The collector listens on the OTLP ports and writes JSONL files under `./otel-export/`.

To generate protobuf files, switch the exporter to `encoding: proto`.

## Query Exported Files

```sql
INSTALL otlp FROM community;
LOAD otlp;

SELECT trace_id, name, duration_time_unix_nano
FROM read_otlp_traces('otel-export/*.jsonl')
ORDER BY start_time_unix_nano DESC
LIMIT 20;

SELECT time_unix_nano, severity_text, body
FROM read_otlp_logs('otel-export/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL');

SELECT time_unix_nano, name, coalesce(double_value, int_value::DOUBLE) AS value
FROM read_otlp_metrics_gauge('otel-export/*.jsonl');
```

## Next Steps

- [Get Started](../../get-started/)
- [Stream to Local DuckLake](../../guides/stream-to-local-ducklake/)
- [Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/)
- [Stream to Parquet](../../guides/stream-to-parquet/)
- [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/)
- [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/)
- [Schema Reference](../../reference/schemas/)
