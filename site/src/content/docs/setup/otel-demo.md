---
title: "How to Point the OpenTelemetry Demo at Local DuckLake"
---

Run the `duckdb-otlp` server image in `local-ducklake` mode and send OpenTelemetry Demo traces, logs, and metrics to it over OTLP/HTTP.

The demo keeps its existing observability backends. You add `duckdb-otlp` as one more collector exporter.

## Start the Local DuckLake Server

Create `.env`:

```ini
DUCKDB_MODE=local-ducklake
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKLAKE_NAME=lake
DUCKLAKE_CATALOG_PATH=/data/ducklake/catalog.duckdb
DUCKLAKE_DATA_PATH=/data/ducklake/storage

DUCKDB_QUACK_ENABLED=1
DUCKDB_QUACK_ADDR=0.0.0.0:9494
DUCKDB_QUACK_TOKEN=dev-quack-token-123456
```

Start the server:

```bash
mkdir -p data

docker run --rm --name duckdb-otlp \
  --env-file .env \
  -p 4318:4318 \
  -p 9494:9494 \
  -v "$(pwd)/data:/data" \
  ghcr.io/smithclay/duckdb-otlp:latest
```

Leave this container running. It accepts OTLP/HTTP at `http://localhost:4318`.

## Configure the OpenTelemetry Demo Collector

In another terminal, clone the OpenTelemetry Demo:

```bash
git clone https://github.com/open-telemetry/opentelemetry-demo.git
cd opentelemetry-demo
```

Create `src/otel-collector/otelcol-config-extras.yml`:

```yaml
exporters:
  otlp_http/duckdb:
    endpoint: http://host.docker.internal:4318
    headers:
      Authorization: Bearer dev-token-123456

service:
  pipelines:
    traces:
      exporters: [otlp, debug, spanmetrics, otlp_http/duckdb]
    logs:
      exporters: [opensearch, debug, otlp_http/duckdb]
    metrics:
      exporters: [otlp_http/prometheus, debug, otlp_http/duckdb]
```

The OpenTelemetry Demo merges this file with its main collector config. The merge replaces pipeline arrays, so keep the demo's existing exporters in each list and add `otlp_http/duckdb`.

On Linux, add `host.docker.internal` to the `otelcol` service in `docker-compose.yml`:

```yaml
services:
  otelcol:
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

Docker Desktop for macOS and Windows provides `host.docker.internal`.

## Start the Demo

```bash
docker compose up -d
```

Generate traffic by opening the demo frontend and using the store:

```text
http://localhost:8080
```

## Query the DuckLake Tables

Flush buffered telemetry and query the running `duckdb-otlp` container:

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

Inspect recent logs:

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

FROM quack_query(
  'quack:localhost:9494',
  $$
  SELECT time_unix_nano, service_name, severity_text, body
  FROM lake.main.otlp_logs
  ORDER BY time_unix_nano DESC
  LIMIT 20
  $$,
  token = 'dev-quack-token-123456'
);
SQL
```

The server image is distroless and has no shell or DuckDB CLI, so run inspection SQL from a host DuckDB process through Quack instead of `docker exec ... sh -c`.

## Stop Cleanly

Stop the demo:

```bash
docker compose down
```

Stop the DuckLake server:

```bash
docker stop duckdb-otlp
```

During shutdown, the image sends `otlp_stop('otlp:0.0.0.0:4318')`, so the server commits remaining buffered rows before the process exits.

## Troubleshooting

- If the collector logs connection errors, confirm the `duckdb-otlp` container is still running and `docker logs duckdb-otlp` shows `OTLP HTTP: 0.0.0.0:4318`.
- If you run the collector in Linux Docker and it cannot resolve `host.docker.internal`, add the `extra_hosts` entry above.
- If the server returns `401`, confirm the collector `Authorization` header matches `DUCKDB_OTLP_TOKEN`.
- If you cannot see rows before the next background commit, run `otlp_flush` before querying.

## See Also

- [How to stream to local DuckLake](../../guides/stream-to-local-ducklake/)
- [How to analyze telemetry](../../guides/analyze-telemetry/)
- [Live Ingest Reference](../../reference/serve/)
