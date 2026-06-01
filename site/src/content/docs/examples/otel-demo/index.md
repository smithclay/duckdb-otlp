---
title: "OpenTelemetry Demo Example"
---

This directory contains `otelcol-config-extras.yml` for sending OpenTelemetry Demo traces, logs, and metrics to a local `duckdb-otlp` server running in `local-ducklake` mode.

## Use It

```bash
git clone https://github.com/open-telemetry/opentelemetry-demo.git
cd opentelemetry-demo

cp /path/to/duckdb-otlp/site/public/examples/otel-demo/otelcol-config-extras.yml \
  src/otel-collector/otelcol-config-extras.yml
```

Start `duckdb-otlp`, then start the demo:

```bash
docker compose up -d
```

See [Point the OpenTelemetry Demo at Local DuckLake](../../setup/otel-demo/).
