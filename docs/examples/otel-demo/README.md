# OpenTelemetry Demo Export Config

This directory contains `otelcol-config-extras.yml` for exporting OpenTelemetry Demo traces, logs, and metrics to JSONL and protobuf files.

## Use It

```bash
git clone https://github.com/open-telemetry/opentelemetry-demo.git
cd opentelemetry-demo

cp /path/to/duckdb-otlp/docs/examples/otel-demo/otelcol-config-extras.yml \
  src/otelcollector/otelcol-config-extras.yml

mkdir -p exports/json exports/proto
```

Add the export mount to the `otelcol` service in `docker-compose.yml`:

```yaml
services:
  otelcol:
    volumes:
      - ./exports:/export
```

Start the demo and query the files:

```bash
docker compose up -d
duckdb
```

```sql
LOAD otlp;
SELECT * FROM read_otlp_traces('exports/json/traces.jsonl') LIMIT 10;
```

See [OpenTelemetry Demo Exports](../../guides/otel-collector-demo-exports.md).
