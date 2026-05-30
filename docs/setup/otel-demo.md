# OpenTelemetry Demo Exports

Use this when you want real sample telemetry from the OpenTelemetry Demo. For a generic collector config, see [Collector Setup](collector.md).

## 1. Add File Exporters

In the OpenTelemetry Demo repository, create `src/otelcollector/otelcol-config-extras.yml`:

```yaml
exporters:
  file/json-traces:
    path: /export/json/traces.jsonl
    encoding: json
    rotation:
      max_megabytes: 50
      max_days: 1
  file/json-logs:
    path: /export/json/logs.jsonl
    encoding: json
    rotation:
      max_megabytes: 50
      max_days: 1
  file/json-metrics:
    path: /export/json/metrics.jsonl
    encoding: json
    rotation:
      max_megabytes: 50
      max_days: 1

service:
  pipelines:
    traces:
      exporters: [otlp, debug, spanmetrics, file/json-traces]
    logs:
      exporters: [opensearch, debug, file/json-logs]
    metrics:
      exporters: [otlphttp/prometheus, debug, file/json-metrics]
```

If your collector version expects `format` instead of `encoding`, use `format: json`.

## 2. Mount the Export Directory

Add an export volume to the `otelcol` service in `docker-compose.yml`:

```yaml
services:
  otelcol:
    volumes:
      - ./exports:/export
```

Then start the demo:

```bash
mkdir -p exports/json
docker compose up -d
```

## 3. Query the Files

```sql
INSTALL otlp FROM community;
LOAD otlp;

SELECT service_name, span_name, count(*) AS span_count, avg(duration) / 1000000 AS avg_ms
FROM read_otlp_traces('exports/json/traces.jsonl*')
GROUP BY service_name, span_name
ORDER BY avg_ms DESC
LIMIT 10;

SELECT timestamp, service_name, severity_text, body
FROM read_otlp_logs('exports/json/logs.jsonl*')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp DESC;

SELECT timestamp, service_name, metric_name, value
FROM read_otlp_metrics_gauge('exports/json/metrics.jsonl*')
LIMIT 20;
```

## 4. Archive What You Need

```sql
COPY (
  SELECT * FROM read_otlp_traces('exports/json/traces.jsonl*')
) TO 'archives/traces.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

Troubleshoot with:

```bash
docker compose logs otelcol | grep file
docker compose exec otelcol ls -la /export/json
wc -l exports/json/*.jsonl
```
