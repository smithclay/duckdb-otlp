---
title: "How to Get Sample Data"
---

The repository includes small OTLP fixtures under `test/data/`.

```bash
git clone https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp
ls test/data/
```

```sql
LOAD otlp;

SELECT * FROM read_otlp_traces('test/data/traces_simple.jsonl');
SELECT * FROM read_otlp_logs('test/data/logs_simple.jsonl');
SELECT * FROM read_otlp_metrics_gauge('test/data/metrics_simple.jsonl');
SELECT * FROM read_otlp_metrics_sum('test/data/metrics_simple.jsonl');
SELECT * FROM read_otlp_metrics_histogram('test/data/metrics_simple.jsonl');
```

## Download Individual Fixtures

```bash
curl -LO https://raw.githubusercontent.com/smithclay/duckdb-otlp/main/test/data/traces_simple.jsonl
curl -LO https://raw.githubusercontent.com/smithclay/duckdb-otlp/main/test/data/logs_simple.jsonl
curl -LO https://raw.githubusercontent.com/smithclay/duckdb-otlp/main/test/data/metrics_simple.jsonl
```

## Generate Your Own

Use the collector file exporter for application telemetry:

- [How to Configure the OpenTelemetry Collector](../collector/)
- [How to Export the OpenTelemetry Demo](../otel-demo/)

Use the live ingest server for quick HTTP tests:

```sql
SELECT listen_url
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

```bash
curl -sS http://localhost:4318/v1/logs -H 'Authorization: Bearer dev-token-123456' -H 'Content-Type: application/json' -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"curl-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from curl"}}]}]}]}'
```

## Validate a File

Run the matching reader. Malformed files fail fast.

```sql
SELECT count(*) FROM read_otlp_traces('my_traces.jsonl');
SELECT count(*) FROM read_otlp_logs('my_logs.jsonl');
SELECT count(*) FROM read_otlp_metrics_gauge('my_metrics.jsonl');
```

See [Error Handling](../../guides/error-handling/) for common failures.
