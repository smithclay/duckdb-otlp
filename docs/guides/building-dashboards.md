# Building Dashboards

For dashboards, first turn raw OTLP files into small, typed tables or Parquet files. Keep the dashboard layer reading compact data, not re-parsing telemetry exports on every refresh.

## Useful Rollups

```sql
LOAD otlp;

CREATE TABLE hourly_trace_stats AS
SELECT
  date_trunc('hour', timestamp) AS hour,
  service_name,
  count(*) AS span_count,
  avg(duration) / 1000000 AS avg_duration_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY duration) / 1000000 AS p95_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE span_kind = 2
GROUP BY hour, service_name;

CREATE TABLE error_logs_hourly AS
SELECT
  date_trunc('hour', timestamp) AS hour,
  service_name,
  severity_text,
  count(*) AS log_count
FROM read_otlp_logs('logs/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
GROUP BY hour, service_name, severity_text;

CREATE TABLE gauge_metrics_minute AS
SELECT
  date_trunc('minute', timestamp) AS minute,
  service_name,
  metric_name,
  avg(value) AS avg_value,
  max(value) AS max_value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
GROUP BY minute, service_name, metric_name;
```

## Export for BI Tools

```sql
COPY hourly_trace_stats TO 'dashboards/hourly_trace_stats.parquet' (FORMAT PARQUET);
COPY error_logs_hourly TO 'dashboards/error_logs_hourly.parquet' (FORMAT PARQUET);
COPY gauge_metrics_minute TO 'dashboards/gauge_metrics_minute.parquet' (FORMAT PARQUET);
```

## Refresh Pattern

For file exports, refresh from a narrow glob:

```sql
INSERT INTO hourly_trace_stats
SELECT
  date_trunc('hour', timestamp) AS hour,
  service_name,
  count(*) AS span_count,
  avg(duration) / 1000000 AS avg_duration_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY duration) / 1000000 AS p95_ms
FROM read_otlp_traces('traces/latest/*.jsonl')
GROUP BY hour, service_name;
```

For live ingest, query the target tables after `otlp_flush` or after an automatic seal:

```sql
SELECT * FROM otlp_flush('otlp:localhost:4318');
SELECT count(*) FROM otlp_logs;
```

See [Exporting to Parquet](exporting-to-parquet.md) and [Live Ingest Quickstart](../quickstart/serve.md).
