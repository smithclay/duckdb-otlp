# Exporting to Parquet

Use Parquet when you will query the same telemetry repeatedly or hand it to BI/data tools. After export, DuckDB can read the Parquet files without loading the OTLP extension.

## Traces and Logs

```sql
LOAD otlp;

COPY (
  SELECT * FROM read_otlp_traces('otel-export/*.jsonl')
) TO 'warehouse/traces.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
  SELECT * FROM read_otlp_logs('otel-export/*.jsonl')
  WHERE severity_text IN ('ERROR', 'WARN', 'FATAL')
) TO 'warehouse/error_logs.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

Query later:

```sql
SELECT trace_id, span_name, duration / 1000000 AS duration_ms
FROM read_parquet('warehouse/traces.parquet')
WHERE duration >= 1000000000
ORDER BY duration DESC;
```

## Metrics

Metrics have shape-specific schemas, so write each shape separately:

```sql
COPY (
  SELECT * FROM read_otlp_metrics_gauge('metrics/*.jsonl')
) TO 'warehouse/metrics_gauge.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
  SELECT * FROM read_otlp_metrics_sum('metrics/*.jsonl')
) TO 'warehouse/metrics_sum.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
  SELECT * FROM read_otlp_metrics_histogram('metrics/*.jsonl')
) TO 'warehouse/metrics_histogram.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (
  SELECT * FROM read_otlp_metrics_exp_histogram('metrics/*.jsonl')
) TO 'warehouse/metrics_exp_histogram.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

## Partitioned Datasets

```sql
COPY (
  SELECT *, date_trunc('day', timestamp) AS partition_date
  FROM read_otlp_traces('traces/*.jsonl')
) TO 'warehouse/traces' (
  FORMAT PARQUET,
  COMPRESSION ZSTD,
  PARTITION_BY (partition_date)
);
```

## Join Exported Signals

```sql
SELECT
  t.span_name,
  t.duration / 1000000 AS duration_ms,
  count(l.body) AS error_count
FROM read_parquet('warehouse/traces.parquet') t
LEFT JOIN read_parquet('warehouse/error_logs.parquet') l
  ON t.trace_id = l.trace_id
GROUP BY t.span_name, t.duration
ORDER BY error_count DESC;
```

See [DuckDB Parquet docs](https://duckdb.org/docs/data/parquet/overview) and [How-to Guides](README.md).
