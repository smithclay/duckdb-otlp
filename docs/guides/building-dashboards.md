# Building Dashboards

Learn how to create dashboards and visualizations from OpenTelemetry data using the DuckDB OTLP extension.

## Overview

The DuckDB OTLP extension integrates with the DuckDB ecosystem, enabling you to:
- Build dashboards with BI tools (Metabase, Superset, Grafana, etc.)
- Create Jupyter notebook visualizations
- Use DuckDB-WASM for browser-based dashboards
- Query from Python/R/Node.js applications

## Create Materialized Views

Build tables optimized for dashboard queries:

```sql
LOAD otlp;

-- Hourly trace statistics
CREATE TABLE hourly_trace_stats AS
SELECT
  date_trunc('hour', Timestamp) AS hour,
  ServiceName,
  COUNT(*) AS span_count,
  AVG(Duration) / 1000000 AS avg_duration_ms,
  MAX(Duration) / 1000000 AS max_duration_ms,
  SUM(CASE WHEN StatusCode = 'ERROR' THEN 1 ELSE 0 END) AS error_count
FROM read_otlp_traces('traces/*.jsonl')
GROUP BY hour, ServiceName;

-- Error log summary
CREATE TABLE error_summary AS
SELECT
  date_trunc('day', Timestamp) AS day,
  ServiceName,
  SeverityText,
  COUNT(*) AS error_count
FROM read_otlp_logs('logs/*.jsonl')
WHERE SeverityText IN ('ERROR', 'FATAL')
GROUP BY day, ServiceName, SeverityText;

-- Gauge metric timeseries
CREATE TABLE gauge_timeseries AS
SELECT
  date_trunc('minute', Timestamp) AS minute,
  ServiceName,
  MetricName,
  AVG(Value) AS avg_value,
  MAX(Value) AS max_value,
  MIN(Value) AS min_value
FROM read_otlp_metrics_gauge('metrics/*.jsonl')
GROUP BY minute, ServiceName, MetricName;
```

## Export for BI Tools

Export aggregated data to Parquet for import into BI tools:

```sql
COPY hourly_trace_stats
TO 'dashboards/trace_stats.parquet' (FORMAT PARQUET);

COPY error_summary
TO 'dashboards/errors.parquet' (FORMAT PARQUET);

COPY gauge_timeseries
TO 'dashboards/metrics.parquet' (FORMAT PARQUET);
```

## Python Visualization

Use DuckDB with Python visualization libraries:

```python
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Connect to DuckDB
con = duckdb.connect()
con.execute("INSTALL otlp FROM community")
con.execute("LOAD otlp")

# Query traces
df = con.execute("""
    SELECT
        date_trunc('hour', Timestamp) AS hour,
        ServiceName,
        AVG(Duration) / 1000000 AS avg_duration_ms
    FROM read_otlp_traces('traces/*.jsonl')
    GROUP BY hour, ServiceName
    ORDER BY hour
""").df()

# Plot
df.pivot(index='hour', columns='ServiceName', values='avg_duration_ms').plot()
plt.title('Average Request Duration by Service')
plt.ylabel('Duration (ms)')
plt.show()
```

## Jupyter Notebooks

Create interactive dashboards in Jupyter:

```python
import duckdb
import plotly.express as px

con = duckdb.connect()
con.execute("LOAD otlp")

# Query error rates
errors = con.execute("""
    SELECT
        date_trunc('hour', Timestamp) AS hour,
        ServiceName,
        COUNT(*) AS error_count
    FROM read_otlp_logs('logs/*.jsonl')
    WHERE SeverityText = 'ERROR'
    GROUP BY hour, ServiceName
""").df()

# Interactive plot
fig = px.line(errors, x='hour', y='error_count', color='ServiceName',
              title='Error Count Over Time')
fig.show()
```

## Web Dashboards with DuckDB-WASM

Build browser-based dashboards using the WASM extension:

```html
<!DOCTYPE html>
<html>
<head>
  <script src="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
  <canvas id="traceChart"></canvas>

  <script>
    async function loadDashboard() {
      const bundle = await duckdb.selectBundle({
        mvp: {
          mainModule: duckdb.WASM_BUNDLES.mvp.mainModule,
          mainWorker: duckdb.WASM_BUNDLES.mvp.mainWorker,
        },
      });

      const worker = new Worker(bundle.mainWorker);
      const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
      await db.instantiate(bundle.mainModule);

      // Load OTLP extension
      await db.open({ query: "INSTALL otlp FROM community; LOAD otlp;" });

      // Query data
      const conn = await db.connect();
      const result = await conn.query(`
        SELECT
          date_trunc('hour', Timestamp) AS hour,
          COUNT(*) AS span_count
        FROM read_otlp_traces('traces.jsonl')
        GROUP BY hour
      `);

      // Render chart
      const data = await result.toArray();
      renderChart(data);
    }

    function renderChart(data) {
      new Chart(document.getElementById('traceChart'), {
        type: 'line',
        data: {
          labels: data.map(d => d.hour),
          datasets: [{
            label: 'Span Count',
            data: data.map(d => d.span_count)
          }]
        }
      });
    }

    loadDashboard();
  </script>
</body>
</html>
```

## Common Dashboard Queries

### Service Request Rate

```sql
SELECT
  date_trunc('minute', Timestamp) AS time,
  ServiceName,
  COUNT(*) / 60.0 AS requests_per_second
FROM read_otlp_traces('traces/*.jsonl')
WHERE SpanKind = 'SERVER'
GROUP BY time, ServiceName
ORDER BY time DESC;
```

### Error Rate Percentage

```sql
SELECT
  date_trunc('hour', Timestamp) AS hour,
  ServiceName,
  COUNT(*) AS total_requests,
  SUM(CASE WHEN StatusCode = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
  (error_count * 100.0 / total_requests) AS error_percentage
FROM read_otlp_traces('traces/*.jsonl')
WHERE SpanKind = 'SERVER'
GROUP BY hour, ServiceName;
```

### P95 Latency

```sql
SELECT
  date_trunc('hour', Timestamp) AS hour,
  ServiceName,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY Duration) / 1000000 AS p95_ms
FROM read_otlp_traces('traces/*.jsonl')
WHERE SpanKind = 'SERVER'
GROUP BY hour, ServiceName;
```

## Refresh Strategies

### Manual Refresh

```sql
-- Drop and recreate tables
DROP TABLE IF EXISTS hourly_trace_stats;
CREATE TABLE hourly_trace_stats AS
SELECT ... FROM read_otlp_traces('traces/*.jsonl');
```

### Incremental Updates

```sql
-- Append new data only
INSERT INTO hourly_trace_stats
SELECT ...
FROM read_otlp_traces('traces/latest/*.jsonl')
WHERE Timestamp > (SELECT MAX(hour) FROM hourly_trace_stats);
```

## See Also

- [Working with Metrics](working-with-metrics.md) - Metrics-specific dashboard patterns
- [Exporting to Parquet](exporting-to-parquet.md) - Prepare data for BI tools
- [DuckDB Clients](https://duckdb.org/docs/api/overview) - Connect from various languages
