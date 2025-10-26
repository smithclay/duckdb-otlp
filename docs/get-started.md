# Get Started in 3 Minutes

Get up and running with the DuckDB OTLP extension in three simple steps.

## Prerequisites

Just DuckDB installed. That's it.

- **Mac/Linux**: [Install DuckDB](https://duckdb.org/docs/installation/)
- **Windows**: [Install DuckDB](https://duckdb.org/docs/installation/)

## Step 1: Install the Extension

Open DuckDB and install the extension from the community repository:

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

**✅ Checkpoint**: You should see no errors. The extension is now loaded.

## Step 2: Query Sample Data

The extension repository includes sample OTLP files for testing. Clone the repo or download sample data:

```bash
# Clone the repository (or download sample files)
git clone https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp
```

Now query the sample traces:

```sql
SELECT
    TraceId,
    SpanName,
    Duration / 1000000 AS duration_ms
FROM read_otlp_traces('test/data/traces_simple.jsonl')
ORDER BY Duration DESC;
```

**Expected output:**
```
┌─────────────────────────────────┬─────────────┬──────────────┐
│            TraceId              │  SpanName   │ duration_ms  │
├─────────────────────────────────┼─────────────┼──────────────┤
│ 0x5b8aa5a2d2c872e8321120eab66d...│ operation-1 │     500.0    │
│ 0x5b8aa5a2d2c872e8321120eab66d...│ operation-2 │     300.0    │
└─────────────────────────────────┴─────────────┴──────────────┘
```

## Step 3: Try Logs and Metrics

**Query logs:**
```sql
SELECT Timestamp, SeverityText, Body
FROM read_otlp_logs('test/data/logs_simple.jsonl')
WHERE SeverityText = 'ERROR';
```

**Query metrics:**
```sql
SELECT MetricName, MetricType, Value
FROM read_otlp_metrics('test/data/metrics_simple.jsonl')
WHERE MetricType = 'gauge'
LIMIT 5;
```

**✅ Checkpoint**: If you see data, you're all set! The extension is working.

## What Just Happened?

You've successfully:
1. ✅ Installed the OTLP extension
2. ✅ Loaded sample OpenTelemetry data
3. ✅ Queried traces, logs, and metrics with SQL

The extension automatically detected the JSON format, parsed the OTLP data, and returned it as strongly-typed DuckDB tables.

## Next Steps

### Use Your Own Data

**Option 1: Use existing OTLP exports**

If you already have OTLP JSON or protobuf files:

```sql
SELECT * FROM read_otlp_traces('path/to/your/traces.jsonl');
SELECT * FROM read_otlp_logs('s3://your-bucket/logs/*.jsonl');
SELECT * FROM read_otlp_metrics('https://example.com/metrics.pb');
```

**Option 2: Set up the OpenTelemetry Collector**

Export telemetry from your applications using the OTel Collector:

→ **[OpenTelemetry Collector Setup](setup/collector.md)**

### Learn Common Patterns

→ **[Analyzing Traces](guides/analyzing-traces.md)** - Find slow requests and bottlenecks
→ **[Filtering Logs](guides/filtering-logs.md)** - Debug with SQL
→ **[Working with Metrics](guides/working-with-metrics.md)** - Build dashboards
→ **[Exporting to Parquet](guides/exporting-to-parquet.md)** - Archive for long-term storage

### Explore the Schemas

Understand what columns are available:

→ **[Traces Schema](reference/traces-schema.md)** - 22 columns for trace spans
→ **[Logs Schema](reference/logs-schema.md)** - 15 columns for log records
→ **[Metrics Schema](reference/metrics-schema.md)** - Union and typed helpers

## Common Issues

**"Extension 'otlp' not found"**

Make sure you installed from community:
```sql
INSTALL otlp FROM community;
LOAD otlp;
```

**"File does not exist"**

Use the correct path to your data files:
```sql
-- Absolute path
SELECT * FROM read_otlp_traces('/full/path/to/traces.jsonl');

-- Relative path (from DuckDB working directory)
SELECT * FROM read_otlp_traces('./test/data/traces_simple.jsonl');
```

**"Cannot parse OTLP data"**

Check the file format. The extension auto-detects JSON and protobuf, but the file must be valid OTLP:
```sql
-- Check scan diagnostics
SELECT * FROM read_otlp_scan_stats();
```

**Still stuck?**

→ Ask on [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
→ Check [Error Handling Guide](guides/error-handling.md)

## Quick Reference

**Install and load:**
```sql
INSTALL otlp FROM community;
LOAD otlp;
```

**Query traces:**
```sql
SELECT * FROM read_otlp_traces('traces.jsonl');
```

**Query logs:**
```sql
SELECT * FROM read_otlp_logs('logs.jsonl');
```

**Query metrics:**
```sql
SELECT * FROM read_otlp_metrics('metrics.jsonl');
```

**Use typed metric helpers:**
```sql
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');
SELECT * FROM read_otlp_metrics_sum('metrics.jsonl');
SELECT * FROM read_otlp_metrics_histogram('metrics.jsonl');
```

---

**Next**: [Guides →](guides/) | [API Reference →](reference/api.md) | [Back to Docs →](README.md)
