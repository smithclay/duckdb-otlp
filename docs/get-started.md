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

**Checkpoint**: You should see no errors. The extension is now loaded.

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
    trace_id,
    span_name,
    duration / 1000000 AS duration_ms
FROM read_otlp_traces('test/data/traces_simple.jsonl')
ORDER BY duration DESC;
```

**Expected output:**
```
┌─────────────────────────────────┬─────────────┬──────────────┐
│            trace_id             │  span_name  │ duration_ms  │
├─────────────────────────────────┼─────────────┼──────────────┤
│ 5b8aa5a2d2c872e8321120eab66d... │ operation-1 │     500.0    │
│ 5b8aa5a2d2c872e8321120eab66d... │ operation-2 │     300.0    │
└─────────────────────────────────┴─────────────┴──────────────┘
```

## Step 3: Try Logs and Metrics

**Query logs:**
```sql
SELECT timestamp, severity_text, body
FROM read_otlp_logs('test/data/logs_simple.jsonl')
WHERE severity_text = 'ERROR';
```

**Query gauge metrics:**
```sql
SELECT metric_name, value
FROM read_otlp_metrics_gauge('test/data/metrics_simple.jsonl')
LIMIT 5;
```

**Checkpoint**: If you see data, you're all set! The extension is working.

## What Just Happened?

You've successfully:
1. Installed the OTLP extension
2. Loaded sample OpenTelemetry data
3. Queried traces, logs, and metrics with SQL

The extension automatically detected the JSON format, parsed the OTLP data, and returned it as strongly-typed DuckDB tables.

## Next Steps

### Use Your Own Data

**Option 1: Use existing OTLP exports**

If you already have OTLP JSON or protobuf files:

```sql
SELECT * FROM read_otlp_traces('path/to/your/traces.jsonl');
SELECT * FROM read_otlp_logs('s3://your-bucket/logs/*.jsonl');
SELECT * FROM read_otlp_metrics_gauge('https://example.com/metrics.pb');
```

**Option 2: Set up the OpenTelemetry Collector**

Export telemetry from your applications using the OTel Collector:

> **[OpenTelemetry Collector Setup](setup/collector.md)**

### Explore the Schemas

Understand what columns are available:

> **[Schema Reference](reference/schemas.md)** - Complete column listings for traces (25), logs (15), and metrics (16-18)

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

**Still stuck?**

> Ask on [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)

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
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');
SELECT * FROM read_otlp_metrics_sum('metrics.jsonl');
```

---

**Next**: [Cookbook](guides/cookbook.md) | [API Reference](reference/api.md) | [Schema Reference](reference/schemas.md)
