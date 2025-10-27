# Exporting OTLP Files from the OpenTelemetry Demo

Export traces, logs, and metrics to JSON and protobuf files from the OpenTelemetry Collector demo for analysis with DuckDB.

## What You'll Learn

In 15 minutes, you'll know how to:
- Configure the OTel Collector demo to export telemetry files
- Set up file exporters for JSON and protobuf formats
- Access and manage exported telemetry data
- Query the exported files with DuckDB

## Prerequisites

- Docker and Docker Compose installed
- The [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo) repository cloned
- DuckDB with the OTLP extension installed ([Get Started](../get-started.md))

## Quick Start

### 1. Configure File Exporters

Create a file `src/otelcollector/otelcol-config-extras.yml` in the OpenTelemetry demo repository:

```yaml
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

# File exporters for persistent storage in JSON and protobuf formats
exporters:
  file/json-traces:
    path: /export/json/traces.jsonl
    format: json
    rotation:
      max_megabytes: 128
      max_days: 7
      max_backups: 100
      localtime: true
    flush_interval: 1s

  file/json-metrics:
    path: /export/json/metrics.jsonl
    format: json
    rotation:
      max_megabytes: 128
      max_days: 7
      max_backups: 100
      localtime: true
    flush_interval: 1s

  file/json-logs:
    path: /export/json/logs.jsonl
    format: json
    rotation:
      max_megabytes: 128
      max_days: 7
      max_backups: 100
      localtime: true
    flush_interval: 1s

  file/proto-traces:
    path: /export/proto/traces.proto
    format: proto
    rotation:
      max_megabytes: 128
      max_days: 7
      max_backups: 100
      localtime: true
    flush_interval: 1s

  file/proto-metrics:
    path: /export/proto/metrics.proto
    format: proto
    rotation:
      max_megabytes: 128
      max_days: 7
      max_backups: 100
      localtime: true
    flush_interval: 1s

  file/proto-logs:
    path: /export/proto/logs.proto
    format: proto
    rotation:
      max_megabytes: 128
      max_days: 7
      max_backups: 100
      localtime: true
    flush_interval: 1s

service:
  pipelines:
    traces:
      # Keep existing exporters and add file exporters
      exporters: [otlp, debug, spanmetrics, file/json-traces, file/proto-traces]
    metrics:
      # Keep existing exporters and add file exporters
      exporters: [otlphttp/prometheus, debug, file/json-metrics, file/proto-metrics]
    logs:
      # Keep existing exporters and add file exporters
      exporters: [opensearch, debug, file/json-logs, file/proto-logs]
```

### 2. Mount Export Volume

Add a volume mount to `docker-compose.yml` for the OTel Collector service:

```yaml
services:
  otelcol:
    # ... existing configuration ...
    volumes:
      # ... existing volumes ...
      - ./exports:/export  # Add this line
```

### 3. Create Export Directory

```bash
# In the opentelemetry-demo directory
mkdir -p exports/json exports/proto
```

### 4. Start the Demo

```bash
docker compose up -d
```

The collector will now export telemetry to both JSON and protobuf files in the `exports/` directory.

## Step-by-Step: Working with Exported Files

### 1. Verify Files Are Being Created

Check that the collector is writing files:

```bash
# Watch for new files
ls -lh exports/json/ exports/proto/

# Expected output:
# exports/json/traces.jsonl
# exports/json/metrics.jsonl
# exports/json/logs.jsonl
# exports/proto/traces.proto
# exports/proto/metrics.proto
# exports/proto/logs.proto
```

### 2. Monitor File Growth

See telemetry data accumulating in real-time:

```bash
# Watch file sizes update
watch -n 2 'ls -lh exports/json/'

# Or tail the JSON files
tail -f exports/json/traces.jsonl
```

### 3. Query with DuckDB

Once files are being written, query them directly:

```sql
-- Open DuckDB in the demo directory
-- Start DuckDB and load the extension
D LOAD otlp;

-- Query trace data
SELECT
    ServiceName,
    SpanName,
    COUNT(*) as span_count,
    AVG(Duration) / 1000000 as avg_duration_ms
FROM read_otlp_traces('exports/json/traces.jsonl')
GROUP BY ServiceName, SpanName
ORDER BY avg_duration_ms DESC
LIMIT 10;
```

**Output (example from the demo):**
```
┌─────────────────────┬──────────────────────────┬────────────┬─────────────────┐
│    ServiceName      │        SpanName          │ span_count │ avg_duration_ms │
├─────────────────────┼──────────────────────────┼────────────┼─────────────────┤
│ recommendationservice│ /hipstershop.Product...│     45     │     125.3       │
│ frontend            │ /                        │    152     │      89.4       │
│ checkoutservice     │ /hipstershop.CheckOut...│     38     │      78.2       │
└─────────────────────┴──────────────────────────┴────────────┴─────────────────┘
```

### 4. Compare JSON vs Protobuf Performance

Test query performance between formats:

```sql
-- Time JSON parsing
SELECT COUNT(*) FROM read_otlp_traces('exports/json/traces.jsonl');

-- Time protobuf parsing
SELECT COUNT(*) FROM read_otlp_traces('exports/proto/traces.proto');
```

**Performance Notes:**
- **JSON**: Human-readable, easier to debug, slightly slower parsing
- **Protobuf**: Binary format, smaller file size, faster parsing, requires native build

## Configuration Options

### File Rotation Settings

Control how files are rotated to prevent disk space issues:

```yaml
rotation:
  max_megabytes: 128    # Rotate when file reaches 128MB
  max_days: 7           # Delete files older than 7 days
  max_backups: 100      # Keep max 100 rotated files
  localtime: true       # Use local time for rotation timestamps
```

### Flush Interval

Control how often data is written to disk:

```yaml
flush_interval: 1s      # Write buffered data every second
```

**Options:**
- `1s` - Low latency, more I/O operations
- `10s` - Balanced (default)
- `30s` - Higher latency, fewer I/O operations

### File Paths

Customize where files are written:

```yaml
exporters:
  file/json-traces:
    path: /export/json/traces-${env:HOSTNAME}.jsonl  # Include hostname
    # OR
    path: /export/json/traces-2024-10-26.jsonl       # Date-stamped
```

## Advanced Patterns

### Querying Rotated Files

The file exporter creates backups like `traces.jsonl.1`, `traces.jsonl.2`:

```sql
-- Query all rotated trace files
SELECT * FROM read_otlp_traces('exports/json/traces.jsonl*');

-- Combine current and backup files
SELECT
    DATE_TRUNC('hour', Timestamp) as hour,
    COUNT(*) as span_count
FROM read_otlp_traces('exports/json/traces.jsonl*')
WHERE Timestamp >= NOW() - INTERVAL 7 DAYS
GROUP BY hour
ORDER BY hour;
```

### Export to Parquet for Long-Term Storage

Convert OTLP files to Parquet for better compression:

```sql
-- Export traces to Parquet
COPY (
    SELECT * FROM read_otlp_traces('exports/json/traces.jsonl')
) TO 'archives/traces-2024-10-26.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Export metrics to Parquet
COPY (
    SELECT * FROM read_otlp_metrics('exports/json/metrics.jsonl')
) TO 'archives/metrics-2024-10-26.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Export logs to Parquet
COPY (
    SELECT * FROM read_otlp_logs('exports/json/logs.jsonl')
) TO 'archives/logs-2024-10-26.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

### Create Daily Archive Tables

Automate archival with scheduled exports:

```sql
-- Create date-partitioned tables
CREATE TABLE traces_archive AS
SELECT
    DATE_TRUNC('day', Timestamp) as date,
    *
FROM read_otlp_traces('exports/json/traces.jsonl');

-- Add index for fast queries
CREATE INDEX idx_traces_date ON traces_archive(date);

-- Query by date range
SELECT * FROM traces_archive
WHERE date BETWEEN '2024-10-20' AND '2024-10-26';
```

### Monitor Demo Services

Track service health using exported data:

```sql
-- Service error rates
SELECT
    ServiceName,
    COUNT(*) as total_requests,
    SUM(CASE WHEN StatusCode = 'ERROR' THEN 1 ELSE 0 END) as errors,
    (SUM(CASE WHEN StatusCode = 'ERROR' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as error_rate_pct
FROM read_otlp_traces('exports/json/traces.jsonl')
WHERE SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY error_rate_pct DESC;

-- Service latency percentiles
SELECT
    ServiceName,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY Duration) / 1000000 as p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Duration) / 1000000 as p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY Duration) / 1000000 as p99_ms
FROM read_otlp_traces('exports/json/traces.jsonl')
WHERE SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY p95_ms DESC;
```

## Understanding Export Formats

### JSON Format (JSONL)

Newline-delimited JSON with one OTLP message per line:

```json
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"frontend"}}]},"scopeSpans":[{"scope":{"name":"frontend"},"spans":[{"traceId":"5b8aa5a2d2c872e8","spanId":"7a3f91b4c2d8e5f6","name":"GET /","kind":"SERVER","startTimeUnixNano":"1729900000000000000","endTimeUnixNano":"1729900000089400000"}]}]}]}
```

**Advantages:**
- Human-readable and debuggable
- Works with standard text tools (grep, jq)
- Supported in WASM builds

**Use when:**
- Debugging collector configurations
- Sharing samples with others
- Running DuckDB-WASM in the browser

### Protobuf Format

Binary protocol buffer messages:

**Advantages:**
- Smaller file size (typically 40-60% smaller)
- Faster parsing in DuckDB
- Preserves exact wire format

**Use when:**
- Performance is critical
- Storage space is limited
- Running native DuckDB builds

## Troubleshooting

**Files not being created**

Check collector logs:
```bash
docker compose logs otelcol | grep file
```

Verify volume mount:
```bash
docker compose exec otelcol ls -la /export/json
```

**Permission denied errors**

Fix directory permissions:
```bash
chmod -R 777 exports/
```

**Files are empty**

Wait a few seconds for the flush interval, then check:
```bash
wc -l exports/json/*.jsonl
```

**DuckDB can't read files**

Verify file format:
```bash
# Check JSON files are valid JSONL
head -1 exports/json/traces.jsonl | jq .

# Check protobuf files have content
file exports/proto/traces.proto
```

**Rotated files missing**

Check rotation settings and disk space:
```bash
df -h .
docker compose exec otelcol df -h /export
```

## Cleaning Up

### Remove Old Files

```bash
# Delete files older than 7 days
find exports/ -type f -mtime +7 -delete

# Or delete all exported data
rm -rf exports/json/* exports/proto/*
```

### Stop the Demo

```bash
docker compose down

# Remove volumes too
docker compose down -v
```

## Next Steps

- **[Analyzing Traces](analyzing-traces.md)** - Query the exported trace data
- **[Filtering Logs](filtering-logs.md)** - Search exported logs for errors
- **[Working with Metrics](working-with-metrics.md)** - Analyze exported metrics
- **[Exporting to Parquet](exporting-to-parquet.md)** - Archive telemetry long-term

## Additional Resources

- [OpenTelemetry Collector File Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/fileexporter)
- [OpenTelemetry Demo Repository](https://github.com/open-telemetry/opentelemetry-demo)
- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)

---

**Related**: [Get Started](../get-started.md) | [Analyzing Traces](analyzing-traces.md) | [Back to Guides](README.md)
