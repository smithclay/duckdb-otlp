# OpenTelemetry Demo Integration

Configuration files for exporting OTLP telemetry from the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo).

## Files

- **otelcol-config-extras.yml** - OpenTelemetry Collector configuration for file exports

## Quick Start

1. Clone the OpenTelemetry Demo:
```bash
git clone https://github.com/open-telemetry/opentelemetry-demo.git
cd opentelemetry-demo
```

2. Copy the collector configuration:
```bash
cp /path/to/duckdb-otlp/docs/examples/otel-demo/otelcol-config-extras.yml \
   src/otelcollector/otelcol-config-extras.yml
```

3. Add volume mount to `docker-compose.yml`:
```yaml
services:
  otelcol:
    volumes:
      - ./exports:/export  # Add this line
```

4. Create export directories and start:
```bash
mkdir -p exports/json exports/proto
docker compose up -d
```

5. Query with DuckDB:
```bash
duckdb
```
```sql
LOAD otlp;
SELECT * FROM read_otlp_traces('exports/json/traces.jsonl') LIMIT 10;
```

## Full Documentation

See the [OTel Collector Demo Exports Guide](../guides/otel-collector-demo-exports.md) for:
- Detailed configuration options
- File rotation settings
- Performance comparisons (JSON vs protobuf)
- Example queries
- Troubleshooting

## What Gets Exported

The configuration exports all telemetry signals in both formats:

**JSON Format (JSONL):**
- `/export/json/traces.jsonl` - Trace spans
- `/export/json/metrics.jsonl` - Metrics
- `/export/json/logs.jsonl` - Log records

**Protobuf Format:**
- `/export/proto/traces.proto` - Trace spans
- `/export/proto/metrics.proto` - Metrics
- `/export/proto/logs.proto` - Log records

All files include automatic rotation (max 128MB per file, 7-day retention, up to 100 backups).
