# Sample Data

Get sample OTLP data for testing and exploration.

## Quick Start: Use Built-in Samples

The extension repository includes sample files for testing:

```bash
# Clone repository
git clone https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp

# Sample files are in test/data/
ls -la test/data/
```

```sql
LOAD otlp;

-- Query sample traces
SELECT * FROM read_otlp_traces('test/data/traces_simple.jsonl');

-- Query sample logs
SELECT * FROM read_otlp_logs('test/data/logs_simple.jsonl');

-- Query sample metrics
SELECT * FROM read_otlp_metrics('test/data/metrics_simple.jsonl');
```

## Browser Demo Samples

Try the [interactive browser demo](https://smithclay.github.io/duckdb-otlp/) which includes:
- Pre-loaded sample OTLP traces, logs, and metrics
- Ability to upload your own JSONL files
- Example queries to get started

## Generate Data with OpenTelemetry Demo

Export OTLP data from the official OpenTelemetry demo application:

See the [OTel Collector Demo Exports Guide](../guides/otel-collector-demo-exports.md) for step-by-step instructions.

## Generate Data with Collector

Configure the OpenTelemetry Collector to export OTLP files from your own applications:

See the [Collector Setup Guide](collector.md) for configuration examples.

## Download Public Datasets

Several public OTLP datasets are available online:

### Example Datasets

1. **OpenTelemetry Demo Exports** - Sample data from the OTel demo app
   ```bash
   # Download sample files (example)
   wget https://github.com/smithclay/duckdb-otlp/raw/main/test/data/traces_simple.jsonl
   wget https://github.com/smithclay/duckdb-otlp/raw/main/test/data/logs_simple.jsonl
   wget https://github.com/smithclay/duckdb-otlp/raw/main/test/data/metrics_simple.jsonl
   ```

2. **Generate with otel-cli** - Command-line tool for generating telemetry
   ```bash
   # Install otel-cli
   brew install otel-cli  # macOS
   # or download from: https://github.com/equinix-labs/otel-cli

   # Generate sample trace
   otel-cli span \
     --service "sample-service" \
     --name "sample-operation" \
     --endpoint http://localhost:4318/v1/traces
   ```

## Generate Synthetic Data

Create your own test data with Python:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# Configure exporter
exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")

# Setup tracer
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# Generate sample spans
for i in range(100):
    with tracer.start_as_current_span(f"operation-{i}"):
        # Simulate work
        time.sleep(random.random() * 0.1)
```

Configure the collector to write these spans to JSONL files (see [Collector Setup](collector.md)).

## Sample File Formats

### Traces (JSONL)

```jsonl
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeSpans":[{"spans":[{"traceId":"1234567890abcdef","spanId":"abcdef123456","name":"POST /checkout","kind":2,"startTimeUnixNano":"1704067200000000000","endTimeUnixNano":"1704067201000000000"}]}]}]}
```

### Logs (JSONL)

```jsonl
{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"ERROR","body":{"stringValue":"Connection timeout"}}]}]}]}
```

### Metrics (JSONL)

```jsonl
{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},"scopeMetrics":[{"metrics":[{"name":"http.server.duration","unit":"ms","gauge":{"dataPoints":[{"timeUnixNano":"1704067200000000000","asDouble":123.45}]}}]}]}]}
```

## Validate OTLP Files

Check if your files are valid OTLP format:

```sql
LOAD otlp;

-- Try reading with error handling
SELECT COUNT(*) AS valid_records
FROM read_otlp_traces('my_data.jsonl', on_error := 'skip');

-- Check scan stats
SELECT * FROM read_otlp_scan_stats();
```

## Convert Between Formats

### JSON to Protobuf

Use the OpenTelemetry Collector to convert:

```yaml
receivers:
  otlp:
    protocols:
      http:

exporters:
  file/json:
    path: output.jsonl
    encoding: json
  file/proto:
    path: output.pb
    encoding: proto

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [file/json, file/proto]
```

### Query and Re-export

```sql
LOAD otlp;

-- Convert JSONL to Parquet
COPY (
  SELECT * FROM read_otlp_traces('input.jsonl')
) TO 'output.parquet' (FORMAT PARQUET);

-- Later, query Parquet directly
SELECT * FROM read_parquet('output.parquet');
```

## Next Steps

- **Query Sample Data**: Follow the [Get Started Guide](../get-started.md)
- **Setup Collector**: See [Collector Setup](collector.md)
- **Learn Query Patterns**: Browse the [Cookbook](../guides/cookbook.md)

## See Also

- [Collector Setup](collector.md) - Configure OpenTelemetry Collector
- [OTel Demo Exports](../guides/otel-collector-demo-exports.md) - Export from demo app
- [Installation](installation.md) - Install the extension
