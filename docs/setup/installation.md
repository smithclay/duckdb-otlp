# Installation

Install the DuckDB OTLP extension to start querying OpenTelemetry data with SQL.

## Install from Community (Recommended)

The easiest way to get started:

```sql
-- Install from DuckDB community extensions
INSTALL otlp FROM community;
LOAD otlp;
```

This downloads the pre-built extension for your platform.

### Requirements

- DuckDB 0.10.0 or later
- Supported platforms:
  - Linux (x86_64, arm64)
  - macOS (Intel, Apple Silicon)
  - Windows (x86_64)

## Use in Browser (DuckDB-WASM)

Try the extension in your browser without installation:

**[→ Interactive Demo](https://smithclay.github.io/duckdb-otlp/)**

The WASM demo supports:
- JSON format only (protobuf requires native builds)
- Loading sample OTLP data
- Running SQL queries in-browser
- Uploading your own JSONL files

## Build from Source

For development or custom builds, see [CONTRIBUTING.md](../../CONTRIBUTING.md).

### Quick Build

```bash
# Clone repository
git clone https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp

# Install vcpkg dependencies
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build with ninja (recommended)
GEN=ninja make

# DuckDB shell with extension loaded
./build/release/duckdb
```

### WASM Build

```bash
# Build for browser
make wasm_eh

# Output: build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm
```

**Note**: WASM builds support JSON format only. Protobuf parsing requires native builds.

## Verify Installation

Check that the extension loaded successfully:

```sql
-- List installed extensions
SELECT * FROM duckdb_extensions() WHERE extension_name = 'otlp';

-- Test with sample data
SELECT * FROM read_otlp_options();
```

## Usage

Once installed, use the table functions to query OTLP data:

```sql
LOAD otlp;

-- Query traces
SELECT TraceId, SpanName, Duration
FROM read_otlp_traces('traces.jsonl')
LIMIT 10;

-- Query logs
SELECT Timestamp, SeverityText, Body
FROM read_otlp_logs('logs.jsonl')
WHERE SeverityText = 'ERROR';

-- Query metrics
SELECT Timestamp, MetricName, Value
FROM read_otlp_metrics_gauge('metrics.jsonl');
```

## Next Steps

- **Get OTLP Data**: See [Collector Setup](collector.md) to export data from OpenTelemetry Collector
- **Sample Data**: See [Sample Data](sample-data.md) for test files
- **Quick Start**: Follow the [Get Started Guide](../get-started.md)
- **Examples**: Browse the [Cookbook](../guides/cookbook.md)

## Troubleshooting

### Extension Not Found

```sql
Error: Extension "otlp" not found
```

Ensure you're using DuckDB 0.10+ and run:

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

### Platform Not Supported

If pre-built binaries aren't available for your platform, [build from source](../../CONTRIBUTING.md).

### WASM Protobuf Errors

```
Error: Protobuf format not supported in WASM builds
```

WASM builds only support JSON format. Use native builds for protobuf, or convert protobuf files to JSON.

## See Also

- [Collector Setup](collector.md) - Configure OpenTelemetry Collector
- [CONTRIBUTING.md](../../CONTRIBUTING.md) - Build instructions
- [Get Started](../get-started.md) - Quick start tutorial
