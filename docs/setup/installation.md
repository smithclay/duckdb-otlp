# How to Install the Extension

Install the DuckDB OpenTelemetry Extension to start querying OpenTelemetry data with SQL.

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
- JSON, JSONL, and protobuf file reads
- Loading sample OTLP data
- Running SQL queries in-browser
- Uploading your own files

The live ingest server is available only in native builds.

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

**Note**: The live ingest server is not available in WASM builds.

## Verify Installation

Check that the extension loaded successfully:

```sql
-- List installed extensions
SELECT * FROM duckdb_extensions() WHERE extension_name = 'otlp';

-- Test with sample data
SELECT count(*) FROM read_otlp_logs('test/data/logs_simple.jsonl');
```

## Next Steps

- [Get Started](../get-started.md) - install, load, and run first queries.
- [OpenTelemetry Collector](collector.md) - export OTLP files from the collector.
- [Sample Data](sample-data.md) - use small test files.
- [How-to Guides](../guides/README.md) - complete query and export tasks.

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

### Live Ingest in WASM

```
Error: otlp_serve is not implemented for the wasm platform
```

The HTTP ingest server is native-only. Use a native DuckDB build for `otlp_serve`, `otlp_flush`, `otlp_stop`, and `otlp_server_list`.

## See Also

- [How to Configure the OpenTelemetry Collector](collector.md) - export OTLP files
- [CONTRIBUTING.md](../../CONTRIBUTING.md) - Build instructions
- [Get Started](../get-started.md) - first queries and one HTTP ingest request
