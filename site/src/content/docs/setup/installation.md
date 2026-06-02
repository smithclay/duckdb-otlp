---
title: "How to Install the Extension"
---

Install the DuckDB OpenTelemetry Extension to start querying OpenTelemetry data with SQL.

## Install from Community

Install from DuckDB's community extension repository:

```sql
-- Install from DuckDB community extensions
INSTALL otlp FROM community;
LOAD otlp;
```

DuckDB downloads the pre-built extension for your platform.

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

Native builds include the live ingest server.

## Build from Source

For development or custom builds, see [CONTRIBUTING.md](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md).

### Quick Build

```bash
# Clone repository
git clone https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp

# Install vcpkg dependencies
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build with ninja
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

Check that DuckDB loaded the extension:

```sql
-- List installed extensions
SELECT * FROM duckdb_extensions() WHERE extension_name = 'otlp';

-- Test with sample data
SELECT count(*) FROM read_otlp_logs('test/data/logs_simple.jsonl');
```

## Next Steps

- [Get Started](../../get-started/) - install, load, and run first queries.
- [OpenTelemetry Collector](../collector/) - export OTLP files from the collector.
- [OpenTelemetry Demo](../otel-demo/) - stream demo traces, logs, and metrics into local DuckLake.
- [How-to Guides](../../guides/) - query and export telemetry.

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

If pre-built binaries aren't available for your platform, [build from source](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md).

### Live Ingest in WASM

```
Error: otlp_serve is not implemented for the wasm platform
```

Use a native DuckDB build for the HTTP ingest functions: `otlp_serve`, `otlp_flush`, `otlp_stop`, and `otlp_server_list`.

## See Also

- [How to Configure the OpenTelemetry Collector](../collector/) - export OTLP files
- [CONTRIBUTING.md](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md) - build instructions
- [Get Started](../../get-started/) - first queries and one HTTP ingest request
