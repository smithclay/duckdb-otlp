# How to Install the Extension

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

- DuckDB v2.0.0 for the current extension build
- Supported platforms:
  - Linux (x86_64, arm64)
  - macOS (Intel, Apple Silicon)
  - Windows (x86_64)

## Install the `duckdb-otlp` CLI

The standalone `duckdb-otlp` binary runs the receiver with an embedded DuckDB instance. Use the `otlp` extension in the `duckdb` CLI for file conversion and SQL queries.

```sh
# macOS or Linux (x86_64 or arm64), installs into ~/.local/bin
curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sh
```

The script picks the latest release for your platform, verifies the download against the release's `SHA256SUMS`, and installs a single executable; re-run it to upgrade. Homebrew works too:

```sh
brew install smithclay/tap/duckdb-otlp
```

See the [CLI reference](../../reference/cli/) for install options, every command, and the release tarballs to install by hand.

## Use in Browser (DuckDB-WASM)

Try the extension in your browser without installation:

**[Interactive Demo](../../demo/)**

The WASM demo supports:
- JSON, JSONL, and protobuf file reads
- Loading sample OTLP data
- Running SQL queries in-browser
- Uploading your own files

Native builds include the live ingest server.

## Build from Source

For development or custom builds, see [CONTRIBUTING.md](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md).

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
- [CLI Reference](../../reference/cli/) - configure, run, and check the standalone receiver.
- [OpenTelemetry Collector](../collector/) - export OTLP files from the collector.
- [OpenTelemetry Demo](../otel-demo/) - stream demo traces, logs, and metrics into local DuckLake.
- [How-to Guides](../../guides/) - query and export telemetry.

## Troubleshooting

### Extension Not Found

```sql
Error: Extension "otlp" not found
```

Check that your DuckDB version matches the extension build, then install and load the extension.

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

Use a native DuckDB build for live ingestion.

## See Also

- [How to Configure the OpenTelemetry Collector](../collector/) - export OTLP files
- [Get Started](../../get-started/) - first queries and one HTTP ingest request
