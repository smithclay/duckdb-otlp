# Architecture

This document describes the internal architecture of the DuckDB OTLP extension.

## Overview

The DuckDB OTLP extension is a **table-function extension** that exposes strongly-typed readers for OpenTelemetry Protocol (OTLP) telemetry files. It supports reading traces, logs, and metrics from JSON and protobuf OTLP exports.

## Extension Type

The extension provides:
- Table functions: `read_otlp_traces`, `read_otlp_logs`, `read_otlp_metrics`
- Metrics helper functions: `read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}`
- Diagnostic functions: `read_otlp_scan_stats`, `read_otlp_options`

## How It Works

```
OpenTelemetry     File         DuckDB OTLP          SQL
Collector      Exporter       Extension          Results
   │              │               │                 │
   │  OTLP/gRPC   │               │                 │
   ├─────────────►│  .jsonl/.pb   │                 │
   │              ├──────────────►│  read_otlp_*()  │
   │              │               ├────────────────►│
   │              │               │                 │
```

The extension reads OTLP files (JSON or protobuf), detects the format automatically, and streams strongly-typed rows into DuckDB tables. Schemas match the ClickHouse exporter format for compatibility.

## Core Components

### Format Detector

**Location:** `src/parsers/format_detector.cpp/hpp`

Sniffs the first bytes of a file or stream to decide between JSON and protobuf payloads. Supports automatic format detection for all table functions.

### JSON Parser

**Location:** `src/parsers/json_parser.cpp/hpp`

Streaming newline-delimited JSON parser that emits typed rows. Handles:
- JSONL files (one OTLP message per line)
- JSON arrays
- Single JSON documents

### Protobuf Parser

**Location:** `src/parsers/protobuf_parser.cpp/hpp`

Wraps generated OTLP protobuf stubs and uses shared row-builder helpers to produce DuckDB vectors. Requires protobuf runtime (not available in WASM builds).

### Row Builders

**Location:** `src/receiver/row_builders*.cpp`

Convert OTLP message structures into vectors that match the ClickHouse-compatible schemas. Each signal type (traces, logs, metrics) has dedicated row builders that:
- Extract fields from OTLP messages
- Convert to appropriate DuckDB types
- Handle nested structures (attributes, events, links)
- Compute derived fields (e.g., Duration for traces)

### Schema Definitions

**Location:** `src/schema/*.hpp`

Centralized column layouts for traces, logs, and metrics, including the union schema used by `read_otlp_metrics`. See the [Schema Reference](reference/schemas.md) for complete column details.

## Data Flow

```
User: SELECT * FROM read_otlp_metrics('metrics.pb')
  ↓
FormatDetector::DetectFormat()
  ↓
Choose JSON parser or protobuf parser
  ↓
Parsers populate typed row builders
  ↓
DuckDB emits DataChunks with strongly-typed columns
```

## Key Design Decisions

### ClickHouse-Compatible Schema

The table functions emit the OpenTelemetry ClickHouse exporter schema:

- **Traces**: 22 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration
- **Logs**: 15 columns with severity, body, resource/scope maps, and trace correlation fields
- **Metrics**: Union schema (27 columns) containing a `MetricType` discriminator plus all type-specific payloads

This compatibility ensures users can migrate between ClickHouse and DuckDB, or use both systems with the same data.

### Union Table Strategy

Instead of creating separate tables for each metric type, `read_otlp_metrics` returns a union schema. Users can split this into typed archive tables with simple SQL:

```sql
CREATE TABLE metrics_gauge AS
SELECT *
FROM read_otlp_metrics('otel-export/telemetry.jsonl')
WHERE MetricType = 'gauge';
```

The helper functions (`read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}`) call the union reader internally and project typed schemas for convenience.

### Streaming Architecture

The extension streams data through DuckDB's scan interface without loading entire files into memory. This enables:
- Processing files larger than available RAM
- Low memory footprint for large datasets
- Efficient glob pattern scanning across many files

## File Organization

```
src/
├── include/           # Public headers (forwarding to implementation dirs)
├── storage/           # Extension entry point registration
├── function/          # Table function implementations (`read_otlp_*`)
├── parsers/           # JSON/protobuf parsers and format detector
├── receiver/          # Shared row builders and OTLP helpers
├── schema/            # Column layout helpers
├── generated/         # Protobuf message stubs (DO NOT EDIT)
└── wasm/              # Stubs for JSON-only builds

test/
├── sql/               # SQLLogicTests (primary test format)
└── data/              # Test data (OTLP JSON/protobuf files)

demo/
├── index.html         # Browser-based demo application
├── app.js             # DuckDB-WASM integration code
├── style.css          # Demo styling
├── otlp.duckdb_extension.wasm  # WASM build of extension
└── samples/           # Sample OTLP JSONL files for testing
```

## Generated Code

The `src/generated/` directory contains protobuf message stubs generated from OpenTelemetry `.proto` files (`*.pb.h`, `*.pb.cc`). These provide the message types consumed by `parsers/protobuf_parser.cpp`.

**Do not edit these files directly.** They are excluded from formatting and linting.

## Dependencies

Managed via VCPKG:
- **Protobuf** - Wire format parsing for binary OTLP files (optional for JSON-only builds)

Python dependencies (via `uv`):
- `black` - Python formatting
- `clang-format` / `clang-tidy` - C++ formatting/linting
- `pre-commit` - Git hooks

## Known Limitations

- Live OTLP ingestion via gRPC has been removed; only file-based workloads are supported
- **WASM builds support JSON format only**. Protobuf parsing is only available in native builds
- Protobuf parsing requires linking against the protobuf runtime (available in native builds)
- The metrics table function emits a union schema; consumers must project out the desired metric shapes manually when creating persistent tables

## Building

See [CONTRIBUTING.md](../CONTRIBUTING.md) for build instructions.

## See Also

- [API Reference](reference/api.md) - Table function signatures and parameters
- [Schema Reference](reference/schemas.md) - Complete column layouts
- [ClickHouse Compatibility](clickhouse-compatibility.md) - Migration guide
