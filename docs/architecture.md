# Architecture

This document describes the internal architecture of the DuckDB OTLP extension.

## Overview

The DuckDB OTLP extension is a **table-function extension** that exposes strongly-typed readers for OpenTelemetry Protocol (OTLP) telemetry files. It supports reading traces, logs, and metrics from JSON and protobuf OTLP exports.

## Extension Type

The extension provides:
- Table functions: `read_otlp_traces`, `read_otlp_logs`
- Metrics functions: `read_otlp_metrics_gauge`, `read_otlp_metrics_sum`
- Live ingest functions: `otlp_serve`, `otlp_flush`, `otlp_stop`, `otlp_server_list` (native builds only) — see [OTLP HTTP Ingest Server](#otlp-http-ingest-server)

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
User: SELECT * FROM read_otlp_metrics_gauge('metrics.pb')
  ↓
FormatDetector::DetectFormat()
  ↓
Choose JSON parser or protobuf parser
  ↓
Parsers populate typed row builders
  ↓
DuckDB emits DataChunks with strongly-typed columns
```

## OTLP HTTP Ingest Server

Alongside the file readers, the extension can run an embedded HTTP server that accepts live OTLP/HTTP exports and streams them into a DuckDB catalog — most usefully an attached **DuckLake** lakehouse (Parquet + catalog), or the connection's default in-memory/file catalog. It is registered as a **storage extension** so the running servers are owned by the database and torn down with it. See the [Serve Reference](reference/serve.md) for the user-facing surface.

> Not available in WASM builds. Live ingestion is HTTP-only (no gRPC).

Ingest is **buffered and group-committed** ("sealed"), not per-request. Worker threads validate, convert, and append rows into an in-memory buffer (returning `202`); a single background sealer thread group-commits the buffer to the target in one transaction. This is what makes the DuckLake path viable: one Parquet data file per signal per seal (instead of per request), and a single serialized writer that avoids DuckLake's optimistic-concurrency retries.

### Components

| Component | Location | Role |
|-----------|----------|------|
| `OtlpServer` | `src/otlp_server.cpp` / `src/include/otlp_server.hpp` | Base server: token validation/auth, content-type → format selection, Arrow → DuckDB conversion, the in-memory buffer, and the background sealer that group-commits into the target catalog. |
| `HttpOtlpServer` | `src/otlp_server.cpp` | `OtlpServer` subclass wrapping httplib. Owns the worker pool and the `/v1/logs`, `/v1/traces`, `/v1/metrics`, and `/healthz` routes; binds the socket synchronously so bind failures surface to the caller. |
| `OtlpStorageExtensionInfo` | `src/include/otlp_storage.hpp` | Database-scoped registry of running servers (keyed by listen URI). Backs `CreateServer` / `FlushServer` / `StopServer` / `ListServers`, and stops (sealing first) every server when the database closes. |
| Lifecycle functions | `src/otlp_start_stop.cpp` | The `otlp_serve`, `otlp_flush`, `otlp_stop`, and `otlp_server_list` table functions that drive the registry. |

### Request flow

```
Exporter: POST http://localhost:4318/v1/logs  (Bearer token, OTLP body)
  ↓
HttpOtlpServer route → CheckAuth (Bearer or x-api-key)
  ↓
FormatFromContentType (json / ndjson / protobuf)
  ↓
Worker thread: otlp_transform (FFI) → convert → append to in-memory buffer (brief lock)
  ↓
202 {"status":"buffered","rows":N,"batches":M}   (NOT yet durable)

... asynchronously ...

Sealer thread (trigger: seal_target_bytes / seal_max_age_ms / otlp_flush)
  → one transaction → for DuckLake: one Parquet data file per signal + one snapshot → COMMIT
```

### Seal model and durability

A single background **sealer** thread group-commits the buffer to the target catalog when any trigger fires: buffered bytes reach `seal_target_bytes` (default 64 MiB), the oldest buffered row reaches `seal_max_age_ms` (default 5000 ms), or an explicit `otlp_flush`. Each seal is one transaction.

- A `202` is **not durable**; rows become durable at the next seal (within `seal_max_age_ms`, or immediately on `otlp_flush`). A crash loses buffered-but-unsealed rows (at-most-once for that window).
- Graceful `otlp_stop` / database close seals remaining rows first, so a clean shutdown loses nothing. (A durable raw-spool journal for at-least-once is a future enhancement, not implemented.)
- `otlp_flush(uri, checkpoint := true)` seals synchronously and then runs DuckLake compaction (`ducklake_merge_adjacent_files` + `CHECKPOINT`) to merge the small per-seal Parquet files.

### Concurrency model

Mirrors `duckdb-quack`'s worker pool but inverts the writer: a 128-thread httplib pool parses, converts, and buffers requests **concurrently** (the only shared state is the buffer, guarded by a brief lock), while a **single sealer thread is the only writer** to the target catalog. Serializing all writes through one thread is what lets a DuckLake target avoid tiny-file churn and optimistic-concurrency retries. **Backpressure:** when total buffered bytes exceed `max_buffered_bytes` (default 512 MiB), POSTs return `503` and clients should retry with backoff.

## Key Design Decisions

### Schema Design

The table functions emit schemas inspired by the OpenTelemetry ClickHouse exporter, with all column names in `snake_case`:

- **Traces**: 25 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration
- **Logs**: 15 columns with severity, body, resource/scope maps, and trace correlation fields
- **Metrics (gauge)**: 16 columns with timestamp, service info, metric metadata, and value
- **Metrics (sum)**: 18 columns (gauge columns plus aggregation temporality and is_monotonic)

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

- Live OTLP ingestion is supported over **HTTP** (`otlp_serve` / `otlp_flush` / `otlp_stop` / `otlp_server_list`), not gRPC. See [OTLP HTTP Ingest Server](#otlp-http-ingest-server). Ingest is buffered (a POST returns `202`) and only durable at the next seal; a crash loses buffered-but-unsealed rows (at-most-once). A durable raw-spool journal for at-least-once is a future enhancement. The server is not available in WASM builds.
- **WASM builds support JSON format only**. Protobuf parsing is only available in native builds
- Protobuf parsing requires linking against the protobuf runtime (available in native builds)
- Histogram, exponential histogram, and summary metrics are not yet supported
- The union metrics function (`read_otlp_metrics`) is not yet implemented

## Building

See [CONTRIBUTING.md](../CONTRIBUTING.md) for build instructions.

## See Also

- [API Reference](reference/api.md) - Table function signatures and parameters
- [Schema Reference](reference/schemas.md) - Complete column layouts
- [ClickHouse Compatibility](clickhouse-compatibility.md) - Migration guide
