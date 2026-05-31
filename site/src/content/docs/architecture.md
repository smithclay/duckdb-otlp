---
title: "Architecture"
---

This document describes the internal architecture of the DuckDB OpenTelemetry Extension.

## Overview

The DuckDB OpenTelemetry Extension is a **table-function extension** that exposes strongly-typed readers for OpenTelemetry Protocol (OTLP) telemetry files. It supports reading traces, logs, and metrics from JSON and protobuf OTLP exports.

## Extension Type

The extension provides:
- Table functions: `read_otlp_traces`, `read_otlp_logs`
- Metrics functions: `read_otlp_metrics_gauge`, `read_otlp_metrics_sum`, `read_otlp_metrics_histogram`, `read_otlp_metrics_exp_histogram`
- Live ingest functions: `otlp_serve`, `otlp_flush`, `otlp_stop`, `otlp_server_list` (native builds only) — see [OTLP HTTP Ingest Server](#otlp-http-ingest-server)

## How It Works

```
OpenTelemetry     File        duckdb-otlp          SQL
Collector      Exporter       Extension          Results
   │              │               │                 │
   │  OTLP/gRPC   │               │                 │
   ├─────────────►│  .jsonl/.pb   │                 │
   │              ├──────────────►│  read_otlp_*()  │
   │              │               ├────────────────►│
   │              │               │                 │
```

The extension reads OTLP files (JSON or protobuf), detects the format automatically, and streams strongly-typed rows into DuckDB tables. Schemas use `snake_case` names and are inspired by the OpenTelemetry ClickHouse exporter.

## Core Components

### Rust Backend

**Location:** `external/otlp2records`

Parses OTLP JSON, JSONL, and protobuf payloads and emits Arrow arrays through the Arrow C Data Interface.

### Arrow Bridge

**Location:** `src/otlp_arrow.cpp`, `src/function/read_otlp.cpp`

Converts Arrow arrays from the Rust backend into DuckDB `DataChunk`s for the table functions.

### Schema Definitions

**Location:** `src/schema/*.hpp`

Centralized column layouts for traces, logs, and shape-specific metrics. See the [Schema Reference](../reference/schemas/) for complete column details.

## Data Flow

```
User: SELECT * FROM read_otlp_metrics_gauge('metrics.pb')
  ↓
otlp2records parses OTLP JSON/protobuf into Arrow arrays
  ↓
Arrow bridge copies arrays into DuckDB DataChunks
```

## OTLP HTTP Ingest Server

Alongside the file readers, the extension can run an embedded HTTP server that accepts live OTLP/HTTP exports and streams them into a DuckDB catalog: the connection's default in-memory/file catalog, an attached DuckLake lakehouse, or another writable catalog such as an Iceberg REST catalog. It is registered as a **storage extension** so the running servers are owned by the database and torn down with it. See the [Serve Reference](../reference/serve/) for the user-facing surface.

> Not available in WASM builds. Live ingestion is HTTP-only (no gRPC).

Ingest is **buffered and committed in batches**, not per-request. Worker threads validate, convert, and append rows into an in-memory buffer (returning `202`); a single background writer commits the buffer to the target in one transaction. This is what makes the DuckLake path viable: one Parquet data file per signal per batch commit (instead of per request), and a single serialized writer that avoids DuckLake's optimistic-concurrency retries.

### Components

| Component | Location | Role |
|-----------|----------|------|
| `OtlpServer` | `src/otlp_server.cpp` / `src/include/otlp_server.hpp` | Base server: token validation/auth, content-type → format selection, Arrow → DuckDB conversion, the in-memory buffer, and the background writer that commits batches into the target catalog. |
| `HttpOtlpServer` | `src/otlp_server.cpp` | `OtlpServer` subclass wrapping httplib. Owns the worker pool and the `/v1/logs`, `/v1/traces`, `/v1/metrics`, and `/healthz` routes; binds the socket synchronously so bind failures surface to the caller. |
| `OtlpStorageExtensionInfo` | `src/include/otlp_storage.hpp` | Database-scoped registry of running servers (keyed by listen URI). Backs `CreateServer` / `FlushServer` / `StopServer` / `ListServers`, and stops every server when the database closes (but cannot commit buffered rows at that point — see durability note below). |
| Lifecycle functions | `src/otlp_start_stop.cpp` | The `otlp_serve`, `otlp_flush`, `otlp_stop`, and `otlp_server_list` table functions that drive the registry. |

### Request flow

```
Exporter: POST http://localhost:4318/v1/logs  (Bearer token, OTLP body)
  ↓
HttpOtlpServer route → CheckAuth (Bearer or x-api-key)
  ↓
FormatFromContentType (json / ndjson / protobuf)
  ↓
Worker thread: reserve admission bytes -> otlp_transform (FFI) -> convert -> append to per-signal buffer
  ↓
202 {"status":"buffered","rows":N,"batches":M}   (NOT yet durable)

... asynchronously ...

Background writer (trigger: internal size/age threshold; optional otlp_flush)
  → one transaction → for DuckLake: one Parquet data file per signal + one snapshot → COMMIT
  → after conservative automatic row-seal cadence when there is admission headroom: best-effort CHECKPOINT <catalog> outside the ingest transaction
```

### Batch commit model and durability

A single background writer commits the buffer to the target catalog when any trigger fires: admitted request-body bytes reach the internal size threshold (currently 64 MiB), the oldest buffered row reaches the internal age limit (currently about 5 seconds), or an explicit optional `otlp_flush`. Each batch commit is one transaction.

- A `202` is **not durable**; rows become durable at the next automatic background commit, on `otlp_stop`, or immediately on `otlp_flush`. A crash loses buffered-but-uncommitted rows (at-most-once for that window).
- `otlp_stop` and `otlp_flush` commit remaining buffered rows before returning, so those lose nothing. **A plain database/connection close does NOT commit buffered rows** — the shutdown drain runs after the DuckDB instance is torn down (when `db_ptr` can no longer write), so buffered rows can be dropped. Prefer `otlp_stop` before closing the database; use `otlp_flush` only when the server should stay running but readers need durable rows now. (A durable raw-spool journal / earlier shutdown hook for at-least-once is a tracked follow-up.)
- After successful automatic row-seals into a named catalog, the writer may occasionally run non-force `CHECKPOINT <catalog>` as best-effort catalog-native maintenance when recent ingest rate and pending bytes leave ample admission headroom. This happens after the ingest transaction commits, never inside it. The default catalog is skipped, sustained high ingest and high pending buffered bytes defer it, explicit `otlp_flush` and shutdown drains do not trigger it, and unsupported catalog implementations are logged once and disabled for that server. DuckLake uses this hook to apply its own maintenance policy; `duckdb-otlp` does not implement a custom compaction planner.

### Concurrency model

Mirrors `duckdb-quack`'s worker pool but inverts the writer: a 128-thread httplib pool parses, converts, and buffers requests **concurrently** (each signal table has its own buffer lock), while a **single background writer thread is the only writer** to the target catalog. Serializing all writes through one thread is what lets a DuckLake target avoid tiny-file churn and optimistic-concurrency retries. **Backpressure:** if request admission would exceed `max_buffered_bytes` (default 512 MiB) across in-flight and uncommitted accepted payloads, POSTs return `503` before parse/transform work and clients should retry with backoff.

## Key Design Decisions

### Schema Design

The table functions emit schemas inspired by the OpenTelemetry ClickHouse exporter, with all column names in `snake_case`:

- **Traces**: 25 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration
- **Logs**: 15 columns with severity, body, resource/scope maps, and trace correlation fields
- **Metrics (gauge)**: 16 columns with timestamp, service info, metric metadata, and value
- **Metrics (sum)**: 18 columns (gauge columns plus aggregation temporality and is_monotonic)
- **Metrics (histogram)**: 22 columns with counts, sum, min/max, explicit bounds, and bucket counts
- **Metrics (exponential histogram)**: 27 columns with scale, zero bucket, and positive/negative bucket data

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
├── schema/            # Column layout helpers
├── generated/         # Protobuf message stubs (DO NOT EDIT)
└── wasm/              # WASM build configuration

test/
├── sql/               # SQLLogicTests (primary test format)
└── data/              # Test data (OTLP JSON/protobuf files)

site/
├── src/content/docs/  # Astro/Starlight documentation pages
└── public/wasm-demo/  # Browser demo, WASM extension, and sample OTLP files
```

## Generated Code

The `src/generated/` directory contains protobuf message stubs generated from OpenTelemetry `.proto` files (`*.pb.h`, `*.pb.cc`). These provide the message types consumed by `parsers/protobuf_parser.cpp`.

**Do not edit these files directly.** They are excluded from formatting and linting.

## Dependencies

Managed via VCPKG:
- **Protobuf** - Wire format parsing for binary OTLP files in builds that include protobuf support

Python dependencies (via `uv`):
- `black` - Python formatting
- `clang-format` / `clang-tidy` - C++ formatting/linting
- `pre-commit` - Git hooks

## Known Limitations

- Live OTLP ingestion is supported over **HTTP** (`otlp_serve` / `otlp_flush` / `otlp_stop` / `otlp_server_list`), not gRPC. See [OTLP HTTP Ingest Server](#otlp-http-ingest-server). Ingest is buffered (a POST returns `202`) and durable at the next automatic background commit or graceful `otlp_stop`; a crash loses buffered-but-uncommitted rows (at-most-once). A durable raw-spool journal for at-least-once is a future enhancement. The server is not available in WASM builds.
- **WASM builds support JSON, JSONL, and protobuf file reads only**. Live ingest is only available in native builds.
- Protobuf parsing requires the protobuf runtime in builds that include protobuf support.
- Summary metrics are not yet supported
- The union metrics function (`read_otlp_metrics`) is not yet implemented; use the shape-specific metric readers

## Building

See [CONTRIBUTING.md](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md) for build instructions.

## See Also

- [API Reference](../reference/api/) - Table function signatures and parameters
- [Schema Reference](../reference/schemas/) - Complete column layouts
