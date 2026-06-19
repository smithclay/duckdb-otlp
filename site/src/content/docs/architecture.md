---
title: "Architecture"
---

This page describes the internal architecture of the DuckDB OpenTelemetry Extension.

## Overview

The DuckDB OpenTelemetry Extension exposes typed readers for OpenTelemetry Protocol (OTLP) telemetry files. It reads traces, logs, and metrics from JSON and protobuf OTLP exports.

## Extension Type

The extension includes:
- Table functions: `read_otlp_traces`, `read_otlp_logs`
- Metrics functions: `read_otlp_metrics_gauge`, `read_otlp_metrics_sum`, `read_otlp_metrics_histogram`, `read_otlp_metrics_exp_histogram`
- Live ingest functions: `otlp_serve` (OTLP/HTTP, or OTLP/gRPC with `transport := 'grpc'`), `otap_serve` (OTAP/Arrow streaming), `otlp_flush`, `otlp_stop`, `otlp_server_list`, `otlp_seal_list` (native builds only). See [Live Ingest Server](#otlp-http-ingest-server)
- OTAP file readers: `read_otap_traces`, `read_otap_logs`, `read_otap_metrics_gauge`, `read_otap_metrics_sum`, `read_otap_metrics_histogram`, `read_otap_metrics_exp_histogram` — decode OTAP `BatchArrowRecords` files into the same schemas as the `read_otlp_*` readers

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

The extension reads OTLP files (JSON or protobuf), detects the format, and streams typed rows into DuckDB tables. Schemas use `snake_case` names and follow the OpenTelemetry ClickHouse exporter shape.

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

Alongside the file readers, the extension can run an embedded server that accepts live OpenTelemetry exports and streams them into a DuckDB catalog: the connection's default in-memory/file catalog, an attached DuckLake lakehouse, or another writable catalog such as an Iceberg REST catalog. The extension registers it as a **storage extension**, so the database owns running servers and tears them down with it. See the [Serve Reference](../reference/serve/) for the SQL API.

The server requires a native build (WASM omits it). Three wire protocols feed the same buffering/seal core: **OTLP/HTTP** (`otlp_serve`, the default), **OTLP/gRPC** unary (`otlp_serve(transport := 'grpc')`), and **OTAP/Arrow** bidirectional streaming (`otap_serve`). The HTTP transport uses httplib; both gRPC families run on an embedded tonic server bridged in via `src/otlp_server_grpc.cpp`.

The server **buffers ingest and commits rows in batches**. Worker threads validate, convert, and append rows into an in-memory buffer, then return `202`. A single background writer commits the buffer to the target in one transaction. That model keeps the DuckLake path practical: one Parquet data file per signal per batch commit, and one serialized writer that avoids DuckLake optimistic-concurrency retries.

### Components

| Component | Location | Role |
|-----------|----------|------|
| `OtlpServer` | `src/otlp_server.cpp` / `src/include/otlp_server.hpp` | Base server: token validation/auth, content-type → format selection, Arrow → DuckDB conversion, the in-memory buffer, and the background writer that commits batches into the target catalog. |
| `HttpOtlpServer` | `src/otlp_server.cpp` | `OtlpServer` subclass wrapping httplib. Owns the worker pool and the `/v1/logs`, `/v1/traces`, `/v1/metrics`, `/healthz`, and `/readyz` routes; binds the socket synchronously so callers see bind failures. |
| gRPC transport bridge | `src/otlp_server_grpc.cpp` | Bridges the embedded tonic gRPC server (in `otlp2records`) into the same buffering/seal core via a per-batch C callback. A `service_flags` arg selects the disjoint service family per listener: OTLP/gRPC unary `Export` (for `otlp_serve(transport := 'grpc')`) or OTAP/Arrow streaming (for `otap_serve`). |
| `OtlpStorageExtensionInfo` | `src/include/otlp_storage.hpp` | Database-scoped registry of running servers (keyed by scheme-aware listen URI). Backs `CreateServer` / `FlushServer` / `StopServer` / `ListServers`, and stops every server when the database closes. It cannot commit buffered rows at that point; see the durability note below. |
| Lifecycle functions | `src/otlp_start_stop.cpp` | The `otlp_serve`, `otap_serve`, `otlp_flush`, `otlp_stop`, `otlp_server_list`, and `otlp_seal_list` table functions that drive the registry. |

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

A single background writer commits the buffer to the target catalog when any trigger fires: admitted request-body bytes reach the internal size threshold, 64 MiB today; the oldest buffered row reaches the internal age limit, about 5 seconds today; or a caller runs `otlp_flush`. Each batch commit is one transaction.

- A `202` is **not durable**; rows become durable at the next background commit, on `otlp_stop`, or on `otlp_flush`. A crash loses buffered-but-uncommitted rows (at-most-once for that window).
- `otlp_stop` and `otlp_flush` commit remaining buffered rows before returning, so those calls lose no accepted rows. **A plain database/connection close does NOT commit buffered rows**. The shutdown drain runs after DuckDB tears down the instance, when `db_ptr` can no longer write, so DuckDB can drop buffered rows. Prefer `otlp_stop` before closing the database. Use `otlp_flush` when the server should stay running but readers need durable rows now. The project tracks a durable raw-spool journal and earlier shutdown hook for at-least-once delivery.
- After successful automatic row-seals into a named catalog, the writer may run non-force `CHECKPOINT <catalog>` as best-effort catalog-native maintenance when recent ingest rate and pending bytes leave ample admission headroom. The writer runs maintenance after the ingest transaction commits. It skips the default catalog; sustained high ingest and high pending buffered bytes defer maintenance; explicit `otlp_flush` and shutdown drains skip the hook. The server logs unsupported catalog implementations once and disables maintenance for that server. DuckLake uses this hook to apply its own maintenance policy; `duckdb-otlp` has no custom compaction planner.

### Concurrency model

The server uses a bounded httplib worker pool like `duckdb-quack`, but sends all target writes through one background thread. The pool parses, converts, and buffers requests **concurrently**; each signal table has its own buffer lock. Serial writes let a DuckLake target avoid tiny-file churn and optimistic-concurrency retries. **Backpressure:** if request admission would exceed `max_buffered_bytes` (default 512 MiB) across in-flight and uncommitted accepted payloads, POSTs return `503` before parse/transform work and clients should retry with backoff.

## Key Design Decisions

### Schema Design

The table functions emit schemas inspired by the OpenTelemetry ClickHouse exporter, with all column names in `snake_case`:

- **Traces**: 24 columns covering identifiers, scope metadata, resource attributes, events, links, and computed duration
- **Logs**: 18 columns with severity, body, resource/scope maps, and trace correlation fields
- **Metrics (gauge)**: 17 columns with timestamp, service info, metric metadata, and numeric value fields
- **Metrics (sum)**: 19 columns (gauge columns plus aggregation temporality and is_monotonic)
- **Metrics (histogram)**: 22 columns with counts, sum, min/max, explicit bounds, and bucket counts
- **Metrics (exponential histogram)**: 27 columns with scale, zero bucket, and positive/negative bucket data

### Streaming Architecture

The extension streams data through DuckDB's scan interface without loading entire files into memory. This supports:
- Files larger than available RAM
- Lower memory use for large datasets
- Glob pattern scans across many files

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

**Do not edit these files directly.** The build excludes them from formatting and linting.

## Dependencies

Managed via VCPKG:
- **Protobuf** - Wire format parsing for binary OTLP files in builds that include protobuf support

Python dependencies (via `uv`):
- `black` - Python formatting
- `clang-format` / `clang-tidy` - C++ formatting/linting
- `pre-commit` - Git hooks

## Known Limitations

- Live ingestion supports **OTLP/HTTP**, **OTLP/gRPC** (`otlp_serve(transport := 'grpc')`), and **OTAP/Arrow** bidirectional streaming (`otap_serve`), for all six signals. All transports share one buffering/seal core and the transport-agnostic lifecycle functions (`otlp_flush` / `otlp_stop` / `otlp_server_list` / `otlp_seal_list`). See [Live Ingest Server](#otlp-http-ingest-server). Ingest is buffered (an accepted request returns HTTP `202` / gRPC `OK`) and durable at the next background commit or graceful `otlp_stop`; a crash loses buffered-but-uncommitted rows (at-most-once). The project tracks a durable raw-spool journal for at-least-once delivery. The server requires a native build (WASM omits it entirely).
- **WASM builds support JSON, JSONL, and protobuf file reads only**. Native builds add live ingest.
- Protobuf parsing requires the protobuf runtime in builds that include protobuf support.
- Summary metrics are registered placeholders
- The union metrics function (`read_otlp_metrics`) is a registered placeholder; use the shape-specific metric readers

## Building

See [CONTRIBUTING.md](https://github.com/smithclay/duckdb-otlp/blob/main/CONTRIBUTING.md) for build instructions.

## See Also

- [API Reference](../reference/api/) - Table function signatures and parameters
- [Schema Reference](../reference/schemas/) - Complete column layouts
