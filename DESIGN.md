# DuckSpan Design Document

## Overview

DuckSpan is a DuckDB extension that makes OpenTelemetry data queryable with SQL. It follows DuckDB's design philosophy by extending existing patterns (ATTACH for databases, table functions for file reading) rather than inventing new abstractions.

**Core Capabilities:**
1. **Read OTLP files**: `SELECT * FROM read_otlp('traces.jsonl')`
2. **Stream live data**: `ATTACH 'otlp://localhost:4317' AS live (TYPE otlp)`
3. **Query with SQL**: Standard DuckDB SQL over telemetry signals

## Architectural Principles

### Conceptual Integrity

DuckSpan follows the "self-populating database" pattern:
- `ATTACH 'otlp://...'` creates a database that fills itself via gRPC
- Similar to `:memory:` (creates empty DB) and `postgres_scanner` (connects to external DB)
- Users see tables (`live.traces`), not implementation details (gRPC server)
- Lifecycle managed by DuckDB's ATTACH/DETACH commands

### Data Model

**Unified schema for all signal types:**
```sql
CREATE TABLE {traces|metrics|logs} (
    timestamp TIMESTAMP,    -- Event occurrence time (microsecond precision)
    resource JSON,          -- Service/host metadata
    data JSON              -- Signal-specific payload
);
```

**Design rationale:**
- **JSON storage**: SQL queryability > storage efficiency
- **Unified structure**: Generic pattern works for all telemetry signals
- **Microsecond timestamps**: Converted from nanoseconds (DuckDB standard)
- **Preserved nesting**: Attributes remain as nested JSON for flexible querying

**Example data:**
```json
{
  "timestamp": "2024-01-15T10:30:00",
  "resource": {"service.name": "api-server", "host.name": "prod-01"},
  "data": {
    "traceId": "abc123...",
    "spanId": "def456...",
    "name": "GET /users",
    "startTimeUnixNano": "1640000000000000000",
    "endTimeUnixNano": "1640000000100000000",
    "attributes": {"http.method": "GET"}
  }
}
```

### Critical Architectural Decision: Memory Retention

**Problem**: Attached OTLP streams accumulate data indefinitely → OOM

**Solution** (based on Brooks review): **Ring buffer with row-based limits**

```sql
-- Implementation approach
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);
-- Creates tables with 100,000 row limit per signal type (default)
-- FIFO eviction: oldest rows dropped when limit reached
```

**Why ring buffer over alternatives:**
1. **Simpler than TTL**: No timestamp-based cleanup logic
2. **Predictable memory**: Fixed upper bound
3. **No persistence complexity**: Stays in-memory as intended
4. **No archival pipeline**: User controls data export explicitly

**User workarounds for long-term storage:**
```sql
-- Periodic persist-and-reset
CREATE TABLE archive_traces AS SELECT * FROM live.traces;
DETACH live;
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);

-- Or manual cleanup
DELETE FROM live.traces WHERE timestamp < NOW() - INTERVAL 1 HOUR;
```

## Implementation Phases

### Phase 1: read_otlp() for JSON Files

**Goal**: Establish data model without concurrency complexity

**Components:**
- `src/read_otlp.cpp` - TableFunction implementation
- `src/otlp_schema.cpp` - Schema definition (timestamp, resource, data)
- Uses DuckDB's TableFunction API for streaming

**File format**: JSON Lines (one OTLP object per line)

**Success criteria:**
```sql
SELECT * FROM read_otlp('traces.jsonl');
SELECT * FROM read_otlp('s3://bucket/traces/*.jsonl');  -- DuckDB VFS support
```

**Testing:**
- `test/sql/read_otlp_json.test` - SQLLogicTests
- Sample files: `test/data/traces.jsonl`, `test/data/metrics.jsonl`

### Phase 2: Protobuf Support

**Goal**: Validate protobuf → JSON conversion pipeline

**Components:**
- `src/otlp_conversion.cpp` - Protobuf → JSON converters
- Format auto-detection from magic bytes/structure
- Support all 3 signal types: traces, metrics, logs

**Dependencies:**
- `grpc` - For protobuf definitions
- `protobuf` - For parsing binary format

**Success criteria:**
```sql
SELECT * FROM read_otlp('traces.binpb');  -- Auto-detects protobuf
-- Same results as JSON version
```

**Testing:**
- `test/sql/read_otlp_protobuf.test`
- Binary test files: `test/data/traces.binpb`
- Verify identical output from JSON and protobuf sources

### Phase 3: Ring Buffer Storage

**Goal**: Solve unbounded growth BEFORE adding streaming

**Components:**
- `src/ring_buffer_table.hpp/cpp` - Fixed-size circular buffer
- Default: 100,000 rows per table
- FIFO eviction policy
- Snapshot isolation for queries during wraparound

**Key design:**
```cpp
class RingBufferTable {
    size_t max_rows;           // Capacity limit
    size_t write_index;        // Next insert position
    size_t read_index;         // Oldest valid row
    vector<Row> buffer;        // Fixed-size storage
    mutex lock;                // Concurrent access protection
};
```

**Concurrency model:**
- Readers: MVCC snapshot isolation
- Writers: Single-writer lock on insert
- Queries during wraparound: Consistent view of circular buffer

**Success criteria:**
- Memory plateaus after reaching max_rows
- No unbounded growth under continuous inserts
- Queries return correct results during buffer wraparound

**Testing:**
- `test/sql/ring_buffer.test` - Wraparound behavior
- Load test: Insert 1M rows, verify memory ≤ 100K rows worth
- Concurrent query + insert test

### Phase 4: ATTACH/DETACH Lifecycle

**Goal**: Validate catalog integration and lifecycle management

**Components:**
- `src/otlp_scanner.hpp/cpp` - Scanner extension for TYPE otlp
- Parse `otlp://host:port` connection string
- Create schema with ring buffer tables
- RAII cleanup on DETACH

**ATTACH sequence:**
```
1. Parse connection string → (host, port)
2. CREATE SCHEMA {name}
3. CREATE TABLE {name}.traces (ring buffer)
4. CREATE TABLE {name}.metrics (ring buffer)
5. CREATE TABLE {name}.logs (ring buffer)
6. Register in DuckDB catalog
```

**DETACH sequence:**
```
1. DROP SCHEMA {name} CASCADE
2. Remove from catalog
3. RAII cleanup of resources
```

**Success criteria:**
```sql
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);
SELECT * FROM live.traces;  -- Empty but queryable
DETACH live;
```

**Testing:**
- `test/sql/attach_detach.test`
- Lifecycle test: 1000 ATTACH/DETACH cycles
- Valgrind: Zero memory leaks
- Verify tables accessible after ATTACH
- Verify clean removal after DETACH

### Phase 5: gRPC Receiver Integration

**Goal**: Complete streaming implementation

**Components:**
- `src/otlp_receiver.hpp/cpp` - gRPC service implementation
- Implements OTLP collector endpoints:
  - `TraceService/Export`
  - `MetricsService/Export`
  - `LogsService/Export`

**Lifecycle management:**
- Start gRPC server on ATTACH
- Graceful shutdown on DETACH (wait for in-flight requests)
- Use DuckDB's task scheduler (avoid raw threads)

**Concurrency model:**
- gRPC handlers: Convert protobuf → JSON
- Batch insert: One transaction per gRPC request (atomic)
- Background task: Scheduled via DuckDB task scheduler
- Shutdown: Flush pending batches before stopping

**Success criteria:**
```sql
-- In terminal 1
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);

-- In terminal 2 (OpenTelemetry SDK exports spans)
-- Data appears in DuckDB within 1 second

-- In terminal 1
SELECT COUNT(*) FROM live.traces;  -- Growing count
```

**Testing:**
- `test/integration/otlp_export.py` - OpenTelemetry SDK test client
- `test/sql/live_ingestion.test` - End-to-end streaming
- Performance test: 10,000 spans/sec sustained
- Latency test: p99 insert < 100ms
- Load + query test: Query performance during active ingestion

## Key Design Decisions

### 1. JSON Storage (not Protobuf)

**Rationale**: SQL queryability trumps storage efficiency

```sql
-- Easy with JSON
SELECT json_extract(data, '$.traceId') FROM traces;

-- Hard with binary protobuf
SELECT protobuf_extract(data, 'trace_id', 'TracesData') FROM traces;  -- Not standard SQL
```

**Trade-offs:**
- ✅ Standard SQL functions work
- ✅ Human-readable in database
- ✅ Flexible schema evolution
- ❌ Larger storage footprint
- ❌ Conversion overhead on ingest

**Decision**: Queryability > efficiency for analytics use case

### 2. Unified Schema (not signal-specific columns)

**Rationale**: Follows DuckDB's semi-structured data patterns

**Alternative considered**: Signal-specific schemas
```sql
-- Rejected approach
CREATE TABLE traces (
    trace_id VARCHAR,
    span_id VARCHAR,
    name VARCHAR,
    start_time BIGINT,
    end_time BIGINT,
    attributes JSON,
    ...  -- 20+ columns
);
```

**Problems with alternative:**
- OTLP schema evolution breaks extension
- Different column sets for traces/metrics/logs
- Attributes flattening loses nesting

**Chosen approach**: Generic schema + JSON functions
```sql
-- Flexible and future-proof
CREATE TABLE traces (
    timestamp TIMESTAMP,
    resource JSON,
    data JSON  -- All span fields nested here
);
```

### 3. Ring Buffer Retention (not TTL/archival)

**Rationale**: Simplest solution that prevents OOM

**Alternatives rejected for v1:**
1. **TTL-based cleanup**: Requires background thread for eviction
2. **Persistent backing**: Defeats in-memory streaming purpose
3. **Automatic archival**: Adds S3/filesystem complexity

**Chosen approach**: Row-based FIFO buffer
- Predictable memory usage
- No background cleanup threads
- User controls long-term storage explicitly

### 4. DuckDB Task Scheduler (not raw threads)

**Rationale**: Avoid accidental complexity from manual thread management

**Alternative considered**: `std::thread` for gRPC server

**Problems with alternative:**
- Thread lifecycle coupling to ATTACH/DETACH
- Risk of thread leaks on errors
- Manual synchronization with database transactions

**Chosen approach**: DuckDB's task scheduler
- Managed lifecycle
- Automatic cleanup
- Integrated with DuckDB's concurrency model

## Architectural Guardrails

### Memory Safety
- **Rule**: Memory must plateau after reaching max_rows
- **Metric**: RSS memory per attached database
- **Test**: Insert 1M rows, verify memory ≤ 100K rows worth
- **Stop condition**: Memory growth >1GB/hour with 1000 spans/sec

### Thread Safety
- **Rule**: Zero thread leaks on DETACH
- **Metric**: Thread count before/after DETACH
- **Test**: 1000 ATTACH/DETACH cycles under valgrind
- **Stop condition**: Any thread leak detected

### Performance
- **Rule**: Sub-100ms p99 insert latency
- **Metric**: Insert latency percentiles (p50, p95, p99)
- **Test**: 10,000 spans/sec for 5 minutes
- **Stop condition**: p99 >100ms or query degradation >50%

## Testing Strategy

### Unit Tests (SQLLogicTests)
- `test/sql/read_otlp_json.test` - JSON file reading
- `test/sql/read_otlp_protobuf.test` - Protobuf parsing
- `test/sql/ring_buffer.test` - Buffer wraparound
- `test/sql/attach_detach.test` - Lifecycle management
- `test/sql/live_ingestion.test` - End-to-end streaming

### Integration Tests
- `test/integration/otlp_export.py` - OpenTelemetry SDK client
- Real OTLP exports from Python/Go/Java SDKs
- Verify data appears in DuckDB correctly

### Load Tests
- 10,000 spans/sec sustained for 5 minutes
- Memory plateau verification
- Concurrent query + insert workload

### Reliability Tests
- 1000 ATTACH/DETACH cycles (no leaks)
- Valgrind clean on shutdown
- Error handling: malformed OTLP data, port conflicts

## Dependencies

### Build Dependencies (vcpkg.json)
```json
{
  "dependencies": [
    "grpc",
    "protobuf",
    "nlohmann-json"
  ]
}
```

**Removed**: `openssl` (not needed for OTLP)

### Runtime Dependencies
- DuckDB 1.4.0+
- OTLP protocol v1.0 compliance

## Success Criteria

**Phase 1 (read_otlp JSON):**
- [ ] `SELECT * FROM read_otlp('traces.jsonl')` works
- [ ] S3/HTTP paths supported via DuckDB VFS
- [ ] Streams files larger than RAM

**Phase 2 (Protobuf):**
- [ ] `read_otlp('traces.binpb')` auto-detects format
- [ ] Identical results from JSON and protobuf sources
- [ ] All 3 signal types supported

**Phase 3 (Ring Buffer):**
- [ ] Memory plateaus at max_rows limit
- [ ] No unbounded growth under continuous inserts
- [ ] Queries work correctly during wraparound

**Phase 4 (ATTACH/DETACH):**
- [ ] `ATTACH 'otlp://localhost:4317' AS live` creates tables
- [ ] Tables are queryable (empty initially)
- [ ] `DETACH live` cleans up without leaks
- [ ] 1000 cycles pass valgrind

**Phase 5 (gRPC Receiver):**
- [ ] OpenTelemetry SDK can export to DuckDB
- [ ] Data appears within 1 second of export
- [ ] 10,000 spans/sec sustained ingestion
- [ ] Sub-100ms p99 insert latency
- [ ] Clean shutdown on DETACH

## Future Enhancements (Post-v1)

**Configuration options:**
```sql
ATTACH 'otlp://localhost:4317' AS live (
    TYPE otlp,
    max_rows 1000000,      -- Custom buffer size
    tls_cert 'cert.pem'    -- TLS support
);
```

**Signal-specific optimizations:**
- Columnar storage for metrics (time-series optimization)
- Trace ID indexing for span lookups
- Log sampling for high-volume signals

**Advanced retention:**
- TTL-based eviction
- Automatic archival to S3/Parquet
- Persistent backing option

**Observability:**
- Extension metrics (ingestion rate, buffer utilization)
- Slow query logging
- Receiver health endpoints

## References

- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)
- [DuckDB Extension Template](https://github.com/duckdb/extension-template)
- [DuckDB Scanner Extensions](https://github.com/duckdb/postgres_scanner)
- [OpenTelemetry Protocol](https://github.com/open-telemetry/opentelemetry-proto)
- [Brooks Architectural Review](internal)

---

**Design Philosophy**: Make OpenTelemetry streams feel like native DuckDB databases through careful extension of existing patterns.
