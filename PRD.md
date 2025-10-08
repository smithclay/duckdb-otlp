# DuckSpan: OpenTelemetry for DuckDB

A DuckDB extension that makes OpenTelemetry data queryable with SQL. Attach OTLP streams as databases.

## User Experience

### Attach Live OTLP Stream

Attach an OTLP endpoint as a self-populating database:

```sql
-- Load the extension
LOAD duckspan;

-- Attach OTLP stream - creates receiver + database
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);

-- Query accumulated telemetry (auto-populated via gRPC)
SELECT * FROM live.traces;
SELECT * FROM live.metrics;
SELECT * FROM live.logs;

-- Detach stops receiver and removes database
DETACH live;
```

**What happens:**
- `ATTACH` creates a gRPC receiver on port 4317
- Creates tables: `live.traces`, `live.metrics`, `live.logs`
- OpenTelemetry SDKs send data → DuckDB accumulates it
- `DETACH` stops the receiver and destroys the database

**This is like `:memory:` but populated by OTLP instead of INSERT.**

### Read OTLP Files

Load historical OpenTelemetry data from files (JSON or protobuf):

```sql
-- Load the extension
LOAD duckspan;

-- Read OTLP JSON files
SELECT * FROM read_otlp('traces.jsonl');
SELECT * FROM read_otlp('s3://bucket/traces/*.jsonl');
SELECT * FROM read_otlp('https://example.com/traces.jsonl');

-- Read OTLP protobuf files (auto-detected)
SELECT * FROM read_otlp('traces.binpb');
SELECT * FROM read_otlp('s3://bucket/traces/*.pb');

-- Query the data (format is transparent)
SELECT
    json_extract(resource, '$.service.name') as service,
    json_extract(data, '$.name') as span_name,
    timestamp
FROM read_otlp('traces.jsonl')
WHERE timestamp > NOW() - INTERVAL 1 HOUR;
```

Format detection is automatic based on file content.

### Persist Stream Data

Capture live data into permanent tables:

```sql
-- Attach stream
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);

-- Let data accumulate for a while...

-- Persist to main database
CREATE TABLE permanent_traces AS SELECT * FROM live.traces;
CREATE TABLE permanent_metrics AS SELECT * FROM live.metrics;

-- Stop receiving
DETACH live;

-- Query persisted data
SELECT * FROM permanent_traces WHERE timestamp > NOW() - INTERVAL 1 DAY;
```

### Data Retention (⚠️ TBD - Open Design Decision)

**Problem:** Attached OTLP streams accumulate data in memory indefinitely, which will eventually cause OOM.

```sql
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);
-- Data keeps flowing in from apps...
-- After hours/days: memory exhaustion
```

Unlike `:memory:` databases where the user controls insertions, OTLP streams receive data from external sources continuously. This requires explicit retention management.

**Workarounds for Initial Implementation:**

```sql
-- Option 1: Manual cleanup with scheduled DELETE
DELETE FROM live.traces WHERE timestamp < NOW() - INTERVAL 1 HOUR;

-- Option 2: Periodic persist-and-detach cycle
CREATE TABLE archive_traces AS SELECT * FROM live.traces;
DETACH live;  -- Clears memory
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);  -- Start fresh

-- Option 3: Rolling window view
CREATE VIEW recent_traces AS
SELECT * FROM live.traces
WHERE timestamp > NOW() - INTERVAL 15 MINUTES;
```

**Future Design Options (To Be Decided):**

1. **Automatic TTL**
   ```sql
   ATTACH 'otlp://localhost:4317' AS live (TYPE otlp, max_age '1 hour');
   -- Auto-deletes data older than 1 hour
   ```

2. **Row-based limits**
   ```sql
   ATTACH 'otlp://localhost:4317' AS live (TYPE otlp, max_rows 1000000);
   -- Keeps only most recent 1M spans (circular buffer)
   ```

3. **Persistent backing**
   ```sql
   ATTACH 'otlp://localhost:4317' AS live (TYPE otlp, storage 'telemetry.db');
   -- Writes to disk database instead of memory
   ```

4. **Automatic archival**
   ```sql
   ATTACH 'otlp://localhost:4317' AS live (
       TYPE otlp,
       archive_to 's3://bucket/telemetry/',
       archive_interval '1 hour'
   );
   -- Automatically exports old data to S3 Parquet
   ```

**Decision Required:** Choose default retention strategy before v1.0 release.

## Data Model

All telemetry signals use the same unified schema:

```sql
-- Every signal table (traces, metrics, logs)
CREATE TABLE {traces|metrics|logs} (
    timestamp TIMESTAMP,    -- When the event occurred
    resource JSON,          -- Service/host metadata
    data JSON              -- Signal-specific payload
);
```

**Example data:**

```sql
SELECT * FROM live.traces LIMIT 1;

-- timestamp: 2024-01-15 10:30:00
-- resource: {"service.name": "api-server", "host.name": "prod-01"}
-- data: {
--   "traceId": "abc123...",
--   "spanId": "def456...",
--   "name": "GET /users",
--   "startTimeUnixNano": "1640000000000000000",
--   "endTimeUnixNano": "1640000000100000000",
--   "attributes": {"http.method": "GET"}
-- }
```

**Storage Design:**
- Protobuf ingested → JSON stored (for queryability)
- Timestamps: nanoseconds → microseconds precision
- IDs: bytes → hex strings
- Attributes: nested JSON preserved

## DuckDB Patterns

This extension follows standard DuckDB conventions:

| Pattern | DuckDB Examples | DuckSpan |
|---------|-----------------|----------|
| **Attach databases** | `ATTACH 'file.db'`<br>`ATTACH ':memory:'`<br>`ATTACH 'md:mydb'` | `ATTACH 'otlp://localhost:4317' AS live` |
| **Read files** | `read_csv('file.csv')`<br>`read_parquet('*.parquet')` | `read_otlp('traces.jsonl')` |
| **Query JSON** | `json_extract(data, '$.field')` | `json_extract(data, '$.traceId')` |
| **Schema prefix** | `my_db.my_table` | `live.traces` |
| **Lifecycle** | `DETACH my_db` | `DETACH live` |

### ATTACH Semantics Explained

ATTACH in DuckDB can both **connect to** and **create** databases:

```sql
-- Connects to existing
ATTACH 'postgres://server/db' AS pg;

-- Creates new database
ATTACH ':memory:' AS temp;           -- Creates in-memory DB
ATTACH 'md:mydb' AS cloud;           -- Creates MotherDuck DB
ATTACH 'otlp://localhost:4317' AS live;  -- Creates stream-populated DB
```

DuckSpan's ATTACH creates a **self-populating database** - the gRPC receiver is an implementation detail, not the main abstraction.

## Common Queries

```sql
-- Find slow requests
SELECT
    json_extract(resource, '$.service.name') as service,
    json_extract(data, '$.name') as endpoint,
    (json_extract(data, '$.endTimeUnixNano')::BIGINT -
     json_extract(data, '$.startTimeUnixNano')::BIGINT) / 1000000 as duration_ms
FROM live.traces
WHERE duration_ms > 1000
ORDER BY duration_ms DESC;

-- Error rate by service
SELECT
    json_extract(resource, '$.service.name') as service,
    COUNT(*) as total_spans,
    SUM(CASE WHEN json_extract(data, '$.status.code') != 0 THEN 1 ELSE 0 END) as errors,
    (errors::FLOAT / total_spans * 100)::INT as error_pct
FROM live.traces
WHERE timestamp > NOW() - INTERVAL 15 MINUTES
GROUP BY service;

-- Trace waterfall analysis
WITH spans AS (
    SELECT
        json_extract(data, '$.traceId') as trace_id,
        json_extract(data, '$.spanId') as span_id,
        json_extract(data, '$.parentSpanId') as parent_span_id,
        json_extract(data, '$.name') as operation,
        (json_extract(data, '$.endTimeUnixNano')::BIGINT -
         json_extract(data, '$.startTimeUnixNano')::BIGINT) / 1000000 as duration_ms
    FROM live.traces
    WHERE timestamp > NOW() - INTERVAL 5 MINUTES
)
SELECT * FROM spans
WHERE trace_id = 'abc123...'
ORDER BY duration_ms DESC;

-- Join with business data
SELECT
    u.email,
    COUNT(DISTINCT json_extract(t.data, '$.traceId')) as num_requests,
    AVG((json_extract(t.data, '$.endTimeUnixNano')::BIGINT -
         json_extract(t.data, '$.startTimeUnixNano')::BIGINT) / 1000000) as avg_duration_ms
FROM users u
JOIN live.traces t ON json_extract(t.data, '$.userId') = u.id
WHERE t.timestamp > NOW() - INTERVAL 1 DAY
GROUP BY u.email;
```

## Requirements

### Functional

**F1: OTLP Stream Attachment**
- Attach syntax: `ATTACH 'otlp://host:port' AS name (TYPE otlp)`
- Creates gRPC receiver on specified port
- Auto-creates tables: `{name}.traces`, `{name}.metrics`, `{name}.logs`
- Lifecycle: Start on ATTACH, stop on DETACH
- Accepts standard OTLP v1.0 protobuf protocol

**F2: File Reading**
- Table function: `read_otlp(filepath)`
- Supports DuckDB VFS (local, S3, HTTP, Azure, GCS)
- Streams large files (no memory limits)
- Format support:
  - OTLP JSON (JSON Lines format, one object per line)
  - OTLP Protobuf (binary protocol buffer format)
- Auto-detects format from file content (magic bytes/structure)

**F3: Unified Schema**
- Same structure for all signal types: `(timestamp, resource, data)`
- JSON storage for queryability (not binary protobuf)
- Compatible with DuckDB's JSON functions
- Indexed on timestamp for range queries

### Non-Functional

**Performance**
- Handle 10,000+ spans/second sustained
- Stream files larger than available RAM
- Sub-100ms p99 insert latency
- Batch insertions for efficiency

**Reliability**
- Atomic batch inserts per gRPC request (all-or-nothing)
- Clean shutdown on DETACH (no thread leaks)
- Graceful handling of malformed OTLP data
- Transaction rollback on errors

**Compatibility**
- OTLP v1.0 specification compliance
- Works with standard OpenTelemetry SDKs (Java, Python, Go, JS, etc.)
- DuckDB 1.4.0+ extension API
- Standard SQL for queries

**Data Retention (⚠️ TBD)**
- Initial implementation: unbounded in-memory accumulation
- User responsible for managing retention (manual DELETE or periodic DETACH/ATTACH)
- Future: automatic TTL, row limits, or persistent backing
- See "Data Retention" section for details and workarounds

## Architecture

### Extension Type
Scanner extension registered for `TYPE otlp` (similar to `postgres_scanner`, `mysql_scanner`)

### Components

**1. ATTACH Handler**
- Parses `otlp://host:port` connection string
- Starts gRPC server on specified port
- Creates tables in attached schema
- Stores attachment state for DETACH cleanup

**2. gRPC Receiver**
- Implements OTLP collector service endpoints:
  - `opentelemetry.proto.collector.trace.v1.TraceService/Export`
  - `opentelemetry.proto.collector.metrics.v1.MetricsService/Export`
  - `opentelemetry.proto.collector.logs.v1.LogsService/Export`
- Runs in background thread (managed by extension)
- Converts protobuf → JSON
- Batches inserts for performance

**3. Table Functions**
- `read_otlp(filepath)` - streams OTLP files (JSON or protobuf)
- Auto-detects format from file content
- Returns same schema as attached tables
- Lazy evaluation (parsing during query execution)

**4. Data Conversion**
- Input formats: OTLP JSON or OTLP Protobuf
- Output format: Unified JSON for storage
- Timestamp: nanoseconds → microseconds
- Binary IDs → hex strings
- Preserves nested attributes as JSON

### Lifecycle Management

```
ATTACH 'otlp://localhost:4317' AS live
    ↓
1. Parse connection string (host, port)
2. Create gRPC server thread
3. Bind to port 4317
4. CREATE SCHEMA live
5. CREATE TABLE live.traces/metrics/logs
6. Register attachment in DuckDB catalog
    ↓
    [gRPC receiver runs in background]
    [Apps send OTLP data]
    [Batches inserted into tables]
    ↓
DETACH live
    ↓
1. Stop gRPC server
2. Wait for pending requests
3. DROP SCHEMA live CASCADE
4. Join receiver thread
5. Remove from catalog
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **JSON storage** | SQL queryability > storage efficiency |
| **Unified schema** | Generic pattern for all signals |
| **ATTACH semantics** | Database abstraction matches use case |
| **Background thread** | Non-blocking concurrent ingestion |
| **Batch insertions** | Transaction per gRPC request for atomicity |
| **TYPE otlp scanner** | Follows DuckDB extension patterns |

### What Makes This Work

1. **Self-populating database**: The "database" is data accumulated from a stream
2. **ATTACH creates resources**: Precedent in `:memory:`, MotherDuck
3. **Clean abstraction**: Users see tables, not gRPC details
4. **Standard SQL**: Query telemetry like any other data

## Implementation Notes

**Base Template:** Use `duckdb/extension-template`

**Dependencies:**
- gRPC (for OTLP receiver)
- Protobuf (for OTLP wire format)
- nlohmann/json (for JSON storage)

**Key Patterns:**
- Scanner extension registration (`TYPE otlp`)
- ATTACH/DETACH lifecycle hooks
- Background thread with RAII cleanup
- Protobuf → JSON conversion pipeline
- Batch appender for inserts

**Must Avoid:**
- Global mutable state (use ATTACH context)
- Thread leaks on DETACH (strict RAII)
- Breaking unified schema (all signals same structure)
- Protobuf in database (defeats queryability)

**Memory Management:**
- `shared_ptr` for connection sharing
- `unique_ptr` for owned resources
- RAII for gRPC server lifecycle
- Transaction guards for rollback safety

## Success Criteria

- [ ] `LOAD duckspan` loads without errors
- [ ] `ATTACH 'otlp://localhost:4317' AS live` starts receiver
- [ ] Tables auto-created: `live.traces`, `live.metrics`, `live.logs`
- [ ] OpenTelemetry SDK can export to localhost:4317
- [ ] Data appears in tables within 1 second of export
- [ ] `SELECT * FROM live.traces` returns accumulated spans
- [ ] `DETACH live` stops receiver cleanly (no hanging threads)
- [ ] `read_otlp('file.jsonl')` parses OTLP JSON files
- [ ] `read_otlp('file.binpb')` parses OTLP protobuf files
- [ ] Format auto-detection works correctly
- [ ] Data retention workarounds documented (manual cleanup patterns)
- [ ] Memory growth behavior documented (unbounded accumulation warning)
- [ ] No memory leaks over 24-hour run (valgrind clean)
- [ ] 10,000 spans/second sustained ingestion
- [ ] Sub-100ms p99 batch insert latency

## References

- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)
- [DuckDB Extension Template](https://github.com/duckdb/extension-template)
- [DuckDB Scanner Extensions](https://github.com/duckdb/postgres_scanner)
- [OpenTelemetry Protocol](https://github.com/open-telemetry/opentelemetry-proto)

---

*Goal: Make OpenTelemetry streams feel like native DuckDB databases.*
