# DuckSpan Implementation Tasks

Detailed breakdown of all implementation tasks across 5 phases. Each task is independently testable and builds on previous tasks.

## Phase 1: read_otlp() for JSON Files

### Task 1.1: Define Unified Schema
**Files to create:**
- `src/include/otlp_schema.hpp`
- `src/otlp_schema.cpp`

**Implementation:**
```cpp
// otlp_schema.hpp
struct OTLPSchema {
    static vector<LogicalType> GetTypes();  // [TIMESTAMP, JSON, JSON]
    static vector<string> GetNames();       // ["timestamp", "resource", "data"]
    static void CreateTable(Connection &con, const string &table_name);
};
```

**Success criteria:**
- Schema definition compiles
- Returns correct column types and names

**Testing:**
- Unit test in `test/sql/schema.test`

**Dependencies:** None (foundational)

---

### Task 1.2: Implement JSON Parser
**Files to create:**
- `src/include/json_parser.hpp`
- `src/json_parser.cpp`

**Implementation:**
```cpp
// Parse OTLP JSON Lines format
class OTLPJSONParser {
public:
    bool ParseLine(const string &line, Timestamp &ts, string &resource, string &data);
    bool IsValidOTLPJSON(const string &line);
};
```

**Success criteria:**
- Parses valid OTLP JSON line → (timestamp, resource, data)
- Rejects malformed JSON
- Handles missing fields gracefully

**Testing:**
- Unit test with sample OTLP JSON
- Error cases: malformed JSON, missing required fields

**Dependencies:** None

---

### Task 1.3: Create Test Data Files
**Files to create:**
- `test/data/traces_simple.jsonl` - Single trace with 3 spans
- `test/data/metrics_simple.jsonl` - Counter + gauge metrics
- `test/data/logs_simple.jsonl` - Log records
- `test/data/malformed.jsonl` - Invalid JSON for error testing

**Implementation:**
Generate valid OTLP JSON Lines files with realistic data:
```json
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"test-service"}}]},"scopeSpans":[{"spans":[{"traceId":"abc123","spanId":"def456","name":"GET /users","startTimeUnixNano":"1640000000000000000","endTimeUnixNano":"1640000000100000000"}]}]}]}
```

**Success criteria:**
- Files parse correctly with standard JSON tools
- Conform to OTLP JSON spec
- Cover all 3 signal types

**Testing:**
- Validate against OTLP JSON schema
- Parse with jq/python to verify structure

**Dependencies:** None

---

### Task 1.4: Implement read_otlp() Table Function
**Files to create:**
- `src/include/read_otlp.hpp`
- `src/read_otlp.cpp`

**Files to modify:**
- `src/duckspan_extension.cpp` - Register table function

**Implementation:**
```cpp
// read_otlp.hpp
class ReadOTLPFunction : public TableFunction {
public:
    static unique_ptr<FunctionData> Bind(ClientContext &context,
                                          vector<Value> &inputs);
    static void Execute(ClientContext &context,
                        FunctionData *bind_data,
                        DataChunk &output);
};
```

**Key features:**
- Stream file line-by-line (no memory limits)
- Use DuckDB's VFS for file access (S3/HTTP support)
- Parse each line with OTLPJSONParser
- Populate output DataChunk with unified schema

**Success criteria:**
```sql
SELECT * FROM read_otlp('test/data/traces_simple.jsonl');
-- Returns rows with (timestamp, resource, data) columns
```

**Testing:**
- `test/sql/read_otlp_json.test`
- Local file paths
- S3 paths (if AWS credentials available)
- HTTP URLs

**Dependencies:** Task 1.1 (schema), Task 1.2 (parser), Task 1.3 (test data)

---

### Task 1.5: Add Error Handling
**Files to modify:**
- `src/read_otlp.cpp`

**Implementation:**
- Malformed JSON: Skip line + warning (don't fail entire query)
- Missing file: Clear error message with path
- Empty file: Return 0 rows (not an error)
- Invalid OTLP structure: Skip line + warning

**Success criteria:**
```sql
SELECT * FROM read_otlp('nonexistent.jsonl');
-- Error: File not found: nonexistent.jsonl

SELECT * FROM read_otlp('test/data/malformed.jsonl');
-- Returns valid rows, skips malformed lines
-- Warning: Skipped 3 malformed lines
```

**Testing:**
- Error test cases in `test/sql/read_otlp_errors.test`

**Dependencies:** Task 1.4

---

### Task 1.6: Update Extension Registration
**Files to modify:**
- `src/duckspan_extension.cpp`
- `CMakeLists.txt`

**Implementation:**
```cpp
// duckspan_extension.cpp
static void LoadInternal(ExtensionLoader &loader) {
    // Register read_otlp table function
    auto read_otlp_func = ReadOTLPFunction::GetFunction();
    loader.RegisterFunction(read_otlp_func);
}
```

```cmake
# CMakeLists.txt
set(EXTENSION_SOURCES
    src/duckspan_extension.cpp
    src/otlp_schema.cpp
    src/json_parser.cpp
    src/read_otlp.cpp
)
```

**Success criteria:**
- Extension compiles
- `LOAD duckspan` loads without errors
- `read_otlp` function is available

**Testing:**
- `make test` passes
- Function shows up in `SELECT * FROM duckdb_functions() WHERE function_name = 'read_otlp'`

**Dependencies:** All Phase 1 tasks

---

## Phase 2: Protobuf Support

### Task 2.1: Add Dependencies
**Files to modify:**
- `vcpkg.json`
- `CMakeLists.txt`

**Implementation:**
```json
// vcpkg.json
{
  "dependencies": [
    "grpc",
    "protobuf",
    "nlohmann-json"
  ]
}
```

```cmake
# CMakeLists.txt
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)
find_package(nlohmann_json REQUIRED)

target_link_libraries(${EXTENSION_NAME}
    gRPC::grpc++
    protobuf::libprotobuf
    nlohmann_json::nlohmann_json
)
```

**Success criteria:**
- Dependencies build via vcpkg
- Extension links successfully

**Testing:**
- `make clean && make` succeeds

**Dependencies:** None

---

### Task 2.2: Generate OTLP Protobuf Code
**Files to create:**
- `src/proto/` - Directory for generated code
- `scripts/generate_proto.sh` - Code generation script

**Implementation:**
```bash
#!/bin/bash
# Download OTLP proto definitions
OTLP_VERSION=v1.0.0
wget https://github.com/open-telemetry/opentelemetry-proto/archive/refs/tags/${OTLP_VERSION}.tar.gz
tar -xzf ${OTLP_VERSION}.tar.gz

# Generate C++ code
protoc --cpp_out=src/proto \
    opentelemetry-proto-${OTLP_VERSION}/opentelemetry/proto/**/*.proto
```

**Success criteria:**
- Generated .pb.h and .pb.cc files in `src/proto/`
- Files compile without errors

**Testing:**
- Include generated headers in test file, verify compilation

**Dependencies:** Task 2.1

---

### Task 2.3: Implement Protobuf Parser
**Files to create:**
- `src/include/protobuf_parser.hpp`
- `src/protobuf_parser.cpp`

**Implementation:**
```cpp
// protobuf_parser.hpp
class OTLPProtobufParser {
public:
    bool ParseTracesData(const string &binary, vector<Row> &output);
    bool ParseMetricsData(const string &binary, vector<Row> &output);
    bool ParseLogsData(const string &binary, vector<Row> &output);

private:
    string ProtobufToJSON(const google::protobuf::Message &msg);
};
```

**Key features:**
- Parse binary protobuf → Message objects
- Convert Message → JSON string (for storage)
- Extract timestamp fields
- Extract resource attributes

**Success criteria:**
- Parses valid OTLP protobuf binary
- Outputs same JSON structure as Phase 1 parser
- Handles all 3 signal types

**Testing:**
- Unit test with sample protobuf binaries
- Compare output with JSON parser (should match)

**Dependencies:** Task 2.2

---

### Task 2.4: Implement Format Auto-Detection
**Files to create:**
- `src/include/format_detector.hpp`
- `src/format_detector.cpp`

**Implementation:**
```cpp
enum class OTLPFormat {
    JSON,
    PROTOBUF,
    UNKNOWN
};

class FormatDetector {
public:
    static OTLPFormat DetectFormat(const char *data, size_t len);
};
```

**Detection logic:**
- JSON: First byte is `{` or `[`
- Protobuf: First bytes match protobuf field encoding
- Read first 1KB of file for detection

**Success criteria:**
- Correctly identifies JSON vs protobuf
- Returns UNKNOWN for invalid data

**Testing:**
- Test with JSON and protobuf files
- Test with empty file, binary garbage

**Dependencies:** None

---

### Task 2.5: Update read_otlp() for Protobuf
**Files to modify:**
- `src/read_otlp.cpp`

**Implementation:**
```cpp
void ReadOTLPFunction::Execute(...) {
    // Detect format
    auto format = FormatDetector::DetectFormat(file_data, file_size);

    if (format == OTLPFormat::JSON) {
        // Use JSON parser (existing)
        json_parser.Parse(line, output);
    } else if (format == OTLPFormat::PROTOBUF) {
        // Use protobuf parser (new)
        protobuf_parser.Parse(binary_data, output);
    }
}
```

**Success criteria:**
```sql
SELECT * FROM read_otlp('traces.jsonl');   -- Uses JSON parser
SELECT * FROM read_otlp('traces.binpb');   -- Uses protobuf parser
-- Both return same schema
```

**Testing:**
- `test/sql/read_otlp_protobuf.test`
- Verify identical results from JSON and protobuf sources

**Dependencies:** Task 2.3, Task 2.4

---

### Task 2.6: Create Protobuf Test Data
**Files to create:**
- `test/data/traces_simple.binpb`
- `test/data/metrics_simple.binpb`
- `test/data/logs_simple.binpb`
- `scripts/generate_test_protos.py`

**Implementation:**
```python
# Generate protobuf test files using OpenTelemetry SDK
from opentelemetry.proto.trace.v1 import trace_pb2

traces_data = trace_pb2.TracesData()
# ... populate with test data
with open('test/data/traces_simple.binpb', 'wb') as f:
    f.write(traces_data.SerializeToString())
```

**Success criteria:**
- Binary files parse correctly with protobuf parser
- Contain same logical data as JSON test files

**Testing:**
- Verify with protoc decoder
- Compare output with JSON version

**Dependencies:** Task 2.2

---

## Phase 3: Ring Buffer Storage

### Task 3.1: Design Ring Buffer Interface
**Files to create:**
- `src/include/ring_buffer_table.hpp`

**Implementation:**
```cpp
class RingBufferTable {
public:
    RingBufferTable(size_t max_rows);

    void Insert(DataChunk &chunk);
    void Scan(DataChunk &output, size_t &scan_offset);
    size_t GetRowCount() const;

private:
    size_t max_rows_;
    size_t write_index_;
    size_t current_size_;
    vector<DataChunk> buffer_;
    mutex lock_;
};
```

**Key decisions:**
- Fixed capacity set at construction
- FIFO eviction (overwrites oldest)
- Thread-safe via mutex
- Scan uses offset for iteration

**Success criteria:**
- Interface compiles
- Clear semantics documented

**Testing:**
- Header-only test (compilation)

**Dependencies:** None

---

### Task 3.2: Implement Ring Buffer Insert
**Files to create:**
- `src/ring_buffer_table.cpp`

**Implementation:**
```cpp
void RingBufferTable::Insert(DataChunk &chunk) {
    lock_guard<mutex> lock(lock_);

    for (size_t i = 0; i < chunk.size(); i++) {
        size_t pos = write_index_ % max_rows_;
        buffer_[pos] = chunk.GetRow(i);

        write_index_++;
        if (current_size_ < max_rows_) {
            current_size_++;
        }
    }
}
```

**Key features:**
- Circular buffer wrapping via modulo
- Atomic insert with lock
- Tracks current size (grows to max_rows then plateaus)

**Success criteria:**
- Insert N rows → buffer contains min(N, max_rows) rows
- Oldest rows evicted when full

**Testing:**
- Unit test: Insert 1000 rows into 100-row buffer
- Verify only last 100 rows remain

**Dependencies:** Task 3.1

---

### Task 3.3: Implement Ring Buffer Scan
**Files to modify:**
- `src/ring_buffer_table.cpp`

**Implementation:**
```cpp
void RingBufferTable::Scan(DataChunk &output, size_t &scan_offset) {
    lock_guard<mutex> lock(lock_);

    size_t start_index = (write_index_ - current_size_) % max_rows_;
    size_t rows_to_read = min(STANDARD_VECTOR_SIZE, current_size_ - scan_offset);

    for (size_t i = 0; i < rows_to_read; i++) {
        size_t pos = (start_index + scan_offset + i) % max_rows_;
        output.SetRow(i, buffer_[pos]);
    }

    scan_offset += rows_to_read;
}
```

**Key features:**
- Returns rows in logical order (oldest to newest)
- Handles circular buffer correctly
- Pagination via scan_offset

**Success criteria:**
- Scan returns all rows in buffer
- Order is correct (oldest first)
- Works correctly after wraparound

**Testing:**
- Unit test: Insert 1000 rows, scan, verify order
- Wraparound test: Verify logical order maintained

**Dependencies:** Task 3.2

---

### Task 3.4: Add Snapshot Isolation
**Files to modify:**
- `src/ring_buffer_table.cpp`

**Implementation:**
```cpp
class RingBufferTable {
    struct Snapshot {
        size_t write_index;
        size_t current_size;
    };

    Snapshot CreateSnapshot();
    void ScanSnapshot(Snapshot snap, DataChunk &output, size_t &offset);
};
```

**Key features:**
- Snapshot captures buffer state at point in time
- Queries see consistent view even during inserts
- Prevents reading partially-written data

**Success criteria:**
- Concurrent insert + scan don't interfere
- Query results are consistent

**Testing:**
- Concurrent test: Insert thread + scan thread
- Verify no partial reads

**Dependencies:** Task 3.3

---

### Task 3.5: Integrate with DuckDB Catalog
**Files to create:**
- `src/include/ring_buffer_catalog_entry.hpp`
- `src/ring_buffer_catalog_entry.cpp`

**Implementation:**
```cpp
class RingBufferCatalogEntry : public TableCatalogEntry {
public:
    unique_ptr<RingBufferTable> table;

    unique_ptr<BaseTableRef> GetTableRef(ClientContext &context) override;
};
```

**Key features:**
- Register ring buffer as DuckDB table
- Appears in catalog like regular table
- Supports SELECT queries

**Success criteria:**
```sql
-- After creating ring buffer table
SELECT * FROM my_ring_buffer;
SELECT COUNT(*) FROM my_ring_buffer;
```

**Testing:**
- Create ring buffer table, run queries
- Verify appears in `duckdb_tables()`

**Dependencies:** Task 3.4

---

### Task 3.6: Memory Plateau Test
**Files to create:**
- `test/sql/ring_buffer_memory.test`
- `scripts/memory_test.py`

**Implementation:**
```python
# Insert 1M rows into 100K row buffer
# Monitor RSS memory during insertion
# Verify memory plateaus after 100K rows

import duckdb
import psutil

con = duckdb.connect()
con.execute("LOAD duckspan")

process = psutil.Process()
memory_samples = []

for i in range(1000000):
    con.execute("INSERT INTO ring_buffer VALUES (...)")
    if i % 10000 == 0:
        memory_samples.append(process.memory_info().rss)

# Verify plateau: last 10 samples have <5% variance
assert plateau_detected(memory_samples)
```

**Success criteria:**
- Memory growth stops after max_rows reached
- No unbounded growth over 1M inserts

**Testing:**
- Load test with memory monitoring
- Valgrind for leak detection

**Dependencies:** Task 3.5

---

## Phase 4: ATTACH/DETACH Lifecycle

### Task 4.1: Create Scanner Extension Interface
**Files to create:**
- `src/include/otlp_scanner.hpp`
- `src/otlp_scanner.cpp`

**Implementation:**
```cpp
class OTLPScanner : public TableFunction {
public:
    static unique_ptr<FunctionData> AttachBind(ClientContext &context,
                                                const string &connection_string);
    static void DetachCleanup(unique_ptr<FunctionData> data);
};
```

**Key features:**
- Parse `otlp://host:port` connection strings
- Register as scanner for TYPE otlp
- Lifecycle hooks for ATTACH/DETACH

**Success criteria:**
- Interface compiles
- Can be registered with DuckDB

**Testing:**
- Compilation test

**Dependencies:** None

---

### Task 4.2: Implement Connection String Parser
**Files to create:**
- `src/include/otlp_connection.hpp`
- `src/otlp_connection.cpp`

**Implementation:**
```cpp
struct OTLPConnection {
    string host;
    uint16_t port;

    static OTLPConnection Parse(const string &connection_string);
};

// Parse "otlp://localhost:4317" -> {host: "localhost", port: 4317}
```

**Success criteria:**
- Parses valid connection strings
- Rejects invalid formats with clear errors
- Handles IPv4, IPv6, hostnames

**Testing:**
- Unit test with various formats
- Error cases: missing port, invalid host, etc.

**Dependencies:** None

---

### Task 4.3: Implement ATTACH Handler
**Files to modify:**
- `src/otlp_scanner.cpp`
- `src/duckspan_extension.cpp`

**Implementation:**
```cpp
unique_ptr<FunctionData> OTLPScanner::AttachBind(ClientContext &context,
                                                  const string &connection_string) {
    // Parse connection string
    auto conn = OTLPConnection::Parse(connection_string);

    // Create schema
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateSchema(context, schema_name);

    // Create ring buffer tables
    CreateRingBufferTable(context, schema_name + ".traces");
    CreateRingBufferTable(context, schema_name + ".metrics");
    CreateRingBufferTable(context, schema_name + ".logs");

    // Return bind data for cleanup
    return make_unique<OTLPBindData>(conn, schema_name);
}
```

**Success criteria:**
```sql
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);
-- Creates schema 'live'
-- Creates tables: live.traces, live.metrics, live.logs
```

**Testing:**
- `test/sql/attach_basic.test`
- Verify schema created
- Verify tables exist and are queryable

**Dependencies:** Task 4.2, Phase 3 (ring buffer tables)

---

### Task 4.4: Implement DETACH Handler
**Files to modify:**
- `src/otlp_scanner.cpp`

**Implementation:**
```cpp
void OTLPScanner::DetachCleanup(unique_ptr<FunctionData> data) {
    auto otlp_data = (OTLPBindData*)data.get();

    // Drop schema (cascades to tables)
    auto &catalog = Catalog::GetCatalog(context);
    catalog.DropSchema(context, otlp_data->schema_name);

    // RAII cleanup happens automatically
}
```

**Success criteria:**
```sql
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);
DETACH live;
-- Schema 'live' removed
-- Tables no longer exist
```

**Testing:**
- `test/sql/detach_basic.test`
- Verify schema removed
- Verify tables gone

**Dependencies:** Task 4.3

---

### Task 4.5: Add RAII Resource Management
**Files to create:**
- `src/include/otlp_bind_data.hpp`
- `src/otlp_bind_data.cpp`

**Implementation:**
```cpp
class OTLPBindData : public FunctionData {
public:
    OTLPConnection connection;
    string schema_name;

    ~OTLPBindData() {
        // RAII cleanup
        // Phase 5 will add gRPC server shutdown here
    }
};
```

**Success criteria:**
- Destructor called on DETACH
- No resource leaks

**Testing:**
- Valgrind: No leaks after DETACH

**Dependencies:** Task 4.4

---

### Task 4.6: Lifecycle Stress Test
**Files to create:**
- `test/sql/attach_detach_stress.test`
- `scripts/lifecycle_test.py`

**Implementation:**
```python
# 1000 ATTACH/DETACH cycles
import duckdb

con = duckdb.connect()
con.execute("LOAD duckspan")

for i in range(1000):
    con.execute("ATTACH 'otlp://localhost:4317' AS live (TYPE otlp)")
    con.execute("SELECT COUNT(*) FROM live.traces")  # Verify queryable
    con.execute("DETACH live")

# Run under valgrind
# Verify zero memory leaks
# Verify zero thread leaks
```

**Success criteria:**
- 1000 cycles complete without errors
- Valgrind reports zero leaks
- No fd leaks (check with lsof)

**Testing:**
- Automated stress test
- Memory profiling

**Dependencies:** Task 4.5

---

### Task 4.7: Register Scanner Extension
**Files to modify:**
- `src/duckspan_extension.cpp`

**Implementation:**
```cpp
static void LoadInternal(ExtensionLoader &loader) {
    // Register table function
    loader.RegisterFunction(ReadOTLPFunction::GetFunction());

    // Register scanner extension
    auto attach_info = make_unique<AttachFunctionInfo>();
    attach_info->type = "otlp";
    attach_info->bind = OTLPScanner::AttachBind;
    attach_info->detach = OTLPScanner::DetachCleanup;
    loader.RegisterAttachFunction(attach_info);
}
```

**Success criteria:**
- Extension loads successfully
- ATTACH with TYPE otlp works
- Shows up in `duckdb_extensions()`

**Testing:**
- End-to-end test: Load extension, ATTACH, query, DETACH

**Dependencies:** All Phase 4 tasks

---

## Phase 5: gRPC Receiver Integration

### Task 5.1: Implement OTLP Service Stubs
**Files to create:**
- `src/include/otlp_service.hpp`
- `src/otlp_service.cpp`

**Implementation:**
```cpp
class OTLPTraceService : public TraceService::Service {
public:
    Status Export(ServerContext* context,
                  const ExportTraceServiceRequest* request,
                  ExportTraceServiceResponse* response) override;

private:
    RingBufferTable *traces_table_;
};

// Similar for MetricsService and LogsService
```

**Success criteria:**
- gRPC service compiles
- Implements all required OTLP endpoints

**Testing:**
- Compilation test
- Link with gRPC successfully

**Dependencies:** Phase 2 (protobuf)

---

### Task 5.2: Implement Protobuf → JSON Conversion
**Files to modify:**
- `src/otlp_service.cpp`

**Implementation:**
```cpp
Status OTLPTraceService::Export(...) {
    // Convert protobuf request to JSON
    for (auto &resource_span : request->resource_spans()) {
        string resource_json = ResourceToJSON(resource_span.resource());

        for (auto &scope_span : resource_span.scope_spans()) {
            for (auto &span : scope_span.spans()) {
                string data_json = SpanToJSON(span);
                Timestamp ts = NanosToMicros(span.start_time_unix_nano());

                // Insert into ring buffer
                traces_table_->Insert({ts, resource_json, data_json});
            }
        }
    }

    response->set_success(true);
    return Status::OK;
}
```

**Success criteria:**
- Converts OTLP protobuf to unified schema
- Inserts into ring buffer correctly

**Testing:**
- Unit test with sample OTLP request
- Verify JSON output format

**Dependencies:** Task 5.1, Phase 3 (ring buffer)

---

### Task 5.3: Implement gRPC Server Lifecycle
**Files to create:**
- `src/include/otlp_server.hpp`
- `src/otlp_server.cpp`

**Implementation:**
```cpp
class OTLPServer {
public:
    OTLPServer(const string &host, uint16_t port,
               RingBufferTable *traces,
               RingBufferTable *metrics,
               RingBufferTable *logs);

    void Start();
    void Shutdown();  // Graceful shutdown

private:
    unique_ptr<Server> grpc_server_;
    OTLPTraceService trace_service_;
    OTLPMetricsService metrics_service_;
    OTLPLogsService logs_service_;
};
```

**Key features:**
- Start gRPC server on construction
- Graceful shutdown: wait for in-flight requests
- RAII pattern for cleanup

**Success criteria:**
- Server starts and binds to port
- Accepts OTLP gRPC requests
- Shuts down cleanly

**Testing:**
- Unit test: Start server, send request, shutdown
- Verify no thread leaks

**Dependencies:** Task 5.2

---

### Task 5.4: Integrate Server with ATTACH
**Files to modify:**
- `src/otlp_scanner.cpp`
- `src/otlp_bind_data.cpp`

**Implementation:**
```cpp
unique_ptr<FunctionData> OTLPScanner::AttachBind(...) {
    // ... create tables (existing code)

    // Start gRPC server
    auto server = make_unique<OTLPServer>(
        conn.host, conn.port,
        traces_table, metrics_table, logs_table
    );
    server->Start();

    // Store server in bind data for cleanup
    auto bind_data = make_unique<OTLPBindData>(conn, schema_name);
    bind_data->server = move(server);
    return bind_data;
}

// DETACH cleanup
OTLPBindData::~OTLPBindData() {
    if (server) {
        server->Shutdown();  // Wait for in-flight requests
    }
    // Schema drop happens in DetachCleanup
}
```

**Success criteria:**
```sql
ATTACH 'otlp://localhost:4317' AS live (TYPE otlp);
-- gRPC server starts on port 4317
-- Accepts OTLP exports

DETACH live;
-- gRPC server shuts down gracefully
-- No thread leaks
```

**Testing:**
- Integration test: ATTACH, send gRPC request, verify data, DETACH

**Dependencies:** Task 5.3, Phase 4 (ATTACH/DETACH)

---

### Task 5.5: Add Error Handling
**Files to modify:**
- `src/otlp_server.cpp`
- `src/otlp_service.cpp`

**Implementation:**
```cpp
// Port conflict detection
void OTLPServer::Start() {
    try {
        grpc_server_->AddListeningPort(
            server_address,
            grpc::InsecureServerCredentials()
        );
        grpc_server_->Start();
    } catch (const exception &e) {
        throw IOException("Failed to start OTLP receiver on port " +
                          to_string(port) + ": " + e.what() +
                          "\nPort may already be in use.");
    }
}

// Malformed OTLP handling
Status OTLPTraceService::Export(...) {
    try {
        // Conversion logic
    } catch (const exception &e) {
        // Log error, return error status
        return Status(StatusCode::INVALID_ARGUMENT,
                      "Malformed OTLP data: " + string(e.what()));
    }
}
```

**Success criteria:**
- Clear error on port conflict
- Graceful handling of malformed OTLP
- Transaction rollback on errors

**Testing:**
- Port conflict test: ATTACH twice on same port
- Malformed data test: Send invalid protobuf
- Error recovery test: Error doesn't crash extension

**Dependencies:** Task 5.4

---

### Task 5.6: Create Integration Test Client
**Files to create:**
- `test/integration/otlp_export.py`

**Implementation:**
```python
# OpenTelemetry SDK test client
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Configure exporter to DuckDB
exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

# Generate test spans
tracer = trace.get_tracer(__name__)
with tracer.start_as_current_span("test-span") as span:
    span.set_attribute("test.attribute", "value")

# Verify data in DuckDB
import duckdb
con = duckdb.connect()
con.execute("LOAD duckspan")
con.execute("ATTACH 'otlp://localhost:4317' AS live (TYPE otlp)")
time.sleep(1)  # Allow ingestion
result = con.execute("SELECT COUNT(*) FROM live.traces").fetchone()
assert result[0] > 0, "No spans received"
```

**Success criteria:**
- Python SDK exports to DuckDB successfully
- Data appears in tables within 1 second
- All span fields preserved correctly

**Testing:**
- Run as automated test
- Test with Python, Go, Java SDKs

**Dependencies:** Task 5.5

---

### Task 5.7: Performance Testing
**Files to create:**
- `test/performance/load_test.py`

**Implementation:**
```python
# Generate 10,000 spans/sec for 5 minutes
# Monitor insert latency percentiles
# Verify memory stability

import time
import statistics
from opentelemetry import trace

latencies = []
start_time = time.time()
target_duration = 300  # 5 minutes
target_rate = 10000  # spans/sec

while time.time() - start_time < target_duration:
    batch_start = time.time()

    # Generate 100 spans (batch)
    for _ in range(100):
        with tracer.start_as_current_span("load-test") as span:
            span.set_attribute("timestamp", time.time())

    batch_latency = time.time() - batch_start
    latencies.append(batch_latency)

    # Rate limiting to maintain 10K spans/sec
    time.sleep(max(0, 0.01 - batch_latency))

# Analyze results
print(f"p50: {statistics.median(latencies):.4f}s")
print(f"p95: {statistics.quantiles(latencies, n=20)[18]:.4f}s")
print(f"p99: {statistics.quantiles(latencies, n=100)[98]:.4f}s")

# Verify guardrails
assert statistics.quantiles(latencies, n=100)[98] < 0.1, "p99 > 100ms"
```

**Success criteria:**
- Sustain 10,000 spans/sec for 5 minutes
- p99 insert latency < 100ms
- Memory plateaus (no unbounded growth)

**Testing:**
- Automated performance test
- Memory monitoring with psutil

**Dependencies:** Task 5.6

---

### Task 5.8: Concurrent Query + Insert Test
**Files to create:**
- `test/integration/concurrent_test.py`

**Implementation:**
```python
# Thread 1: Continuous inserts
# Thread 2: Continuous queries
# Verify no deadlocks, consistent results

import threading
import duckdb

insert_errors = []
query_errors = []

def insert_thread():
    try:
        for i in range(10000):
            # Export spans
            with tracer.start_as_current_span(f"span-{i}"):
                pass
    except Exception as e:
        insert_errors.append(e)

def query_thread():
    con = duckdb.connect()
    con.execute("ATTACH 'otlp://localhost:4317' AS live (TYPE otlp)")
    try:
        for i in range(1000):
            result = con.execute("SELECT COUNT(*) FROM live.traces").fetchone()
            # Count should be monotonically increasing
    except Exception as e:
        query_errors.append(e)

# Run concurrent threads
t1 = threading.Thread(target=insert_thread)
t2 = threading.Thread(target=query_thread)
t1.start()
t2.start()
t1.join()
t2.join()

assert len(insert_errors) == 0
assert len(query_errors) == 0
```

**Success criteria:**
- No deadlocks
- Queries don't block inserts
- Inserts don't corrupt query results

**Testing:**
- Concurrent load test
- Thread sanitizer for race conditions

**Dependencies:** Task 5.7

---

## Summary

**Total Tasks:** 42 tasks across 5 phases

**Estimated Effort:**
- Phase 1 (read_otlp JSON): 6 tasks, ~3-5 days
- Phase 2 (Protobuf): 6 tasks, ~3-4 days
- Phase 3 (Ring Buffer): 6 tasks, ~4-6 days
- Phase 4 (ATTACH/DETACH): 7 tasks, ~4-5 days
- Phase 5 (gRPC): 8 tasks, ~5-7 days

**Total Estimate:** 19-27 days (4-6 weeks)

**Key Milestones:**
1. ✅ Phase 1 complete: Can query OTLP files
2. ✅ Phase 2 complete: Binary format support
3. ✅ Phase 3 complete: Memory-bounded storage
4. ✅ Phase 4 complete: ATTACH/DETACH works
5. ✅ Phase 5 complete: Live streaming operational

**Critical Path:**
Phase 1 → Phase 3 → Phase 4 → Phase 5
(Phase 2 can be done in parallel with Phase 3)
