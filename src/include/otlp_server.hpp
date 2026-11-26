#pragma once

#include "duckdb.hpp"

#include "otlp_column_promote.hpp"
#include "otlp_ingest_limits.hpp"
#include "otlp_log.hpp"
#include "otlp_uri.hpp"
#include "otlp2records.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <thread>

namespace duckdb {

class ClientContext;
class Connection;
class DatabaseInstance;
class ColumnDataCollection;
class DataChunk;
struct OtlpSignal;

//! Wire transport for one listener: HTTP (cpp-httplib) or GRPC (OTLP/gRPC + OTAP/Arrow via the
//! tonic server in otlp2records). otap: URIs are always GRPC.
enum class OtlpTransport { HTTP, GRPC };

enum class OtlpRequestKind : uint8_t { LOGS, TRACES, METRICS };

const char *OtlpTransportName(OtlpTransport transport);

//! One bound socket plus the transport that serves it. A server owns one or more.
struct OtlpListenerSpec {
	OtlpUri uri;
	OtlpTransport transport = OtlpTransport::HTTP;
	//! GRPC only: OR of OTLP_GRPC_SERVICE_* bits selecting the service families to register.
	//! 0 would register both.
	uint32_t grpc_service_flags = 0;
};

//! Server-wide settings shared by every listener.
struct OtlpServerConfig {
	string token;
	//! Accept every request without checking the token (for producers that cannot send one, e.g.
	//! the otel-arrow OTAP exporter). When true, no token is generated or validated at bind time.
	bool disable_auth = false;
	//! Target catalog (attached database). Empty = the connection's default catalog.
	string catalog_name;
	string schema_name = "main";
	//! Optional Parquet export root: each seal COPYs to <root>/<table>/year=/month=/day=/ instead of
	//! a table. At-least-once (a COPY cannot be rolled back), so downstream readers must dedupe.
	string parquet_export_path;
	//! Parquet mode only: create an inspection view per signal. Binding it reads every file's
	//! footer, so a caller that never reads the view should turn it off.
	bool parquet_views = true;
	bool create_tables = true;
	idx_t max_body_bytes = otlp_limits::DEFAULT_MAX_BODY_BYTES;
	//! Worker threads for each HTTP listener. Zero means choose a conservative host-based default.
	idx_t http_threads = 0;
	//! Group-commit ("seal") triggers: one writer seals buffered rows on size or age, avoiding
	//! per-request Parquet files and write conflicts. Independent of max_buffered_bytes.
	idx_t seal_target_bytes = otlp_limits::DEFAULT_SEAL_TARGET_BYTES; //! seal when admitted bytes reach this
	int64_t seal_max_age_ms = otlp_limits::DEFAULT_SEAL_MAX_AGE_MS;   //! seal when the oldest buffered row is this old
	//! Catalog maintenance: the sealer runs `CHECKPOINT <catalog>` at most once per
	//! maintenance_interval_ms, after rows were sealed (never from otlp_flush or shutdown drains).
	int64_t maintenance_interval_ms = otlp_limits::DEFAULT_MAINTENANCE_INTERVAL_MS;
	//! Backpressure cap (over it -> 503) on admitted request-body bytes (max(body_size, 1024)
	//! each), not decoded buffer heap: a throughput proxy, not a precise memory bound.
	idx_t max_buffered_bytes = otlp_limits::DEFAULT_MAX_BUFFERED_BYTES;
	//! Resource/scope attribute keys promoted to columns. Off when empty; catalog mode only.
	OtlpPromoteConfig promote;
};

struct OtlpIngestResult {
	idx_t rows = 0;
	idx_t batches = 0;
	idx_t skipped_summaries = 0;
	idx_t skipped_nan_values = 0;
	idx_t skipped_infinity_values = 0;
	idx_t skipped_missing_values = 0;

	bool HasSkipped() const {
		return skipped_summaries > 0 || skipped_nan_values > 0 || skipped_infinity_values > 0 ||
		       skipped_missing_values > 0;
	}
};

//! Why the server refused an ingest call. The server decides; each transport only maps the
//! refusal to its own wire status (OtlpHttpListener: an HTTP status, the gRPC bridge: an
//! OtlpIngestStatus), so both report the same failure the same way.
enum class OtlpIngestRefusal : uint8_t {
	NONE,              //!< accepted: the rows are buffered
	PAYLOAD_TOO_LARGE, //!< body over max_body_bytes; permanent
	UNSUPPORTED_MEDIA, //!< content type or content encoding the server does not decode; permanent
	MALFORMED,         //!< the payload did not parse or convert; permanent
	BUFFER_FULL,       //!< admission would exceed max_buffered_bytes; retry with backoff
	INTERNAL           //!< a server-side failure, logged as an OTLP event
};

//! The result of one ingest call, whichever transport carried it.
struct OtlpIngestOutcome {
	OtlpIngestRefusal refusal = OtlpIngestRefusal::NONE;
	//! Why the call was refused; empty when accepted.
	string message;
	//! What was buffered; all zero when refused.
	OtlpIngestResult counts;

	bool Accepted() const {
		return refusal == OtlpIngestRefusal::NONE;
	}
};

class OtlpServer;

//! One bound socket feeding a shared OtlpServer. Listeners own no buffers, sealer, or counters.
class OtlpListener {
public:
	OtlpListener(OtlpServer &server_p, OtlpListenerSpec spec_p) : server(server_p), spec(std::move(spec_p)) {
	}
	virtual ~OtlpListener() = default;

	//! Bind synchronously (so address-in-use surfaces to otlp_serve) and start serving.
	virtual void Start() = 0;
	//! Stop accepting new connections without joining. Safe from a request-handler thread.
	virtual void StopAccepting() = 0;
	//! Stop accepting and join the transport's threads. Idempotent; must not run on a worker thread.
	virtual void Close() = 0;

	const OtlpUri &Uri() const {
		return spec.uri;
	}
	OtlpTransport Transport() const {
		return spec.transport;
	}
	//! False once the listener has stopped or its accept loop failed after bind.
	bool IsListening() const {
		return is_running.load() && !failed.load();
	}
	string LastError() const {
		std::lock_guard<std::mutex> lock(error_mutex);
		return last_error;
	}

protected:
	void RecordFailure(const string &error);

	OtlpServer &server;
	OtlpListenerSpec spec;
	std::atomic<bool> is_running {false};
	std::atomic<bool> failed {false};
	mutable mutex error_mutex;
	string last_error;
};

//! One otlp_server_list() column and how to read it for one listener of a server.
struct OtlpServerListColumn {
	const char *name;
	LogicalTypeId type;
	Value (*get)(const OtlpServer &server, const OtlpListener &listener);
};

unique_ptr<OtlpListener> MakeOtlpHttpListener(OtlpServer &server, const OtlpListenerSpec &spec);
unique_ptr<OtlpListener> MakeOtlpGrpcListener(OtlpServer &server, const OtlpListenerSpec &spec);

//! One ingest pipeline (buffers, admission, a single sealer/writer, maintenance) fed by N listeners.
class OtlpServer {
public:
	//! Build the pipeline, then start every listener in order. If any listener fails to bind,
	//! the already-started listeners are closed and the sealer stopped before this throws.
	OtlpServer(ClientContext &context, const vector<OtlpListenerSpec> &listener_specs, const OtlpServerConfig &config);
	~OtlpServer();

	//! Stop every listener, then drain with one final seal. Must NOT be called from a request-handler
	//! thread: httplib's teardown joins all workers, which would deadlock.
	void Close();

	const vector<unique_ptr<OtlpListener>> &Listeners() const {
		return listeners;
	}
	//! The first listener's URI.
	const OtlpUri &PrimaryUri() const;

	//! CSPRNG-backed 128-bit token, hex-encoded.
	static string GenerateRandomToken(DatabaseInstance &db);

	static void ValidateToken(const string &token);

	const OtlpServerConfig &Config() const {
		return config;
	}
	//! otlp_server_list()'s columns, in output order.
	static const vector<OtlpServerListColumn> &ListColumns();
	idx_t BufferedRows() const;
	//! Approximate decoded heap held by the buffers (admitted_bytes counts input bytes, not this).
	idx_t BufferedBytes() const;
	//! A seal has failed, rows are still buffered, and no seal succeeded for several cycles.
	//! Drives /readyz.
	bool SealStalled() const;
	idx_t AdmittedBytes() const {
		return admitted_bytes.load();
	}
	//! Milliseconds since the oldest currently buffered row was admitted, or -1 if empty.
	int64_t OldestBufferedAgeMs() const;
	idx_t SealsTotal() const {
		return seals_total.load();
	}
	idx_t CommittedRowsTotal() const {
		return committed_rows_total.load();
	}
	//! Never reset, unlike seal_last_error, which clears on the next success.
	idx_t SealFailuresTotal() const {
		return seal_failures_total.load();
	}
	//! Milliseconds since the last successful seal, or -1 if none yet.
	int64_t LastSealAgeMs() const;
	string SealLastError() const {
		std::lock_guard<std::mutex> lock(seal_error_mutex);
		return seal_last_error;
	}
	idx_t MaintenanceRunsTotal() const {
		return maintenance_runs_total.load();
	}
	idx_t MaintenanceFailuresTotal() const {
		return maintenance_failures_total.load();
	}
	//! Passes that lost a CHECKPOINT race to another writer on the same catalog. Not failures.
	idx_t MaintenanceContendedTotal() const {
		return maintenance_contended_total.load();
	}
	//! Milliseconds since the last successful catalog maintenance, or -1 if none yet.
	int64_t LastMaintenanceAgeMs() const;
	string MaintenanceLastError() const {
		std::lock_guard<std::mutex> lock(seal_error_mutex);
		return maintenance_last_error;
	}
	idx_t PromotedColumnsTotal() const {
		return promoted_columns;
	}

	//! Synchronous seal of all buffered rows (otlp_flush).
	idx_t SealOnce();

	//! Rows dropped because the final shutdown drain failed; 0 on a clean shutdown. Read after
	//! Close() so otlp_stop / the daemon can surface a data-dropping shutdown.
	idx_t ShutdownDroppedRows() const {
		return shutdown_dropped_rows.load();
	}

	// --- test-only seam for test/cpp/test_seal_harness.cpp, which drives the buffer/seal path
	// without a socket. Every hook defaults to unset, which is a no-op. ---
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
	//! Runs immediately before the catalog seal's COMMIT; a throw takes the rollback/re-buffer path.
	void SetSealFaultHookForTest(std::function<void()> hook) {
		seal_fault_hook_for_test = std::move(hook);
	}
	//! Runs before the Parquet COPY for signal `i`; a throw exercises the partial-export re-buffer path.
	void SetParquetSealFaultHookForTest(std::function<void(idx_t)> hook) {
		parquet_seal_fault_hook_for_test = std::move(hook);
	}
	//! Runs in StageMetrics after the first metric shape is staged, before anything reaches the live
	//! buffers; a throw must refuse the request and leave the buffers and admission untouched.
	void SetMetricsStageFaultHookForTest(std::function<void()> hook) {
		metrics_stage_fault_hook_for_test = std::move(hook);
	}
	//! Runs immediately before the maintenance CHECKPOINT. Set it before the trigger can fire.
	void SetMaintenanceFaultHookForTest(std::function<void()> hook) {
		maintenance_fault_hook_for_test = std::move(hook);
	}
#endif

	// --- Listener API ---
	bool CheckAuth(const string &authorization, const string &api_key = "") const;
	//! Check an OTLP/HTTP body and buffer its rows.
	OtlpIngestOutcome Ingest(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
	                         const string &body);
	//! Buffer one decoded Arrow batch through the same admission path as HTTP (Admit). `input_bytes`
	//! is the wire size charged against admission. The array/schema are borrowed, never released.
	OtlpIngestOutcome IngestArrow(OtlpSignalType signal_type, idx_t input_bytes, ArrowArray &array,
	                              ArrowSchema &schema);
	//! Log to duckdb_logs from any thread. Use LOG_WARNING for failures.
	void LogServerEvent(const string &message, LogLevel level = OtlpLogType::LEVEL) const;

	//! Counts HTTP ingest requests from handler entry, including rejected requests.
	class RequestGuard {
	public:
		explicit RequestGuard(OtlpServer &server);
		~RequestGuard();
		RequestGuard(const RequestGuard &) = delete;
		RequestGuard &operator=(const RequestGuard &) = delete;

	private:
		OtlpServer &server;
	};

private:
	void EnsureTargetTables();
	//! Stop the sealer and seal remaining rows before the writer connection / database go away.
	//! Idempotent. Returns the rows dropped by a failed final drain (0 on every later call).
	idx_t ShutdownIngest();
	//! Reverse start order. Idempotent.
	void CloseListeners();

	std::atomic<idx_t> active_requests {0};
	std::atomic<idx_t> total_requests {0};
	std::atomic<idx_t> total_rows {0};

	// --- request path (runs on httplib worker threads; buffers, never commits) ---
	//! One signal's rows converted off to the side. A request's signals reach the live buffers only
	//! once ALL convert, so a mid-conversion failure cannot make a client retry double-buffer.
	struct StagedSignal {
		OtlpSignalType signal_type;
		unique_ptr<ColumnDataCollection> collection;
		idx_t rows = 0;
		idx_t batches = 0;
	};
	//! Converts a request into staged signals off to the side; may add to `counts` (skipped values).
	using StageFn = std::function<vector<StagedSignal>(OtlpIngestResult &counts)>;
	//! The one admission sequence every transport goes through: reserve `input_bytes` against
	//! max_buffered_bytes, stage, move the staged rows into the live buffers, release whatever the
	//! buffers did not claim. A throw from `stage` becomes a refusal (see RefusalFromException).
	OtlpIngestOutcome Admit(idx_t input_bytes, const StageFn &stage);
	//! Classify the in-flight exception as a refusal, logging internal failures.
	OtlpIngestOutcome RefusalFromException() const;
	vector<StagedSignal> StageSignal(OtlpSignalType signal_type, const string &body, OtlpInputFormat format);
	vector<StagedSignal> StageMetrics(const string &body, OtlpInputFormat format, OtlpIngestResult &counts);
	void StageArrowBatch(OtlpSignalType signal_type, ArrowArray &array, ArrowSchema &schema,
	                     vector<StagedSignal> &staged);
	//! Move staged signals into the live buffers and transfer their reservation (zeroes it once consumed).
	void CommitStaged(vector<StagedSignal> &staged, OtlpIngestResult &result, idx_t &admission_bytes);
	bool TryReserveAdmission(idx_t bytes, idx_t &current);
	void ReleaseAdmission(idx_t bytes);

	// --- seal path (single writer) ---
	struct OtlpBuffers {
		vector<unique_ptr<ColumnDataCollection>> rows; // OTLP_SIGNALS order
		idx_t admission_bytes = 0;
		std::chrono::steady_clock::time_point oldest; // meaningful while Rows() > 0
		idx_t Rows() const;
	};

	void InitBuffers();
	OtlpBuffers MakeEmptyBuffers(Allocator &allocator) const;
	void SealerLoop();
	//! Re-buffer unwritten signals ahead of rows appended during the seal.
	void RestoreUnsealed(OtlpBuffers &plan, const std::vector<bool> &sealed, idx_t restored_admission);
	//! At most once per signal, and only if the name is free. Under writer_mutex.
	void EnsureParquetView(OtlpSignal &signal);
	//! One statement per signal that writes the relation `sealed` to the destination.
	void BuildSinks();
	void RecordSealOutcome(bool success, idx_t rows_committed, const string &error);
	void RequestSeal();
	bool SealAgeDue() const;
	//! Sealer thread only.
	void MaybeRunCatalogMaintenance();
	//! Delete untracked files left by a losing checkpoint or crashed seal using DuckLake's retention.
	//! Best-effort. Caller holds writer_mutex.
	void SweepOrphanedFiles();

	void CreateOrValidateTable(const OtlpSignal &signal);
	//! The importer's type, except TIMESTAMP_NS narrows to the tables' TIMESTAMP (us).
	static LogicalType BufferColumnType(const LogicalType &imported);

private:
	weak_ptr<DatabaseInstance> db_ptr;
	OtlpServerConfig config;
	const bool maintains_catalog;
	bool is_ducklake = false; // Resolved before the sealer starts.
	//! Start order. Immutable after construction.
	vector<unique_ptr<OtlpListener>> listeners;

	//! OTLP_SIGNALS order. Metadata and sink SQL are fixed before ingest starts.
	vector<unique_ptr<OtlpSignal>> signals;
	mutable std::mutex buffer_mutex;
	OtlpBuffers live; // under buffer_mutex once ingest starts
	//! Admitted bytes of in-flight and unsealed requests: enforces max_buffered_bytes and drives the
	//! seal size trigger. Includes reservations for staged, live and sealing rows.
	std::atomic<idx_t> admitted_bytes {0};

	// writer_mutex serializes seals, including the shutdown drain, and catalog maintenance.
	unique_ptr<Connection> writer_con;
	mutex writer_mutex;
	//! Shared by every ingest thread for DuckDB's Arrow importer: safe because it runs no queries and
	//! the importer reads it only for extension types, which ForSignal rules out.
	unique_ptr<Connection> arrow_con;
	//! Empty unless promotion succeeded. Immutable after construction.
	string promote_projection;
	idx_t promoted_columns = 0;
	std::thread sealer_thread;
	std::mutex sealer_mutex;
	std::condition_variable sealer_cv;
	std::atomic<bool> sealer_stop {false};
	std::atomic<bool> flush_requested {false};
	std::atomic<bool> ingest_shutdown_done {false};
	std::atomic<idx_t> shutdown_dropped_rows {0};

	std::atomic<idx_t> seals_total {0};
	std::atomic<idx_t> seal_failures_total {0};
	std::atomic<idx_t> committed_rows_total {0};
	std::atomic<int64_t> last_seal_unix_ms {0};
	mutable mutex seal_error_mutex;
	string seal_last_error;

	bool maintenance_unsupported = false; // Sealer thread only.
	//! Rows were sealed since the last attempt. Starts true so rows a previous process left inlined
	//! are flushed too. Guarded by writer_mutex.
	bool catalog_maintenance_due = true;
	std::chrono::steady_clock::time_point catalog_maintenance_last_attempt = std::chrono::steady_clock::now();
	std::atomic<idx_t> maintenance_runs_total {0};
	std::atomic<idx_t> maintenance_failures_total {0};
	std::atomic<idx_t> maintenance_contended_total {0};
	std::atomic<int64_t> last_maintenance_unix_ms {0};
	string maintenance_last_error; // guarded by seal_error_mutex

#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
	//! Test-only fault hooks (see the Set*FaultHookForTest setters).
	std::function<void()> seal_fault_hook_for_test;
	std::function<void(idx_t)> parquet_seal_fault_hook_for_test;
	std::function<void()> metrics_stage_fault_hook_for_test;
	std::function<void()> maintenance_fault_hook_for_test;
#endif
};

} // namespace duckdb
