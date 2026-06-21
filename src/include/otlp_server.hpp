#pragma once

#include "duckdb.hpp"

#include "otlp_ingest_limits.hpp"
#include "otlp_log.hpp"
#include "otlp_request.hpp"
#include "otlp_uri.hpp"
#include "otlp2records.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <thread>

namespace duckdb {

class ClientContext;
class Connection;
class DatabaseInstance;
class ColumnDataCollection;
class DataChunk;
struct OtlpSignalBuffer;

//! Wire transport for live ingest. HTTP is the OTLP/HTTP server (cpp-httplib);
//! GRPC is the OTLP/gRPC + OTAP/Arrow server (an embedded tonic server reached
//! through the otlp2records FFI). Selected by the listen URI scheme: otlp: =>
//! HTTP, otap: => GRPC.
enum class OtlpTransport { HTTP, GRPC };

struct OtlpServerConfig {
	string token;
	//! Opt-in: accept every request without checking the bearer token / x-api-key. Defaults
	//! to false (auth required). Intended for trusted local networks and for producers that
	//! cannot attach a bearer token (e.g. the otel-arrow OTAP exporter has no auth-header
	//! config). When true, no token is generated or validated at bind time.
	bool disable_auth = false;
	//! Wire transport. Defaults to HTTP; GRPC for otap_serve or otlp_serve(transport:='grpc').
	OtlpTransport transport = OtlpTransport::HTTP;
	//! When transport == GRPC, OR of OTLP_GRPC_SERVICE_* bits selecting which gRPC service
	//! families the listener registers: OTLP_GRPC_SERVICE_OTLP_UNARY for otlp_serve(grpc),
	//! OTLP_GRPC_SERVICE_OTAP_ARROW for otap_serve. Unused for HTTP. 0 would register both.
	uint32_t grpc_service_flags = 0;
	//! Target catalog (attached database). Empty = the connection's default catalog.
	//! Set this to an attached writable catalog name for lakehouse ingest.
	string catalog_name;
	string schema_name = "main";
	//! Optional plain Parquet export root. When set, the server keeps no persistent
	//! destination table: each seal writes the sealed rows straight to
	//! <root>/<table>/year=YYYY/month=MM/day=DD/*.parquet (the only durable store), and a
	//! read-only view over those files is created lazily for inspection. Export is
	//! at-least-once (a COPY cannot be rolled back), so downstream readers must dedupe.
	string parquet_export_path;
	bool create_tables = true;
	idx_t max_body_bytes = otlp_limits::DEFAULT_MAX_BODY_BYTES;
	//! HTTP worker threads. Zero means choose a conservative host-based default.
	idx_t http_threads = 0;
	//! Internal buffered group-commit ("seal") defaults. Ingest buffers rows in memory
	//! and a single writer seals them on a size or age trigger, avoiding per-request
	//! Parquet files and write conflicts. These are exposed as advanced cadence knobs;
	//! max_buffered_bytes remains the independent admission/backpressure cap.
	//! Explicit otlp_flush is optional; it only requests an immediate seal for fresh
	//! reads/durability while the server keeps running. otlp_stop performs a final seal.
	idx_t seal_target_bytes = otlp_limits::DEFAULT_SEAL_TARGET_BYTES; //! seal when admitted bytes reach this
	int64_t seal_max_age_ms = otlp_limits::DEFAULT_SEAL_MAX_AGE_MS;   //! seal when the oldest buffered row is this old
	//! Tier-1 compaction for DuckLake catalogs. Successful automatic row-seals into a named
	//! catalog are followed by best-effort catalog-native maintenance (`CHECKPOINT <catalog>`,
	//! which merges adjacent files and expires/cleans old snapshots+files) when recent ingress
	//! and pending bytes leave ample admission headroom; it is skipped for explicit otlp_flush
	//! and shutdown drains so those stay pure durability/seal operations. The maintenance CADENCE
	//! is internal, but these two knobs (set once on the catalog at startup) bound it:
	//!  - target_file_size: OUTPUT Parquet file size the merge bin-packs toward; bounds write
	//!    amplification (files already at target are left alone). Distinct from seal_target_bytes,
	//!    which is admitted *input* bytes.
	//!  - maintenance_retention_ms: how old snapshots/files must be before CHECKPOINT expires and
	//!    deletes them (expire_older_than / delete_older_than). Long enough for in-flight reads /
	//!    time-travel, short enough to bound disk.
	idx_t target_file_size = otlp_limits::DEFAULT_TARGET_FILE_SIZE;
	int64_t maintenance_retention_ms = otlp_limits::DEFAULT_MAINTENANCE_RETENTION_MS;
	//! Backpressure admission cap (over it -> 503). NOTE: this bounds cumulative *admitted
	//! request-body bytes* (each request reserves max(body_size, 1024) input bytes), NOT
	//! decoded buffer heap — decoded columnar size differs from the encoded/compressed
	//! input size. It is an admission/throughput proxy, not a precise memory bound.
	idx_t max_buffered_bytes = otlp_limits::DEFAULT_MAX_BUFFERED_BYTES;
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

struct OtlpSealEvent {
	idx_t seal_sequence = 0;
	int64_t started_unix_ms = 0;
	int64_t completed_unix_ms = 0;
	int64_t duration_ms = 0;
	int64_t append_duration_ms = 0;
	int64_t commit_duration_ms = 0;
	idx_t rows_committed = 0;
	idx_t admitted_bytes_committed = 0;
	bool success = false;
	// Whole-server running totals as of this seal, snapshotted in RecordSealEvent *after* the
	// per-seal counters have been incremented. Deliberately denormalized so otlp_seal_list() can
	// show the cumulative tally at each seal without a window function; keep the increment-then-
	// snapshot order in SealOnce or these values will be off by one seal.
	idx_t seals_total = 0;
	idx_t seal_failures_total = 0;
	idx_t committed_rows_total = 0;
	string error;
};

class OtlpServer {
public:
	OtlpServer(ClientContext &context, const OtlpUri &uri, const OtlpServerConfig &config);
	~OtlpServer();

	//! Stop accepting new connections (close the listener socket) without joining
	//! listener threads. Safe to call from a request-handler thread — does not wait
	//! on httplib's task queue, which would deadlock when the caller is a worker.
	void StopAccepting();

	//! Synchronously stop accepting connections and join the listener threads. Must
	//! NOT be called from a worker / request-handler thread; httplib's listen-loop
	//! teardown joins all workers, which would deadlock.
	void Close();

	//! Whether the server is currently accepting connections. False once the
	//! listener thread has exited (e.g. an error after a successful bind), so an
	//! operator can tell a registered server has fallen over.
	bool IsListening() const {
		return is_running.load() && !listener_failed.load();
	}
	//! Last fatal listener error, or empty if none.
	string LastError() const {
		std::lock_guard<std::mutex> lock(error_mutex);
		return last_error;
	}

	//! Generate a fresh CSPRNG-backed 128-bit token, hex-encoded (32 chars).
	static string GenerateRandomToken(DatabaseInstance &db);

	//! Throw InvalidInputException if `token` doesn't meet requirements (length >= 16).
	static void ValidateToken(const string &token);

	const OtlpUri &ListenUri() const {
		return uri;
	}
	const string &Token() const {
		return config.token;
	}
	const string &SchemaName() const {
		return config.schema_name;
	}
	const string &CatalogName() const {
		return config.catalog_name;
	}
	idx_t ActiveRequests() const {
		return active_requests.load();
	}
	idx_t TotalRequests() const {
		return total_requests.load();
	}
	idx_t TotalRows() const {
		return total_rows.load();
	}
	idx_t BufferedRows() const;
	//! Approximate decoded heap held by the in-memory buffers (sum of each signal's
	//! ColumnDataCollection size). admitted_bytes bounds *input* bytes, not this; surfaced so
	//! the real memory footprint under a slow/stuck seal is observable.
	idx_t BufferedBytes() const;
	//! True when buffered rows are not committing: at least one seal has failed, rows are
	//! still buffered, and the last successful seal is absent or older than several seal
	//! cycles. Drives /readyz so a wedged seal backend is visible to orchestrators.
	bool SealStalled() const;
	idx_t AdmittedBytes() const {
		return admitted_bytes.load();
	}
	idx_t SealTargetBytes() const {
		return config.seal_target_bytes;
	}
	int64_t SealMaxAgeMs() const {
		return config.seal_max_age_ms;
	}
	//! Milliseconds since the oldest currently buffered row was admitted, or -1 if empty.
	int64_t OldestBufferedAgeMs() const;
	idx_t SealsTotal() const {
		return seals_total.load();
	}
	idx_t CommittedRowsTotal() const {
		return committed_rows_total.load();
	}
	//! Monotonic count of failed seal attempts (never reset), so a flapping/failing
	//! sealer is visible even when seal_last_error self-clears on the next success.
	idx_t SealFailuresTotal() const {
		return seal_failures_total.load();
	}
	//! Milliseconds since the last successful seal, or -1 if none yet.
	int64_t LastSealAgeMs() const;
	string SealLastError() const {
		std::lock_guard<std::mutex> lock(seal_error_mutex);
		return seal_last_error;
	}
	//! Catalog-maintenance (CHECKPOINT) telemetry, mirroring the seal counters.
	idx_t MaintenanceRunsTotal() const {
		return maintenance_runs_total.load();
	}
	idx_t MaintenanceFailuresTotal() const {
		return maintenance_failures_total.load();
	}
	//! Milliseconds since the last successful catalog maintenance, or -1 if none yet.
	int64_t LastMaintenanceAgeMs() const;
	string MaintenanceLastError() const {
		std::lock_guard<std::mutex> lock(seal_error_mutex);
		return maintenance_last_error;
	}
	vector<OtlpSealEvent> SealHistory() const;

	//! Force a synchronous seal of all buffered rows (used by otlp_flush). Returns the
	//! number of rows sealed.
	OtlpIngestResult FlushNow();

	//! Rows that were still buffered (and therefore dropped) after the final shutdown drain
	//! gave up. 0 on a clean shutdown. Populated by ShutdownIngest(); read by the storage
	//! layer after Close() so otlp_stop / the daemon can surface a data-dropping shutdown.
	//! A second (idempotent) ShutdownIngest() drops nothing and leaves this value unchanged.
	idx_t ShutdownDroppedRows() const {
		return shutdown_dropped_rows.load();
	}

	// --- test-only seam (no production caller) ---
	// These exist solely so the C++ ingest/seal harness (test/cpp/test_seal_harness.cpp)
	// can drive the buffer/seal path directly: SQL cannot drive HTTP, and the seal-failure
	// re-buffer/retry invariants must be locked in before the SealOnce refactor (finding H4).
	// They are NEVER referenced by production code, so production behavior is unchanged: the
	// fault hook is a null std::function (the seal path's `if (fault) fault();` is a no-op),
	// and IngestForTest just forwards to the private Ingest() the HTTP handler already calls.
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
	//! Drive the in-memory buffer path exactly as an HTTP POST would, without a socket.
	OtlpIngestResult IngestForTest(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
	                               const string &body) {
		return Ingest(kind, content_type, content_encoding, body);
	}
	//! Fault hook invoked inside SealOnce's transaction path immediately before COMMIT. If it
	//! throws, the seal fails through the production catch/rollback/re-buffer path. Default
	//! (unset) = no-op, so SealOnce is byte-for-byte identical to production when not set.
	void SetSealFaultHookForTest(std::function<void()> hook) {
		seal_fault_hook_for_test = std::move(hook);
	}
	//! Fault hook invoked inside SealParquet's per-signal loop immediately before the COPY for
	//! signal index `i`. If it throws for some signal, that signal (and all later ones) fail to
	//! export while earlier signals are already durable, exercising the at-least-once partial-
	//! export + proportional-admission re-buffer path. Default (unset) = no-op.
	void SetParquetSealFaultHookForTest(std::function<void(idx_t)> hook) {
		parquet_seal_fault_hook_for_test = std::move(hook);
	}
	//! Fault hook invoked inside BufferMetrics AFTER the first metric sub-signal has converted into
	//! its local staging collection but BEFORE the remaining sub-signals convert and BEFORE anything
	//! is moved into the live buffers. If it throws, the request fails mid-conversion; the staging
	//! design (H3) guarantees NO rows reach the live buffers and admission is untouched, so a retry
	//! buffers everything exactly once. Default (unset) = no-op.
	void SetMetricsStageFaultHookForTest(std::function<void()> hook) {
		metrics_stage_fault_hook_for_test = std::move(hook);
	}
#endif

	// --- gRPC transport bridge ---
	// The gRPC server lives in the otlp2records crate and reaches back into this
	// server through a C callback. These two entry points are the bridge the free
	// callback in otlp_server_grpc.cpp invokes (it holds an OtlpServer* as its
	// user_data). Public only because that callback has C linkage and cannot be a
	// member; not part of the general API.

	//! Buffer one already-decoded Arrow batch into `signal_type`'s buffer, applying
	//! the same admission/backpressure + stage + group-commit path as the HTTP
	//! ingest path. `input_bytes` is the wire size charged against admission. The
	//! array/schema are borrowed (copied, never released here). Returns the gRPC
	//! mapped status (OTLP_INGEST_OK / _RESOURCE_EXHAUSTED / _INTERNAL).
	OtlpIngestStatus IngestDecodedArrowBatch(OtlpSignalType signal_type, idx_t input_bytes, ArrowArray &array,
	                                         ArrowSchema &schema);
	//! Validate a gRPC request's `authorization` metadata against the server token
	//! (shares OtlpServer::CheckAuth with the HTTP path).
	bool CheckGrpcAuth(const string &authorization) const;

private:
	bool CheckAuth(const string &authorization, const string &api_key) const;
	//! Start the embedded gRPC server (otap: transport). Binds synchronously so an
	//! address-in-use surfaces to otlp_serve/otap_serve; on failure seals/stops the
	//! sealer and throws. No-op shell on wasm.
	void StartGrpc();
	//! Graceful gRPC shutdown: stop accepting, drain in-flight requests, join the
	//! runtime, free the handle. Idempotent. Must run before ShutdownIngest().
	void StopGrpc();
	OtlpIngestResult Ingest(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
	                        const string &body);
	void EnsureTargetTables();
	//! Stop the sealer thread and seal any remaining buffered rows before the writer
	//! connection / database go away. Idempotent; called from Close() and ~OtlpServer().
	//! Returns the number of rows still buffered after the final drain failed (i.e. dropped):
	//! 0 on a clean shutdown, and 0 on every call after the first (a second call has nothing
	//! left to drain). Also recorded in shutdown_dropped_rows for ShutdownDroppedRows().
	idx_t ShutdownIngest();
	//! Write a server-side diagnostic to duckdb_logs under the OTLP log type. No-op
	//! if the database has been closed. Safe to call from any worker thread. Routine
	//! lifecycle events log at INFO (the default); pass LogLevel::LOG_WARNING for
	//! failure paths so they stay visible at default log thresholds.
	void LogServerEvent(const string &message, LogLevel level = OtlpLogType::LEVEL) const;

	static void ListenThread(OtlpServer *server);

	std::thread listen_thread;
	std::atomic<idx_t> active_requests {0};
	std::atomic<idx_t> total_requests {0};
	std::atomic<idx_t> total_rows {0};

	// --- request path (runs on httplib worker threads; buffers, never commits) ---
	//! One signal's rows converted off to the side (NOT yet in the live buffer). Built so a
	//! request's signals are staged in local collections and only moved into the live buffers
	//! once ALL of them convert successfully — a mid-conversion failure leaves the live buffers
	//! untouched so a client retry cannot double-buffer (review finding H3).
	struct StagedSignal {
		OtlpSignalType signal_type;
		unique_ptr<ColumnDataCollection> collection;
		idx_t rows = 0;
		idx_t batches = 0;
	};
	void BufferSignal(OtlpSignalType signal_type, const string &body, OtlpInputFormat format, OtlpIngestResult &result,
	                  idx_t &admission_bytes);
	void BufferMetrics(const string &body, OtlpInputFormat format, OtlpIngestResult &result, idx_t &admission_bytes);
	//! Convert one Arrow batch into a local staging collection (no live buffer, no admission).
	StagedSignal StageArrowBatch(OtlpSignalType signal_type, ArrowArray &array, ArrowSchema &schema);
	//! Append `chunk` into `collection` (a local staging collection); pure conversion, no locks.
	void StageAppend(ColumnDataCollection &collection, DataChunk &chunk);
	//! Move all staged signals into the live per-signal buffers under their locks and attribute
	//! `admission_bytes` across them by rows, exactly once (all-or-nothing for the request). Sets
	//! `admission_bytes` to 0 once consumed. Updates `result` rows/batches.
	void CommitStaged(vector<StagedSignal> &staged, OtlpIngestResult &result, idx_t &admission_bytes);
	OtlpSignalBuffer &BufferFor(OtlpSignalType signal_type);
	bool TryReserveAdmission(idx_t bytes, idx_t &current);
	void ReleaseAdmission(idx_t bytes);

	// --- seal path (single writer) ---
	void InitBuffers();
	void StartSealer();
	void SealerLoop();

	//! One signal's rows swapped out of the live buffer for the duration of a seal. The
	//! moved-out collection is owned solely by the SealingPlan until it is either appended
	//! (success) or re-buffered (RestoreUnsealed).
	struct SealingSignal {
		unique_ptr<ColumnDataCollection> collection;
		idx_t rows = 0;
		//! Admission bytes attributed to this signal's swapped-out rows (the live buffer's
		//! unsealed_admission_bytes at swap time). On a partial parquet failure the EXACT value is
		//! restored for an un-exported signal and the exact exported share is released — no
		//! proportional re-split, so repeated partial failures cannot drift the budget (M2).
		idx_t admission_bytes = 0;
		//! The swapped-out rows' original buffering timestamp (the live buffer's first_unsealed at
		//! swap time). Preserved so a failed-seal restore keeps the TRUE age of these rows instead
		//! of resetting it to now() — otherwise oldest_buffered_age_ms under-reports how long rows
		//! have actually been stuck across consecutive seal failures (review finding M3). Only
		//! meaningful when rows > 0.
		std::chrono::steady_clock::time_point first_unsealed;
	};
	//! The atomic snapshot a single seal operates on: every signal's rows swapped out under
	//! lock plus the admission bytes attributed to them. Produced once by SwapBuffersForSeal()
	//! and consumed by exactly one of SealParquet()/SealCatalog().
	struct SealingPlan {
		std::vector<SealingSignal> signals;
		idx_t total_rows = 0;
		//! Aggregate admission bytes unsealed at swap time (sum of every signal's per-signal
		//! admission). On success the whole amount is ReleaseAdmission()'d; on a parquet partial
		//! failure each restored signal puts back its OWN SealingSignal::admission_bytes and only
		//! the exported signals' shares are released. Equals the sum of the per-signal values, so
		//! the all-or-nothing catalog path is unchanged.
		idx_t sealed_admission_bytes = 0;
	};

	OtlpIngestResult SealOnce(bool allow_maintenance);
	//! Swap every live signal buffer out for a fresh empty collection under the established
	//! per-signal lock order (forward index order, all held simultaneously), capturing each
	//! signal's per-signal unsealed admission bytes into its SealingSignal (summed into
	//! plan.sealed_admission_bytes). Workers keep filling the fresh buffers during the slow COPY.
	SealingPlan SwapBuffersForSeal(Allocator &allocator);
	//! Re-buffer the un-sealed remainder of `plan` ahead of any rows workers appended during
	//! the seal (old rows first, order preserved), re-acquiring the per-signal locks in the
	//! same forward order. `restore` selects which signals to restore (false = leave released,
	//! used by the parquet path's already-exported signals). Each restored signal puts back its
	//! OWN SealingSignal::admission_bytes (no proportional re-split), so repeated partial failures
	//! cannot drift the budget (M2). Returns the row count restored.
	idx_t RestoreUnsealed(SealingPlan &plan, const std::vector<bool> &restore);
	//! Plain Parquet export protocol (no shared transaction; at-least-once per signal).
	OtlpIngestResult SealParquet(SealingPlan &plan, int64_t seal_started_unix_ms);
	//! Transactional catalog commit protocol (swap-under-lock, full restore on failure). `db` is
	//! the live DatabaseInstance (kept alive by SealOnce's shared_ptr) needed to rebuild the
	//! writer connection if a ROLLBACK wedges the transaction.
	OtlpIngestResult SealCatalog(SealingPlan &plan, int64_t seal_started_unix_ms, bool allow_maintenance,
	                             DatabaseInstance &db);
	//! Centralize the success/failure metric bookkeeping shared by both seal protocols:
	//! seals_total / seal_failures_total / committed_rows_total, last_seal_unix_ms,
	//! seal_last_error, and the RecordSealEvent history append.
	void RecordSealOutcome(bool success, idx_t rows_committed, idx_t admitted_bytes_committed,
	                       int64_t seal_started_unix_ms, int64_t append_duration_ms, int64_t commit_duration_ms,
	                       const string &error);
	void RequestSeal();
	bool SealAgeDue() const;
	void MaybeRunCatalogMaintenance(idx_t sealed_rows, idx_t sealed_admission_bytes);
	//! Set the DuckLake catalog options the post-seal CHECKPOINT consumes (target_file_size,
	//! expire_older_than, delete_older_than). Best-effort and idempotent: called once at startup;
	//! non-DuckLake / default catalogs throw and are ignored (maintenance DISABLEs on its own).
	void ConfigureCatalogMaintenanceOptions();
	void RecordSealEvent(int64_t started_unix_ms, int64_t append_duration_ms, int64_t commit_duration_ms,
	                     idx_t rows_committed, idx_t admitted_bytes_committed, bool success, const string &error);

	void CreateOrValidateTable(Connection &con, OtlpSignalType signal_type, const string &table_name);
	void GetSignalColumns(OtlpSignalType signal_type, vector<LogicalType> &types, vector<string> &names);

private:
	weak_ptr<DatabaseInstance> db_ptr;
	OtlpUri uri;
	OtlpServerConfig config;

	// Per-signal in-memory buffers. The vector is immutable after InitBuffers();
	// each OtlpSignalBuffer carries its own mutex and mutable counters.
	std::vector<unique_ptr<OtlpSignalBuffer>> signal_buffers;
	//! Payload-byte admission counter for in-flight and unsealed accepted requests.
	//! The single real byte counter: enforces max_buffered_bytes under concurrency and
	//! drives the seal size trigger (seal_target_bytes).
	std::atomic<idx_t> admitted_bytes {0};
	//! The subset of admitted_bytes already attached to buffered rows is tracked PER SIGNAL in
	//! OtlpSignalBuffer::unsealed_admission_bytes (guarded by that signal's own mutex), parallel
	//! to buffered_rows. A swap sums those into SealingPlan; a partial parquet failure restores
	//! the exact per-signal share without re-splitting an aggregate (review finding M2).

	// Single serialized writer + background sealer. writer_mutex serializes SealOnce
	// (sealer thread) against FlushNow (otlp_flush) and the final drain on shutdown.
	unique_ptr<Connection> writer_con;
	mutex writer_mutex;
	std::thread sealer_thread;
	std::mutex sealer_mutex;
	std::condition_variable sealer_cv;
	std::atomic<bool> sealer_stop {false};
	std::atomic<bool> flush_requested {false};
	std::atomic<bool> ingest_shutdown_done {false};
	//! Rows dropped by the final shutdown drain (set once by the first ShutdownIngest()).
	std::atomic<idx_t> shutdown_dropped_rows {0};

	std::atomic<idx_t> seals_total {0};
	std::atomic<idx_t> seal_failures_total {0};
	std::atomic<idx_t> committed_rows_total {0};
	std::atomic<idx_t> seal_sequence {0};
	std::atomic<int64_t> last_seal_unix_ms {0};
	mutable mutex seal_error_mutex;
	string seal_last_error;
	mutable mutex seal_history_mutex;
	std::deque<OtlpSealEvent> seal_history;

	enum class CatalogMaintenanceState { PENDING, SUPPORTED, DISABLED };
	CatalogMaintenanceState catalog_maintenance_state = CatalogMaintenanceState::PENDING;
	idx_t catalog_maintenance_row_seals_since_attempt = 0;
	std::chrono::steady_clock::time_point catalog_maintenance_last_attempt = std::chrono::steady_clock::now();
	std::chrono::steady_clock::time_point catalog_maintenance_last_row_seal = std::chrono::steady_clock::now();
	double catalog_maintenance_ingress_rate_bytes_per_ms = 0;
	// Catalog-maintenance telemetry (written by the sealer thread, read by ListServers).
	std::atomic<idx_t> maintenance_runs_total {0};
	std::atomic<idx_t> maintenance_failures_total {0};
	std::atomic<int64_t> last_maintenance_unix_ms {0};
	string maintenance_last_error; // guarded by seal_error_mutex

#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
	//! Test-only seal fault hook (see SetSealFaultHookForTest). Null in production; the seal
	//! path only invokes it when set, so the production seal path is unaffected.
	std::function<void()> seal_fault_hook_for_test;
	//! Test-only parquet per-signal fault hook (see SetParquetSealFaultHookForTest). Null in
	//! production; SealParquet only invokes it when set.
	std::function<void(idx_t)> parquet_seal_fault_hook_for_test;
	//! Test-only metrics staging fault hook (see SetMetricsStageFaultHookForTest). Null in
	//! production; BufferMetrics only invokes it when set.
	std::function<void()> metrics_stage_fault_hook_for_test;
#endif

	// --- HTTP transport (httplib). PIMPL so httplib.hpp stays out of this header. ---
	class Impl;
	unique_ptr<Impl> impl;
	// --- gRPC transport. Opaque handle owned by the otlp2records crate (tonic
	// server + tokio runtime); null unless config.transport == GRPC. ---
	OtlpGrpcServer *grpc_handle = nullptr;
	std::atomic<bool> is_running {false};
	std::atomic<bool> listener_failed {false};
	mutable mutex error_mutex;
	string last_error;
};

//! Loopback HTTP probe backing the daemon's `healthcheck` subcommand. Distroless images
//! ship no shell/curl, so the daemon health-checks itself: returns true if
//! GET http://127.0.0.1:<port><path> answers 2xx/3xx within a short timeout. Defined in
//! otlp_server_http.cpp (httplib lives there); not built for wasm.
#ifndef __EMSCRIPTEN__
bool OtlpLoopbackHttpStatusOk(int port, const string &path);
//! Host-aware variant: probes GET http://<host>:<port><path>. Used by the daemon healthcheck so
//! a server bound to an explicit non-loopback interface is reachable (review finding M5). Defined
//! in otlp_server.cpp; not built for wasm.
bool OtlpHttpStatusOk(const string &host, int port, const string &path);
//! TCP-connect probe for the daemon healthcheck's gRPC (otap:) transport, which has no HTTP
//! /readyz endpoint. Returns true if a connection to <host>:<port> succeeds within 2s. Defined
//! in otlp_server.cpp; not built for wasm.
bool OtlpTcpConnectOk(const string &host, int port);
#endif

} // namespace duckdb
