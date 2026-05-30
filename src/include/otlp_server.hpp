#pragma once

#include "duckdb.hpp"
#include "duckdb/common/encryption_state.hpp"

#include "otlp_request.hpp"
#include "otlp_uri.hpp"
#include "otlp2records.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <optional>
#include <thread>

namespace duckdb {

class ClientContext;
class Connection;
class DatabaseInstance;
class ColumnDataCollection;
class DataChunk;
struct OtlpSignalBuffer;

struct OtlpServerConfig {
	string token;
	//! Target catalog (attached database). Empty = the connection's default catalog.
	//! Set this to an attached DuckLake catalog name to stream OTLP into a lakehouse.
	string catalog_name;
	string schema_name = "main";
	bool create_tables = true;
	idx_t max_body_bytes = 16ULL * 1024ULL * 1024ULL;
	//! Internal buffered group-commit ("seal") defaults. Ingest buffers rows in memory
	//! and a single writer seals them on a size or age trigger, avoiding per-request
	//! Parquet files and write conflicts. seal_target_bytes / seal_max_age_ms are fixed
	//! internal v0 defaults — deliberately NOT exposed as otlp_serve() named parameters
	//! until a caller actually needs to tune the seal cadence.
	idx_t seal_target_bytes = 64ULL * 1024ULL * 1024ULL; //! seal when admitted bytes reach this
	int64_t seal_max_age_ms = 5000;                      //! seal when the oldest buffered row is this old
	//! Backpressure admission cap (over it -> 503). NOTE: this bounds cumulative *admitted
	//! request-body bytes* (each request reserves max(body_size, 1024) input bytes), NOT
	//! decoded buffer heap — decoded columnar size differs from the encoded/compressed
	//! input size. It is an admission/throughput proxy, not a precise memory bound.
	idx_t max_buffered_bytes = 512ULL * 1024ULL * 1024ULL;
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
	idx_t SealsTotal() const {
		return seals_total.load();
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

	//! Force a synchronous seal of all buffered rows (used by otlp_flush). Returns the
	//! number of rows sealed.
	OtlpIngestResult FlushNow();

private:
	bool CheckAuth(const string &authorization, const string &api_key) const;
	OtlpIngestResult Ingest(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
	                        const string &body);
	void EnsureTargetTables();
	//! Stop the sealer thread and seal any remaining buffered rows before the writer
	//! connection / database go away. Idempotent; called from Close() and ~OtlpServer().
	void ShutdownIngest();
	//! Write a server-side diagnostic to duckdb_logs under the OTLP log type. No-op
	//! if the database has been closed. Safe to call from any worker thread.
	void LogServerEvent(const string &message) const;

	static void ListenThread(OtlpServer *server);

	std::thread listen_thread;
	std::atomic<idx_t> active_requests {0};
	std::atomic<idx_t> total_requests {0};
	std::atomic<idx_t> total_rows {0};

	// --- request path (runs on httplib worker threads; buffers, never commits) ---
	void BufferSignal(OtlpSignalType signal_type, const string &body, OtlpInputFormat format, OtlpIngestResult &result,
	                  idx_t &admission_bytes);
	void BufferMetrics(const string &body, OtlpInputFormat format, OtlpIngestResult &result, idx_t &admission_bytes);
	void AppendArrowBatch(OtlpSignalBuffer &buf, ArrowArray &array, ArrowSchema &schema, OtlpIngestResult &result,
	                      idx_t &admission_bytes);
	void BufferAppend(OtlpSignalBuffer &buf, DataChunk &chunk, idx_t &admission_bytes);
	OtlpSignalBuffer &BufferFor(OtlpSignalType signal_type);
	bool TryReserveAdmission(idx_t bytes, idx_t &current);
	void ClaimUnsealedAdmission(idx_t &bytes);
	void ReleaseAdmission(idx_t bytes);

	// --- seal path (single writer) ---
	void InitBuffers();
	void StartSealer();
	void SealerLoop();
	OtlpIngestResult SealOnce();
	void RequestSeal();
	bool SealAgeDue() const;

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
	//! Subset of admitted_bytes already attached to buffered rows and awaiting seal.
	//! Protected separately so admission checks stay atomic-only on the request path.
	std::mutex admission_mutex;
	idx_t unsealed_admission_bytes = 0;

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

	std::atomic<idx_t> seals_total {0};
	std::atomic<idx_t> seal_failures_total {0};
	std::atomic<int64_t> last_seal_unix_ms {0};
	mutable mutex seal_error_mutex;
	string seal_last_error;

	// --- HTTP transport (httplib). PIMPL so httplib.hpp stays out of this header. ---
	class Impl;
	unique_ptr<Impl> impl;
	std::atomic<bool> is_running {false};
	std::atomic<bool> listener_failed {false};
	mutable mutex error_mutex;
	string last_error;
};

} // namespace duckdb
