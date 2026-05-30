#pragma once

#include "duckdb.hpp"
#include "duckdb/common/encryption_state.hpp"

#include "otlp_uri.hpp"
#include "otlp2records.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <thread>

namespace duckdb {

class ClientContext;
class Connection;
class DatabaseInstance;
class ColumnDataCollection;
class DataChunk;
struct OtlpSignalBuffer;

enum class OtlpRequestKind : uint8_t { LOGS, TRACES, METRICS };

struct OtlpServerConfig {
	string token;
	//! Target catalog (attached database). Empty = the connection's default catalog.
	//! Set this to an attached DuckLake catalog name to stream OTLP into a lakehouse.
	string catalog_name;
	string schema_name = "main";
	bool create_tables = true;
	idx_t max_body_bytes = 16ULL * 1024ULL * 1024ULL;
	//! Buffered group-commit ("seal") tuning. Ingest buffers rows in memory and a
	//! single writer seals them on a size or age trigger (one DuckLake snapshot per
	//! seal), avoiding per-request Parquet files and write conflicts.
	idx_t seal_target_bytes = 64ULL * 1024ULL * 1024ULL;   //! seal when a signal buffer reaches this
	int64_t seal_max_age_ms = 5000;                        //! seal when the oldest buffered row is this old
	idx_t max_buffered_bytes = 512ULL * 1024ULL * 1024ULL; //! hard cap across all signals -> 503
};

struct OtlpIngestResult {
	idx_t rows = 0;
	idx_t batches = 0;
};

class OtlpServer {
public:
	OtlpServer(ClientContext &context, OtlpUri uri, OtlpServerConfig config);
	virtual ~OtlpServer();

	//! Stop accepting new connections (close the listener socket) without joining
	//! listener threads. Safe to call from a request-handler thread — does not wait
	//! on httplib's task queue, which would deadlock when the caller is a worker.
	virtual void StopAccepting() {};

	//! Synchronously stop accepting connections and join the listener threads. Must
	//! NOT be called from a worker / request-handler thread; httplib's listen-loop
	//! teardown joins all workers, which would deadlock.
	virtual void Close() {};

	//! Whether the server is currently accepting connections. False once the
	//! listener thread has exited (e.g. an error after a successful bind), so an
	//! operator can tell a registered server has fallen over.
	virtual bool IsListening() const {
		return true;
	}
	//! Last fatal listener error, or empty if none.
	virtual string LastError() const {
		return string();
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
	idx_t BufferedRows() const {
		return buffered_rows.load();
	}
	idx_t BufferedBytes() const {
		return buffered_bytes.load();
	}
	idx_t SealsTotal() const {
		return seals_total.load();
	}
	//! Milliseconds since the last successful seal, or -1 if none yet.
	int64_t LastSealAgeMs() const;
	string SealLastError() const {
		std::lock_guard<std::mutex> lock(seal_error_mutex);
		return seal_last_error;
	}

	//! Force a synchronous seal of all buffered rows (used by otlp_flush). Returns the
	//! number of rows sealed. When run_checkpoint is set, also compacts the catalog.
	OtlpIngestResult FlushNow(bool run_checkpoint);

protected:
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

protected:
	std::vector<std::thread> listen_threads;
	std::atomic<idx_t> active_requests {0};
	std::atomic<idx_t> total_requests {0};
	std::atomic<idx_t> total_rows {0};

private:
	// --- request path (runs on httplib worker threads; buffers, never commits) ---
	void TransformAndBuffer(OtlpRequestKind request_kind, const string &body, OtlpInputFormat format,
	                        OtlpIngestResult &result);
	void BufferSignal(OtlpSignalType signal_type, const string &body, OtlpInputFormat format, OtlpIngestResult &result);
	void BufferAppend(OtlpSignalBuffer &buf, DataChunk &chunk);
	OtlpSignalBuffer &BufferFor(OtlpSignalType signal_type);

	// --- seal path (single writer) ---
	void InitBuffers();
	void StartSealer();
	void SealerLoop();
	OtlpIngestResult SealOnce(bool run_checkpoint);
	void RequestSeal();

	void CreateOrValidateTable(Connection &con, OtlpSignalType signal_type, const string &table_name);

private:
	weak_ptr<DatabaseInstance> db_ptr;
	OtlpUri uri;
	OtlpServerConfig config;

	// Per-signal in-memory buffers. Guarded by buffer_mutex for all structural access;
	// a worker only briefly locks to Append a converted chunk.
	mutex buffer_mutex;
	std::vector<unique_ptr<OtlpSignalBuffer>> signal_buffers;
	std::atomic<idx_t> buffered_rows {0};
	std::atomic<idx_t> buffered_bytes {0};
	bool have_unsealed = false;                           //! guarded by buffer_mutex
	std::chrono::steady_clock::time_point first_unsealed; //! guarded by buffer_mutex

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
	std::atomic<int64_t> last_seal_unix_ms {0};
	mutable mutex seal_error_mutex;
	string seal_last_error;
};

class HttpOtlpServer : public OtlpServer {
public:
	HttpOtlpServer(ClientContext &context, const OtlpUri &uri, const OtlpServerConfig &config);
	void StopAccepting() override;
	void Close() override;
	bool IsListening() const override {
		return is_running.load() && !listener_failed.load();
	}
	string LastError() const override {
		std::lock_guard<std::mutex> lock(error_mutex);
		return last_error;
	}
	~HttpOtlpServer() override;

private:
	static void ListenThread(HttpOtlpServer *server);

private:
	class Impl;
	unique_ptr<Impl> impl;
	std::atomic<bool> is_running {false};
	std::atomic<bool> listener_failed {false};
	mutable mutex error_mutex;
	string last_error;
};

} // namespace duckdb
