#pragma once

#include "duckdb.hpp"
#include "duckdb/common/encryption_state.hpp"

#include "otlp_uri.hpp"
#include "otlp2records.h"

#include <atomic>
#include <thread>

namespace duckdb {

class ClientContext;
class Connection;
class DatabaseInstance;

enum class OtlpRequestKind : uint8_t { LOGS, TRACES, METRICS };

struct OtlpServerConfig {
	string token;
	string schema_name = "main";
	bool create_tables = true;
	idx_t max_body_bytes = 16ULL * 1024ULL * 1024ULL;
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
	idx_t ActiveRequests() const {
		return active_requests.load();
	}
	idx_t TotalRequests() const {
		return total_requests.load();
	}
	idx_t TotalRows() const {
		return total_rows.load();
	}

protected:
	bool CheckAuth(const string &authorization, const string &api_key) const;
	OtlpIngestResult Ingest(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
	                        const string &body);
	void EnsureTargetTables();
	//! Write a server-side diagnostic to duckdb_logs under the OTLP log type. No-op
	//! if the database has been closed. Safe to call from any worker thread.
	void LogServerEvent(const string &message) const;

protected:
	std::vector<std::thread> listen_threads;
	std::atomic<idx_t> active_requests {0};
	std::atomic<idx_t> total_requests {0};
	std::atomic<idx_t> total_rows {0};

private:
	//! Return this worker thread's long-lived Connection, lazily creating one on
	//! first use. Each httplib worker thread gets its own Connection so requests
	//! run concurrently; only DuckDB's per-table append lock briefly serializes at
	//! commit. A Connection is never shared between threads, so no request-path
	//! mutex is needed and each request's BEGIN/COMMIT is isolated and atomic.
	Connection &GetWorkerConnection();
	void TransformAndAppend(Connection &con, OtlpRequestKind request_kind, const string &body, OtlpInputFormat format,
	                        OtlpIngestResult &result);
	void AppendSignal(Connection &con, OtlpSignalType signal_type, const string &table_name, const string &body,
	                  OtlpInputFormat format, OtlpIngestResult &result);
	void AppendArrowBatch(Connection &con, const string &table_name, const ArrowArray &array, const ArrowSchema &schema,
	                      OtlpIngestResult &result);
	void CreateOrValidateTable(Connection &con, OtlpSignalType signal_type, const string &table_name);

private:
	weak_ptr<DatabaseInstance> db_ptr;
	//! Guards worker_connections structural mutation only (find/insert), not query
	//! execution — a worker only ever touches its own Connection.
	mutex connections_mutex;
	unordered_map<std::thread::id, unique_ptr<Connection>> worker_connections;
	OtlpUri uri;
	OtlpServerConfig config;
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
