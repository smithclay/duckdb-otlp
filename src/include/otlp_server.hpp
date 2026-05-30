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

protected:
	std::vector<std::thread> listen_threads;
	std::atomic<idx_t> active_requests {0};
	std::atomic<idx_t> total_requests {0};
	std::atomic<idx_t> total_rows {0};

private:
	void TransformAndAppend(OtlpRequestKind request_kind, const string &body, OtlpInputFormat format,
	                        OtlpIngestResult &result);
	void AppendSignal(OtlpSignalType signal_type, const string &table_name, const string &body, OtlpInputFormat format,
	                  OtlpIngestResult &result);
	void AppendArrowBatch(const string &table_name, const ArrowArray &array, const ArrowSchema &schema,
	                      OtlpIngestResult &result);
	void CreateOrValidateTable(OtlpSignalType signal_type, const string &table_name);

private:
	weak_ptr<DatabaseInstance> db_ptr;
	unique_ptr<Connection> append_connection;
	mutex append_mutex;
	OtlpUri uri;
	OtlpServerConfig config;
};

class HttpOtlpServer : public OtlpServer {
public:
	HttpOtlpServer(ClientContext &context, const OtlpUri &uri, OtlpServerConfig config);
	void StopAccepting() override;
	void Close() override;
	~HttpOtlpServer() override;

private:
	static void ListenThread(HttpOtlpServer *server);

private:
	class Impl;
	unique_ptr<Impl> impl;
	bool is_running = false;
};

} // namespace duckdb
