#include "otlp_server.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

#include "otlp_arrow.hpp"
#include "otlp_log.hpp"

#ifndef __EMSCRIPTEN__
#include "httplib.hpp"
#endif

namespace duckdb {

namespace {

static constexpr idx_t TOKEN_BYTES = 16;
static constexpr idx_t APPEND_CHUNK_SIZE = STANDARD_VECTOR_SIZE;

struct OtlpTargetTable {
	OtlpSignalType signal_type;
	const char *table_name;
};

static const OtlpTargetTable TARGET_TABLES[] = {
    {OTLP_SIGNAL_LOGS, "otlp_logs"},
    {OTLP_SIGNAL_TRACES, "otlp_traces"},
    {OTLP_SIGNAL_METRICS_GAUGE, "otlp_metrics_gauge"},
    {OTLP_SIGNAL_METRICS_SUM, "otlp_metrics_sum"},
    {OTLP_SIGNAL_METRICS_HISTOGRAM, "otlp_metrics_histogram"},
    {OTLP_SIGNAL_METRICS_EXP_HISTOGRAM, "otlp_metrics_exp_histogram"},
};

static string HexEncode(const data_t *bytes, idx_t n) {
	string result(n * 2, '\0');
	for (idx_t i = 0; i < n; i++) {
		result[2 * i] = Blob::HEX_TABLE[bytes[i] >> 4];
		result[2 * i + 1] = Blob::HEX_TABLE[bytes[i] & 0x0F];
	}
	return result;
}

static bool TimingSafeEqual(const string &a, const string &b) {
	if (a.size() != b.size()) {
		return false;
	}
	volatile unsigned char diff = 0;
	for (idx_t i = 0; i < a.size(); i++) {
		diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
	}
	return diff == 0;
}

static string QuoteIdentifier(const string &identifier) {
	return "\"" + StringUtil::Replace(identifier, "\"", "\"\"") + "\"";
}

static string QualifiedTable(const string &schema_name, const string &table_name) {
	return QuoteIdentifier(schema_name) + "." + QuoteIdentifier(table_name);
}

static void RunSQL(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (!result || result->HasError()) {
		auto error = result ? result->GetError() : string("DuckDB query failed");
		throw IOException("%s", error);
	}
}

class OtlpHttpError : public std::exception {
public:
	OtlpHttpError(int status_p, string message_p) : status(status_p), message(std::move(message_p)) {
	}

	const char *what() const noexcept override {
		return message.c_str();
	}

	int status;
	string message;
};

static string JsonEscape(const string &input) {
	string result;
	result.reserve(input.size() + 8);
	for (auto c : input) {
		switch (c) {
		case '\\':
			result += "\\\\";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			result += c;
			break;
		}
	}
	return result;
}

static OtlpInputFormat FormatFromContentType(const string &content_type) {
	auto value = content_type;
	auto semicolon = value.find(';');
	if (semicolon != string::npos) {
		value = value.substr(0, semicolon);
	}
	StringUtil::Trim(value);
	value = StringUtil::Lower(value);
	if (value == "application/json" || value == "application/otlp+json") {
		return OTLP_FORMAT_JSON;
	}
	if (value == "application/x-ndjson") {
		return OTLP_FORMAT_JSONL;
	}
	if (value == "application/x-protobuf" || value == "application/protobuf" || value == "application/otlp") {
		return OTLP_FORMAT_PROTOBUF;
	}
	// 415: a genuine content-type problem. Raise it as an HTTP error directly rather
	// than InvalidInputException, which is also thrown for non-content-type reasons.
	throw OtlpHttpError(415, StringUtil::Format("unsupported content-type %s", value.empty() ? "<missing>" : value));
}

static bool IsUnsupportedEncoding(const string &content_encoding) {
	if (content_encoding.empty()) {
		return false;
	}
	auto value = StringUtil::Lower(content_encoding);
	StringUtil::Trim(value);
	if (value == "identity") {
		return false;
	}
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
	if (value == "gzip" || value == "deflate") {
		return false;
	}
#endif
	return true;
}

static const char *TableNameForSignal(OtlpSignalType signal_type) {
	for (auto &target : TARGET_TABLES) {
		if (target.signal_type == signal_type) {
			return target.table_name;
		}
	}
	throw InternalException("Unknown OTLP signal type");
}

} // namespace

OtlpServer::OtlpServer(ClientContext &context, OtlpUri uri_p, OtlpServerConfig config_p)
    : db_ptr(context.db), uri(std::move(uri_p)), config(std::move(config_p)) {
	ValidateToken(config.token);
	if (!db_ptr.lock()) {
		throw InternalException("Database was closed");
	}
	EnsureTargetTables();
}

Connection &OtlpServer::GetWorkerConnection() {
	auto tid = std::this_thread::get_id();
	std::lock_guard<std::mutex> lock(connections_mutex);
	auto it = worker_connections.find(tid);
	if (it != worker_connections.end()) {
		return *it->second;
	}
	auto db = db_ptr.lock();
	if (!db) {
		throw IOException("Database was closed");
	}
	auto con = make_uniq<Connection>(*db);
	con->context->config.enable_progress_bar = false;
	auto &ref = *con;
	// unordered_map keeps element pointers stable across rehash, so `ref` stays
	// valid after the lock is released and other workers insert their own entries.
	worker_connections.emplace(tid, std::move(con));
	return ref;
}

OtlpServer::~OtlpServer() {
}

void OtlpServer::LogServerEvent(const string &message) const {
	auto db = db_ptr.lock();
	if (!db) {
		return;
	}
	auto &logger = Logger::Get(*db);
	if (logger.ShouldLog(OtlpLogType::NAME, OtlpLogType::LEVEL)) {
		logger.WriteLog(OtlpLogType::NAME, OtlpLogType::LEVEL, message);
	}
}

void OtlpServer::ValidateToken(const string &token) {
	// 16 is the deliberate floor for *user-supplied* tokens. Auto-generated tokens
	// (GenerateRandomToken) carry a full 128 bits of entropy as 32 hex chars; the
	// lower minimum only bounds how weak a hand-picked token may be.
	if (token.size() < 16) {
		throw InvalidInputException("OTLP server token must be at least 16 characters long");
	}
}

string OtlpServer::GenerateRandomToken(DatabaseInstance &db) {
	auto encryption_util = db.GetEncryptionUtil(false);
	if (!encryption_util) {
		throw IOException("Cannot generate OTLP server token: no encryption/RNG provider is available");
	}
	auto metadata =
	    make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, TOKEN_BYTES, EncryptionTypes::EncryptionVersion::NONE);
	auto rng = encryption_util->CreateEncryptionState(std::move(metadata));
	if (!rng) {
		throw IOException("Cannot generate OTLP server token: RNG state could not be created");
	}
	data_t bytes[TOKEN_BYTES];
	rng->GenerateRandomData(bytes, TOKEN_BYTES);
	return HexEncode(bytes, TOKEN_BYTES);
}

bool OtlpServer::CheckAuth(const string &authorization, const string &api_key) const {
	// Accept the request if EITHER a Bearer token OR an x-api-key matches. Checking
	// both independently avoids a wrong/malformed Authorization header masking a
	// valid x-api-key (and vice versa). RFC 7235 schemes are case-insensitive.
	if (authorization.size() >= 7 && StringUtil::Lower(authorization.substr(0, 7)) == "bearer ") {
		auto bearer = authorization.substr(7);
		if (!bearer.empty() && TimingSafeEqual(bearer, config.token)) {
			return true;
		}
	}
	if (!api_key.empty() && TimingSafeEqual(api_key, config.token)) {
		return true;
	}
	return false;
}

void OtlpServer::EnsureTargetTables() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	// Runs once at construction (single-threaded), so a throwaway connection is fine.
	Connection con(*db);
	con.context->config.enable_progress_bar = false;
	for (auto &target : TARGET_TABLES) {
		CreateOrValidateTable(con, target.signal_type, target.table_name);
	}
}

void OtlpServer::CreateOrValidateTable(Connection &con, OtlpSignalType signal_type, const string &table_name) {
	ArrowSchema arrow_schema;
	OtlpStatus status = otlp_get_schema(signal_type, &arrow_schema);
	if (status != OTLP_OK) {
		throw IOException("Failed to get OTLP schema: %s", otlp_status_message(status));
	}

	try {
		vector<LogicalType> expected_types;
		vector<string> expected_names;
		GetArrowSchemaColumns(arrow_schema, expected_types, expected_names);

		auto qualified = QualifiedTable(config.schema_name, table_name);
		if (config.create_tables) {
			string sql = "CREATE TABLE IF NOT EXISTS " + qualified + " (";
			for (idx_t i = 0; i < expected_names.size(); i++) {
				if (i > 0) {
					sql += ", ";
				}
				sql += QuoteIdentifier(expected_names[i]) + " " + expected_types[i].ToString();
			}
			sql += ")";
			RunSQL(con, sql);
		}

		auto result = con.Query("SELECT * FROM " + qualified + " LIMIT 0");
		if (!result || result->HasError()) {
			throw IOException("Target table %s is not available: %s", qualified,
			                  result ? result->GetError() : "query failed");
		}
		if (result->names.size() != expected_names.size()) {
			throw InvalidInputException("Target table %s has %llu columns, expected %llu", qualified,
			                            static_cast<uint64_t>(result->names.size()),
			                            static_cast<uint64_t>(expected_names.size()));
		}
		for (idx_t i = 0; i < expected_names.size(); i++) {
			if (result->names[i] != expected_names[i]) {
				throw InvalidInputException("Target table %s column %llu is %s, expected %s", qualified,
				                            static_cast<uint64_t>(i), result->names[i], expected_names[i]);
			}
			if (result->types[i] != expected_types[i]) {
				throw InvalidInputException("Target table %s column %s has type %s, expected %s", qualified,
				                            expected_names[i], result->types[i].ToString(),
				                            expected_types[i].ToString());
			}
		}
	} catch (...) {
		if (arrow_schema.release) {
			arrow_schema.release(&arrow_schema);
		}
		throw;
	}
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}
}

OtlpIngestResult OtlpServer::Ingest(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
                                    const string &body) {
	if (body.size() > config.max_body_bytes) {
		throw OtlpHttpError(413, StringUtil::Format("payload has %llu bytes; max is %llu",
		                                            static_cast<uint64_t>(body.size()),
		                                            static_cast<uint64_t>(config.max_body_bytes)));
	}
	if (IsUnsupportedEncoding(content_encoding)) {
		throw OtlpHttpError(415, StringUtil::Format("unsupported content-encoding %s", content_encoding));
	}
	auto format = FormatFromContentType(content_type);

	// Each worker thread owns its Connection, so concurrent requests run in
	// parallel; this request's transaction is fully isolated and either commits
	// all signals or rolls them all back.
	auto &con = GetWorkerConnection();
	OtlpIngestResult result;
	RunSQL(con, "BEGIN TRANSACTION");
	try {
		TransformAndAppend(con, kind, body, format, result);
		RunSQL(con, "COMMIT");
		total_rows.fetch_add(result.rows);
		return result;
	} catch (...) {
		try {
			RunSQL(con, "ROLLBACK");
		} catch (std::exception &rollback_ex) {
			// The original ingest error is rethrown below; surface the rollback
			// failure too so a wedged transaction state isn't lost silently.
			LogServerEvent(StringUtil::Format("ingest rollback failed: %s", rollback_ex.what()));
		}
		throw;
	}
}

void OtlpServer::TransformAndAppend(Connection &con, OtlpRequestKind request_kind, const string &body,
                                    OtlpInputFormat format, OtlpIngestResult &result) {
	switch (request_kind) {
	case OtlpRequestKind::LOGS:
		AppendSignal(con, OTLP_SIGNAL_LOGS, TableNameForSignal(OTLP_SIGNAL_LOGS), body, format, result);
		break;
	case OtlpRequestKind::TRACES:
		AppendSignal(con, OTLP_SIGNAL_TRACES, TableNameForSignal(OTLP_SIGNAL_TRACES), body, format, result);
		break;
	case OtlpRequestKind::METRICS:
		// TODO(perf): this re-parses `body` once per metric shape (4x). Collapsing to
		// a single parse requires an otlp2records FFI change that fans one parse out
		// to all present shapes; tracked as a follow-up (see AGENTS.md Known
		// Limitations). Kept as 4 calls for now to stay on the stable single-signal FFI.
		AppendSignal(con, OTLP_SIGNAL_METRICS_GAUGE, TableNameForSignal(OTLP_SIGNAL_METRICS_GAUGE), body, format,
		             result);
		AppendSignal(con, OTLP_SIGNAL_METRICS_SUM, TableNameForSignal(OTLP_SIGNAL_METRICS_SUM), body, format, result);
		AppendSignal(con, OTLP_SIGNAL_METRICS_HISTOGRAM, TableNameForSignal(OTLP_SIGNAL_METRICS_HISTOGRAM), body,
		             format, result);
		AppendSignal(con, OTLP_SIGNAL_METRICS_EXP_HISTOGRAM, TableNameForSignal(OTLP_SIGNAL_METRICS_EXP_HISTOGRAM),
		             body, format, result);
		break;
	default:
		throw InternalException("Unknown OTLP request kind");
	}
}

void OtlpServer::AppendSignal(Connection &con, OtlpSignalType signal_type, const string &table_name, const string &body,
                              OtlpInputFormat format, OtlpIngestResult &result) {
	ArrowArray array;
	ArrowSchema schema;
	OtlpStatus status = otlp_transform(signal_type, format, reinterpret_cast<const uint8_t *>(body.data()), body.size(),
	                                   &array, &schema);
	if (status != OTLP_OK) {
		throw OtlpHttpError(
		    400, StringUtil::Format("OTLP parse failed for %s: %s", table_name, otlp_status_message(status)));
	}

	try {
		AppendArrowBatch(con, table_name, array, schema, result);
	} catch (...) {
		if (array.release) {
			array.release(&array);
		}
		if (schema.release) {
			schema.release(&schema);
		}
		throw;
	}
	if (array.release) {
		array.release(&array);
	}
	if (schema.release) {
		schema.release(&schema);
	}
}

void OtlpServer::AppendArrowBatch(Connection &con, const string &table_name, const ArrowArray &array,
                                  const ArrowSchema &schema, OtlpIngestResult &result) {
	if (array.length <= 0) {
		return;
	}

	vector<LogicalType> types;
	vector<string> names;
	GetArrowSchemaColumns(schema, types, names);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(*con.context), types, APPEND_CHUNK_SIZE);
	Appender appender(con, config.schema_name, table_name);

	for (idx_t offset = 0; offset < static_cast<idx_t>(array.length); offset += APPEND_CHUNK_SIZE) {
		auto count = MinValue<idx_t>(APPEND_CHUNK_SIZE, static_cast<idx_t>(array.length) - offset);
		chunk.Reset();
		CopyArrowStructToDataChunk(array, schema, chunk, offset, count);
		appender.AppendDataChunk(chunk);
		result.rows += count;
		result.batches++;
	}
	appender.Close();
}

#ifndef __EMSCRIPTEN__

class HttpOtlpServer::Impl {
public:
	unique_ptr<duckdb_httplib::Server> server;
};

static void SetJson(duckdb_httplib::Response &res, int status, const string &json) {
	res.status = status;
	res.set_content(json, "application/json");
}

static void SetError(duckdb_httplib::Response &res, int status, const string &reason, const string &message) {
	SetJson(res, status, "{\"error\":\"" + JsonEscape(reason) + "\",\"message\":\"" + JsonEscape(message) + "\"}");
}

HttpOtlpServer::HttpOtlpServer(ClientContext &context, const OtlpUri &uri_p, OtlpServerConfig config_p)
    : OtlpServer(context, uri_p, config_p), impl(make_uniq<Impl>()) {
	impl->server = make_uniq<duckdb_httplib::Server>();
	// Wide worker pool (mirrors duckdb-quack): each keep-alive connection holds a
	// worker thread for its lifetime, so we need enough threads to serve many
	// concurrent exporters at once. Each worker uses its own DuckDB Connection
	// (see OtlpServer::GetWorkerConnection), so requests parse/append in parallel
	// and only DuckDB's per-table append lock briefly serializes the commit.
	impl->server->new_task_queue = [] {
		return new duckdb_httplib::ThreadPool(128);
	};
	impl->server->set_keep_alive_max_count(128);
	impl->server->set_keep_alive_timeout(10);
	impl->server->set_tcp_nodelay(true);
	impl->server->set_payload_max_length(static_cast<size_t>(config_p.max_body_bytes));

	impl->server->Get("/healthz", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		SetJson(res, 200, "{\"status\":\"ok\"}");
	});

	auto post_handler = [&](OtlpRequestKind kind) {
		return [this, kind](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
			// RAII so active_requests is decremented on every exit path, including a
			// throw that escapes the catch chain below (e.g. allocation failure).
			struct RequestGuard {
				std::atomic<idx_t> &counter;
				explicit RequestGuard(std::atomic<idx_t> &counter_p) : counter(counter_p) {
					counter++;
				}
				~RequestGuard() {
					counter--;
				}
			} request_guard {active_requests};
			total_requests++;
			try {
				if (!CheckAuth(req.get_header_value("Authorization"), req.get_header_value("x-api-key"))) {
					SetError(res, 401, "unauthorized", "missing or invalid Authorization bearer token or x-api-key");
				} else {
					auto result = Ingest(kind, req.get_header_value("Content-Type"),
					                     req.get_header_value("Content-Encoding"), req.body);
					SetJson(res, 200,
					        StringUtil::Format("{\"status\":\"ok\",\"rows\":%llu,\"batches\":%llu}",
					                           static_cast<uint64_t>(result.rows),
					                           static_cast<uint64_t>(result.batches)));
				}
			} catch (OtlpHttpError &ex) {
				SetError(res, ex.status, "request_failed", ex.what());
			} catch (InvalidInputException &ex) {
				SetError(res, 400, "bad_request", ex.what());
			} catch (IOException &ex) {
				// 500-class failures are otherwise only visible in the client's
				// response body; log them so the operator has a server-side trace.
				LogServerEvent(StringUtil::Format("ingest I/O error: %s", ex.what()));
				SetError(res, 500, "internal_error", ex.what());
			} catch (std::exception &ex) {
				LogServerEvent(StringUtil::Format("ingest internal error: %s", ex.what()));
				SetError(res, 500, "internal_error", ex.what());
			}
		};
	};

	impl->server->Post("/v1/logs", post_handler(OtlpRequestKind::LOGS));
	impl->server->Post("/v1/traces", post_handler(OtlpRequestKind::TRACES));
	impl->server->Post("/v1/metrics", post_handler(OtlpRequestKind::METRICS));

	if (!impl->server->is_valid()) {
		throw IOException("Failed to instantiate OTLP HTTP server at %s / %s", uri_p.Uri(), uri_p.Http());
	}
	// Bind synchronously here so that bind() failures (e.g. EADDRINUSE) propagate
	// to the caller of otlp_serve() rather than being lost on the listener thread.
	if (!impl->server->bind_to_port(uri_p.Host(), uri_p.Port())) {
		throw IOException("Failed to bind OTLP HTTP server to %s (address in use, permission denied, or invalid host/"
		                  "port)",
		                  uri_p.Http());
	}
	is_running.store(true);
	listen_threads.push_back(std::thread(ListenThread, this));
}

void HttpOtlpServer::StopAccepting() {
	// Closes the listening socket only. Idempotent. Safe to call from a
	// request-handler thread — does not wait on httplib's task queue.
	if (is_running.exchange(false)) {
		impl->server->stop();
	}
}

void HttpOtlpServer::Close() {
	// Stops accepting new connections AND joins the listener threads (NOT the
	// httplib worker pool). Must not be called from a worker thread — the
	// listener's exit path inside httplib joins all workers, so a worker
	// joining the listener would deadlock through that chain.
	StopAccepting();
	for (auto &thread : listen_threads) {
		if (thread.joinable()) {
			thread.join();
		}
	}
}

HttpOtlpServer::~HttpOtlpServer() {
	try {
		HttpOtlpServer::Close();
	} catch (std::exception &) {
	}
}

void HttpOtlpServer::ListenThread(HttpOtlpServer *server) {
	// The socket is already bound (synchronously, in the constructor); this only
	// runs the accept loop. Catch everything so the listener thread never lets an
	// exception escape — that would call std::terminate and abort the host process.
	try {
		server->impl->server->listen_after_bind();
	} catch (std::exception &ex) {
		server->is_running.store(false);
		server->listener_failed.store(true);
		{
			std::lock_guard<std::mutex> lock(server->error_mutex);
			server->last_error = ex.what();
		}
		server->LogServerEvent(
		    StringUtil::Format("OTLP listener for %s stopped: %s", server->ListenUri().Uri(), ex.what()));
	} catch (...) {
		server->is_running.store(false);
		server->listener_failed.store(true);
		{
			std::lock_guard<std::mutex> lock(server->error_mutex);
			server->last_error = "unknown error in listen loop";
		}
		server->LogServerEvent(
		    StringUtil::Format("OTLP listener for %s stopped: unknown error", server->ListenUri().Uri()));
	}
}

#else

class HttpOtlpServer::Impl {};

HttpOtlpServer::HttpOtlpServer(ClientContext &context, const OtlpUri &uri_p, OtlpServerConfig config_p)
    : OtlpServer(context, uri_p, std::move(config_p)), impl(make_uniq<Impl>()) {
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
}

void HttpOtlpServer::StopAccepting() {
}

void HttpOtlpServer::Close() {
}

HttpOtlpServer::~HttpOtlpServer() {
}

void HttpOtlpServer::ListenThread(HttpOtlpServer *server) {
}

#endif

} // namespace duckdb
