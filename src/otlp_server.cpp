#include "otlp_server.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

#include "otlp_arrow.hpp"
#include "otlp_column_promote.hpp"
#include "otlp_log.hpp"
#include "otlp_server_internal.hpp"
#include "otlp_sql_util.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>

namespace duckdb {

static int64_t NowUnixMs();

//! -1 if never set (0); clamps to 0 if the clock went backwards.
static int64_t AgeMsSince(int64_t last_unix_ms);

namespace {

static constexpr idx_t TOKEN_BYTES = 16;
static constexpr idx_t APPEND_CHUNK_SIZE = STANDARD_VECTOR_SIZE;

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

static const char *PartitionTimestampColumn(const string &table_name) {
	if (table_name == "otlp_traces") {
		return "start_time_unix_nano";
	}
	return "time_unix_nano";
}

static bool IsUnsupportedCatalogMaintenanceError(const string &message) {
	auto lower = StringUtil::Lower(message);
	return (lower.find("checkpoint") != string::npos &&
	        (lower.find("not implemented") != string::npos || lower.find("not supported") != string::npos ||
	         lower.find("does not support") != string::npos || lower.find("unsupported") != string::npos)) ||
	       lower.find("checkpoint is only supported") != string::npos;
}

//! A maintenance statement that lost a race to another writer's CHECKPOINT on a shared catalog
//! (Postgres: "could not serialize access due to concurrent delete/update"). Not a failure.
static bool IsContendedCatalogMaintenanceError(const string &message) {
	auto lower = StringUtil::Lower(message);
	return lower.find("could not serialize access") != string::npos ||
	       lower.find("serialization failure") != string::npos || lower.find("concurrent delete") != string::npos ||
	       lower.find("concurrent update") != string::npos;
}

static void RunSQL(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (!result || result->HasError()) {
		auto error = result ? result->GetError() : string("DuckDB query failed");
		throw IOException("%s", error);
	}
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
	if (value == "application/x-ndjson" || value == "application/jsonl") {
		return OTLP_FORMAT_JSONL;
	}
	if (value == "application/x-protobuf" || value == "application/protobuf" || value == "application/otlp") {
		return OTLP_FORMAT_PROTOBUF;
	}
	// Not InvalidInputException, which is also thrown for non-content-type reasons.
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

} // namespace

LogicalType OtlpServer::BufferColumnType(const LogicalType &imported) {
	return imported.id() == LogicalTypeId::TIMESTAMP_NS ? LogicalType::TIMESTAMP : imported;
}

void OtlpServer::GetSignalColumns(ClientContext &context, OtlpSignalType signal_type, vector<LogicalType> &types,
                                  vector<string> &names) {
	auto table = OtlpArrowTable::ForSignal(context, signal_type);
	for (auto &type : table->types) {
		types.push_back(BufferColumnType(type));
	}
	names = table->names;
}

// Truncates toward zero.
static void TimestampNsToTimestamp(Vector &source, Vector &result, idx_t count) {
	auto values = source.Values<int64_t>();
	auto writer = FlatVector::Writer<timestamp_t>(result, count);
	for (auto entry : values) {
		if (!entry.IsValid()) {
			writer.WriteNull();
			continue;
		}
		writer.WriteValue(timestamp_t(entry.GetValue() / Interval::NANOS_PER_MICRO));
	}
}

void OtlpServer::InitBuffers() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto &allocator = Allocator::Get(*db);
	for (auto signal_type : OTLP_SIGNALS) {
		auto arrow_table = OtlpArrowTable::ForSignal(*arrow_con->context, signal_type);
		vector<LogicalType> types;
		for (auto &type : arrow_table->types) {
			types.push_back(BufferColumnType(type));
		}
		auto collection = make_uniq<ColumnDataCollection>(allocator, types);
		vector<Identifier> identifiers(arrow_table->names.begin(), arrow_table->names.end());
		signal_buffers.push_back(make_uniq<OtlpSignalBuffer>(signal_type, OTLP_SIGNAL_TABLES[signal_type],
		                                                     std::move(types), std::move(identifiers),
		                                                     std::move(arrow_table), std::move(collection)));
	}
	parquet_view_settled.assign(signal_buffers.size(), false);
}

void OtlpServer::StartSealer() {
	sealer_thread = std::thread([this] { SealerLoop(); });
}

int64_t OtlpServer::LastSealAgeMs() const {
	return AgeMsSince(last_seal_unix_ms.load());
}

int64_t OtlpServer::LastMaintenanceAgeMs() const {
	return AgeMsSince(last_maintenance_unix_ms.load());
}

idx_t OtlpServer::BufferedRows() const {
	idx_t rows = 0;
	for (auto &buf : signal_buffers) {
		std::lock_guard<std::mutex> lock(buf->mutex);
		rows += buf->buffered_rows;
	}
	return rows;
}

idx_t OtlpServer::BufferedBytes() const {
	idx_t bytes = 0;
	for (auto &buf : signal_buffers) {
		std::lock_guard<std::mutex> lock(buf->mutex);
		if (buf->collection) {
			bytes += buf->collection->SizeInBytes();
		}
	}
	return bytes;
}

bool OtlpServer::SealStalled() const {
	if (seal_failures_total.load() == 0 || BufferedRows() == 0) {
		return false;
	}
	static constexpr int64_t STALL_SEAL_CYCLES = 3;
	auto last_seal_age = LastSealAgeMs();
	return last_seal_age < 0 || last_seal_age > STALL_SEAL_CYCLES * config.seal_max_age_ms;
}

int64_t OtlpServer::OldestBufferedAgeMs() const {
	bool found = false;
	std::chrono::steady_clock::time_point oldest;
	for (auto &buf : signal_buffers) {
		std::lock_guard<std::mutex> lock(buf->mutex);
		if (!buf->have_unsealed) {
			continue;
		}
		if (!found || buf->first_unsealed < oldest) {
			oldest = buf->first_unsealed;
			found = true;
		}
	}
	if (!found) {
		return -1;
	}
	return std::max<int64_t>(
	    0, std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - oldest).count());
}

vector<OtlpSealEvent> OtlpServer::SealHistory() const {
	std::lock_guard<std::mutex> lock(seal_history_mutex);
	return vector<OtlpSealEvent>(seal_history.begin(), seal_history.end());
}

void OtlpServer::RecordSealEvent(int64_t started_unix_ms, int64_t append_duration_ms, int64_t commit_duration_ms,
                                 idx_t rows_committed, idx_t admitted_bytes_committed, bool success,
                                 const string &error) {
	OtlpSealEvent event;
	event.seal_sequence = seal_sequence.fetch_add(1) + 1;
	event.started_unix_ms = started_unix_ms;
	event.completed_unix_ms = NowUnixMs();
	event.duration_ms = std::max<int64_t>(0, event.completed_unix_ms - event.started_unix_ms);
	event.append_duration_ms = append_duration_ms;
	event.commit_duration_ms = commit_duration_ms;
	event.rows_committed = rows_committed;
	event.admitted_bytes_committed = admitted_bytes_committed;
	event.success = success;
	event.seals_total = seals_total.load();
	event.seal_failures_total = seal_failures_total.load();
	event.committed_rows_total = committed_rows_total.load();
	event.error = error;
	std::lock_guard<std::mutex> lock(seal_history_mutex);
	static constexpr idx_t MAX_SEAL_HISTORY = 4096;
	if (seal_history.size() >= MAX_SEAL_HISTORY) {
		seal_history.pop_front();
	}
	seal_history.push_back(std::move(event));
}

void OtlpServer::LogServerEvent(const string &message, LogLevel level) const {
	auto db = db_ptr.lock();
	if (!db) {
		return;
	}
	auto &logger = Logger::Get(*db);
	if (logger.ShouldLog(OtlpLogType::NAME, level)) {
		logger.WriteLog(OtlpLogType::NAME, level, message);
	}
}

const char *OtlpTransportName(OtlpTransport transport) {
	return transport == OtlpTransport::GRPC ? "grpc" : "http";
}

void OtlpListener::RecordFailure(const string &error) {
	is_running.store(false);
	failed.store(true);
	{
		std::lock_guard<std::mutex> lock(error_mutex);
		last_error = error;
	}
	server.LogServerEvent(StringUtil::Format("OTLP listener for %s stopped: %s", spec.uri.Uri(), error),
	                      LogLevel::LOG_WARNING);
}

OtlpServer::OtlpServer(ClientContext &context, const vector<OtlpListenerSpec> &listener_specs,
                       const OtlpServerConfig &config_p)
    : db_ptr(context.db), config(config_p) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
#else
	if (listener_specs.empty()) {
		throw InvalidInputException("OTLP server requires at least one listen URI");
	}
	if (!config.disable_auth) {
		ValidateToken(config.token);
	}
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	arrow_con = make_uniq<Connection>(*db);
	EnsureTargetTables();
	InitBuffers();
	// The sealer thread is the only thing that commits, so concurrent DuckLake writes never conflict.
	writer_con = make_uniq<Connection>(*db);
	writer_con->context->config.enable_progress_bar = false;
	// Before the sealer can fire.
	ConfigureCatalogMaintenanceOptions();
	// Parquet-export mode has no table to ALTER.
	if (config.promote.Enabled() && config.parquet_export_path.empty()) {
		promoter = make_uniq<OtlpColumnPromoter>(config.promote, config.catalog_name, config.schema_name,
		                                         [this](const string &msg) { LogServerEvent(msg); });
		promoter->Initialize(*writer_con);
	}
	// After promotion has settled (it may disable itself), so the sinks project only existing columns.
	BuildSinks();
	StartSealer();

	// A throw here means ~OtlpServer() won't run: close started listeners and stop the sealer, or
	// a socket stays bound and a joinable std::thread is destroyed (std::terminate).
	try {
		for (auto &spec : listener_specs) {
			listeners.push_back(spec.transport == OtlpTransport::GRPC ? MakeOtlpGrpcListener(*this, spec)
			                                                          : MakeOtlpHttpListener(*this, spec));
			listeners.back()->Start();
		}
	} catch (...) {
		CloseListeners();
		ShutdownIngest();
		throw;
	}
#endif
}

static Value NullIfEmpty(const string &value) {
	return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
}

//! Ages are -1 until the event first happens.
static Value NullIfNegative(int64_t value) {
	return value < 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(value);
}

#define OTLP_COLUMN(name, type, expr)                                                                                  \
	{                                                                                                                  \
		name, LogicalTypeId::type, [](const OtlpServer &s, const OtlpListener &l) -> Value {                           \
			return expr;                                                                                               \
		}                                                                                                              \
	}

const vector<OtlpServerListColumn> &OtlpServer::ListColumns() {
	// Append new columns at the end; callers may read by index.
	static const vector<OtlpServerListColumn> columns = {
	    OTLP_COLUMN("listen_uri", VARCHAR, Value(l.Uri().Uri())),
	    OTLP_COLUMN("listen_url", VARCHAR, Value(l.Uri().Http())),
	    OTLP_COLUMN("host", VARCHAR, Value(l.Uri().Host())),
	    OTLP_COLUMN("port", USMALLINT, Value::USMALLINT(l.Uri().Port())),
	    OTLP_COLUMN("schema_name", VARCHAR, Value(s.config.schema_name)),
	    OTLP_COLUMN("active_requests", UBIGINT, Value::UBIGINT(s.active_requests.load())),
	    OTLP_COLUMN("total_requests", UBIGINT, Value::UBIGINT(s.total_requests.load())),
	    OTLP_COLUMN("total_rows", UBIGINT, Value::UBIGINT(s.total_rows.load())),
	    OTLP_COLUMN("is_listening", BOOLEAN, Value::BOOLEAN(l.IsListening())),
	    OTLP_COLUMN("last_error", VARCHAR, NullIfEmpty(l.LastError())),
	    OTLP_COLUMN("catalog_name", VARCHAR, Value(s.config.catalog_name)),
	    OTLP_COLUMN("buffered_rows", UBIGINT, Value::UBIGINT(s.BufferedRows())),
	    OTLP_COLUMN("admitted_bytes", UBIGINT, Value::UBIGINT(s.AdmittedBytes())),
	    OTLP_COLUMN("seal_target_bytes", UBIGINT, Value::UBIGINT(s.config.seal_target_bytes)),
	    OTLP_COLUMN("seal_max_age_ms", BIGINT, Value::BIGINT(s.config.seal_max_age_ms)),
	    OTLP_COLUMN("oldest_buffered_age_ms", BIGINT, NullIfNegative(s.OldestBufferedAgeMs())),
	    OTLP_COLUMN("last_seal_age_ms", BIGINT, NullIfNegative(s.LastSealAgeMs())),
	    OTLP_COLUMN("seals_total", UBIGINT, Value::UBIGINT(s.SealsTotal())),
	    OTLP_COLUMN("committed_rows_total", UBIGINT, Value::UBIGINT(s.CommittedRowsTotal())),
	    OTLP_COLUMN("seal_failures_total", UBIGINT, Value::UBIGINT(s.SealFailuresTotal())),
	    OTLP_COLUMN("seal_last_error", VARCHAR, NullIfEmpty(s.SealLastError())),
	    OTLP_COLUMN("maintenance_runs_total", UBIGINT, Value::UBIGINT(s.MaintenanceRunsTotal())),
	    OTLP_COLUMN("maintenance_failures_total", UBIGINT, Value::UBIGINT(s.MaintenanceFailuresTotal())),
	    OTLP_COLUMN("last_maintenance_age_ms", BIGINT, NullIfNegative(s.LastMaintenanceAgeMs())),
	    OTLP_COLUMN("maintenance_last_error", VARCHAR, NullIfEmpty(s.MaintenanceLastError())),
	    OTLP_COLUMN("buffered_bytes", UBIGINT, Value::UBIGINT(s.BufferedBytes())),
	    OTLP_COLUMN("promoted_columns_total", UBIGINT, Value::UBIGINT(s.PromotedColumnsTotal())),
	    OTLP_COLUMN("transport", VARCHAR, Value(OtlpTransportName(l.Transport()))),
	    OTLP_COLUMN("maintenance_contended_total", UBIGINT, Value::UBIGINT(s.MaintenanceContendedTotal())),
	};
	return columns;
}

#undef OTLP_COLUMN

const OtlpUri &OtlpServer::PrimaryUri() const {
	return listeners.front()->Uri();
}

void OtlpServer::CloseListeners() {
	// Stop accepting everywhere first so no transport admits rows while another drains.
	for (auto &listener : listeners) {
		listener->StopAccepting();
	}
	for (auto it = listeners.rbegin(); it != listeners.rend(); ++it) {
		(*it)->Close();
	}
}

void OtlpServer::Close() {
	// After every listener has closed, so the final seal includes rows from any transport.
	CloseListeners();
	ShutdownIngest();
}

OtlpServer::~OtlpServer() {
	try {
		Close();
	} catch (std::exception &) {
	}
}

void OtlpServer::ValidateToken(const string &token) {
	// Floor for user-supplied tokens; generated ones carry 128 bits.
	if (token.size() < otlp_limits::MIN_TOKEN_LENGTH) {
		throw InvalidInputException("OTLP server token must be at least %llu characters long",
		                            static_cast<uint64_t>(otlp_limits::MIN_TOKEN_LENGTH));
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
	if (config.disable_auth) {
		return true;
	}
	// Check both independently so a malformed Authorization header cannot mask a valid x-api-key.
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

bool OtlpServer::TryReserveAdmission(idx_t bytes, idx_t &current) {
	current = admitted_bytes.load();
	while (true) {
		if (bytes > config.max_buffered_bytes || current > config.max_buffered_bytes - bytes) {
			return false;
		}
		auto next = current + bytes;
		if (admitted_bytes.compare_exchange_weak(current, next)) {
			return true;
		}
	}
}

void OtlpServer::ReleaseAdmission(idx_t bytes) {
	if (bytes == 0) {
		return;
	}
	auto current = admitted_bytes.load();
	while (true) {
		// An over-release means the accounting is out of balance. Release builds clamp at 0 so the
		// counter can never wrap and wedge admission.
		D_ASSERT(current >= bytes);
		auto next = current > bytes ? current - bytes : 0;
		if (admitted_bytes.compare_exchange_weak(current, next)) {
			return;
		}
	}
}

void OtlpServer::EnsureTargetTables() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	// COPY creates only its partition subdirectories. Remote stores need no directory.
	if (!config.parquet_export_path.empty() && config.parquet_export_path.find("://") == string::npos) {
		std::error_code ec;
		std::filesystem::create_directories(config.parquet_export_path, ec);
	}
	Connection con(*db);
	con.context->config.enable_progress_bar = false;
	for (auto signal_type : OTLP_SIGNALS) {
		CreateOrValidateTable(con, signal_type, OTLP_SIGNAL_TABLES[signal_type]);
	}
}

void OtlpServer::CreateOrValidateTable(Connection &con, OtlpSignalType signal_type, const string &table_name) {
	if (!config.parquet_export_path.empty()) {
		return;
	}
	vector<LogicalType> expected_types;
	vector<string> expected_names;
	GetSignalColumns(*con.context, signal_type, expected_types, expected_names);

	auto qualified = QualifiedTable(config.catalog_name, config.schema_name, table_name);
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
	// The base columns must be a leading prefix (name + type, in order). Trailing extras (promoted
	// columns from an earlier run) are allowed; the sink inserts BY NAME and NULL-fills them.
	auto &result_names = result->GetNames();
	auto &result_types = result->GetTypes();
	if (result_names.size() < expected_names.size()) {
		throw InvalidInputException("Target table %s has %llu columns, expected at least %llu", qualified,
		                            static_cast<uint64_t>(result_names.size()),
		                            static_cast<uint64_t>(expected_names.size()));
	}
	for (idx_t i = 0; i < expected_names.size(); i++) {
		// Compared as strings: Identifier equality is case-insensitive, and a column spelled
		// "Trace_ID" is not the documented schema.
		if (result_names[i].GetIdentifierName() != expected_names[i]) {
			throw InvalidInputException("Target table %s column %llu is %s, expected %s", qualified,
			                            static_cast<uint64_t>(i), result_names[i].GetIdentifierName(),
			                            expected_names[i]);
		}
		if (result_types[i] != expected_types[i]) {
			throw InvalidInputException("Target table %s column %s has type %s, expected %s", qualified,
			                            expected_names[i], result_types[i].ToString(), expected_types[i].ToString());
		}
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

	// Reserve before parse so concurrent bursts cannot all pass the check; released immediately on
	// failure, otherwise only after the rows seal. `body` is already decompressed before this, so
	// peak memory is ~max_buffered_bytes + http_threads * max_body_bytes.
	auto reservation_bytes = MaxValue<idx_t>(body.size(), 1024);
	idx_t admitted_before = 0;
	if (!TryReserveAdmission(reservation_bytes, admitted_before)) {
		throw OtlpHttpError(
		    503, StringUtil::Format("ingest buffer full (%llu admitted bytes, request would add %llu); retry later",
		                            static_cast<uint64_t>(admitted_before), static_cast<uint64_t>(reservation_bytes)));
	}

	OtlpIngestResult result;
	idx_t unclaimed_admission = reservation_bytes;
	try {
		switch (kind) {
		case OtlpRequestKind::LOGS:
			BufferSignal(OTLP_SIGNAL_LOGS, body, format, result, unclaimed_admission);
			break;
		case OtlpRequestKind::TRACES:
			BufferSignal(OTLP_SIGNAL_TRACES, body, format, result, unclaimed_admission);
			break;
		case OtlpRequestKind::METRICS:
			BufferMetrics(body, format, result, unclaimed_admission);
			break;
		default:
			throw InternalException("Unknown OTLP request kind");
		}
	} catch (...) {
		ReleaseAdmission(unclaimed_admission);
		throw;
	}
	ReleaseAdmission(unclaimed_admission);
	total_rows.fetch_add(result.rows);
	return result;
}

OtlpSignalBuffer &OtlpServer::BufferFor(OtlpSignalType signal_type) {
	return *signal_buffers[signal_type];
}

void OtlpServer::BufferSignal(OtlpSignalType signal_type, const string &body, OtlpInputFormat format,
                              OtlpIngestResult &result, idx_t &admission_bytes) {
	auto &buf = BufferFor(signal_type);
	ArrowArray array;
	ArrowSchema schema;
	OtlpStatus status =
	    otlp_transform_with_profile(signal_type, format, OTLP_ARROW_PROFILE_DUCKDB,
	                                reinterpret_cast<const uint8_t *>(body.data()), body.size(), &array, &schema);
	if (status != OTLP_OK) {
		throw OtlpHttpError(
		    400, StringUtil::Format("OTLP parse failed for %s: %s", buf.table_name, otlp_status_message(status)));
	}

	auto release = [&]() {
		if (array.release) {
			array.release(&array);
		}
		if (schema.release) {
			schema.release(&schema);
		}
	};
	vector<StagedSignal> staged;
	try {
		// Stage first, so a conversion failure leaves the live buffer untouched.
		auto stage = StageArrowBatch(signal_type, array, schema);
		if (stage.rows > 0) {
			staged.push_back(std::move(stage));
		}
	} catch (...) {
		release();
		throw;
	}
	release();
	CommitStaged(staged, result, admission_bytes);
}

void OtlpServer::BufferMetrics(const string &body, OtlpInputFormat format, OtlpIngestResult &result,
                               idx_t &admission_bytes) {
	OtlpMetricsArrowBatches batches = {};
	OtlpStatus status = otlp_transform_metrics_all_with_profile(
	    format, OTLP_ARROW_PROFILE_DUCKDB, reinterpret_cast<const uint8_t *>(body.data()), body.size(), &batches);

	if (status != OTLP_OK) {
		ReleaseOtlpMetricBatches(batches);
		throw OtlpHttpError(400, StringUtil::Format("OTLP parse failed for metrics: %s", otlp_status_message(status)));
	}

	// Stage every metric shape before any reaches the live buffers (CommitStaged), so a failure on
	// a later shape cannot make a client retry double-buffer the earlier ones.
	vector<StagedSignal> staged;
	try {
		for (auto signal_type : OTLP_SIGNALS) {
			if (signal_type < OTLP_SIGNAL_METRICS_GAUGE) {
				continue;
			}
			auto &shape = OtlpMetricShape(batches, signal_type);
			if (shape.present) {
				auto stage = StageArrowBatch(signal_type, shape.array, shape.schema);
				if (stage.rows > 0) {
					staged.push_back(std::move(stage));
				}
			}
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
			if (metrics_stage_fault_hook_for_test && signal_type == OTLP_SIGNAL_METRICS_GAUGE) {
				metrics_stage_fault_hook_for_test();
			}
#endif
		}
	} catch (...) {
		ReleaseOtlpMetricBatches(batches);
		throw;
	}
	ReleaseOtlpMetricBatches(batches);

	// The skipped-counters describe what the Rust transform dropped, not what was buffered, so they
	// are safe to advance before the all-or-nothing commit.
	result.skipped_summaries += batches.skipped_summaries;
	result.skipped_nan_values += batches.skipped_nan_values;
	result.skipped_infinity_values += batches.skipped_infinity_values;
	result.skipped_missing_values += batches.skipped_missing_values;

	CommitStaged(staged, result, admission_bytes);
}

OtlpServer::StagedSignal OtlpServer::StageArrowBatch(OtlpSignalType signal_type, ArrowArray &array,
                                                     ArrowSchema &schema) {
	auto &buf = BufferFor(signal_type);
	StagedSignal staged;
	staged.signal_type = signal_type;
	if (array.length <= 0) {
		return staged;
	}
	auto db = db_ptr.lock();
	if (!db) {
		throw IOException("Database was closed");
	}
	auto &table = *buf.arrow_table;
	table.CheckBatchSchema(schema);
	auto &allocator = Allocator::Get(*db);
	staged.collection = make_uniq<ColumnDataCollection>(allocator, buf.types);
	// `imported` may reference the borrowed Arrow buffers (zero-copy); Append copies out of them.
	auto batch = OtlpBorrowArrowBatch(array);
	DataChunk imported;
	imported.Initialize(allocator, table.types, APPEND_CHUNK_SIZE);
	DataChunk chunk;
	chunk.Initialize(allocator, buf.types, APPEND_CHUNK_SIZE);
	for (idx_t offset = 0; offset < static_cast<idx_t>(array.length); offset += APPEND_CHUNK_SIZE) {
		auto count = MinValue<idx_t>(APPEND_CHUNK_SIZE, static_cast<idx_t>(array.length) - offset);
		imported.Reset();
		chunk.Reset();
		OtlpArrowScan(*arrow_con->context, table, batch, buf.identity_column_ids, offset, count, imported);
		for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
			if (imported.data[col].GetType() == chunk.data[col].GetType()) {
				chunk.data[col].Reference(imported.data[col]);
			} else {
				TimestampNsToTimestamp(imported.data[col], chunk.data[col], count);
			}
		}
		chunk.SetChildCardinality(count);
		StageAppend(*staged.collection, chunk);
		staged.rows += count;
		staged.batches++;
	}
	return staged;
}

void OtlpServer::StageAppend(ColumnDataCollection &collection, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	collection.Append(chunk);
}

void OtlpServer::CommitStaged(vector<StagedSignal> &staged, OtlpIngestResult &result, idx_t &admission_bytes) {
	if (staged.empty()) {
		// Nothing buffered: Ingest releases the unclaimed admission.
		return;
	}
	idx_t total_rows = 0;
	for (auto &s : staged) {
		total_rows += s.rows;
	}
	// Split admission across signals by rows, exactly once; the remainder goes to the last signal
	// so the shares sum exactly to admission_bytes.
	idx_t admission_remaining = admission_bytes;
	auto now = std::chrono::steady_clock::now();
	for (idx_t i = 0; i < staged.size(); i++) {
		auto &s = staged[i];
		idx_t share;
		if (i + 1 == staged.size()) {
			share = admission_remaining; // last signal soaks up the rounding remainder
		} else {
			share = (total_rows > 0) ? (admission_bytes * s.rows / total_rows) : 0;
		}
		admission_remaining -= share;
		auto &buf = BufferFor(s.signal_type);
		std::lock_guard<std::mutex> lock(buf.mutex);
		buf.collection->Combine(*s.collection);
		if (!buf.have_unsealed) {
			buf.have_unsealed = true;
			buf.first_unsealed = now;
		}
		buf.buffered_rows += s.rows;
		buf.unsealed_admission_bytes += share;
		result.rows += s.rows;
		result.batches += s.batches;
	}
	admission_bytes = 0; // the reservation is now attributed to the live buffers (per-signal)
	// admitted_bytes includes in-flight reservations, so this may fire slightly early.
	if (admitted_bytes.load() >= config.seal_target_bytes) {
		RequestSeal();
	}
}

void OtlpServer::RequestSeal() {
	// Set the flag under sealer_mutex so it cannot land between the sealer's predicate check and
	// its wait (a lost wakeup). notify_one may stay outside the lock.
	{
		std::lock_guard<std::mutex> lock(sealer_mutex);
		flush_requested.store(true);
	}
	sealer_cv.notify_one();
}

static int64_t NowUnixMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

static int64_t AgeMsSince(int64_t last_unix_ms) {
	if (last_unix_ms == 0) {
		return -1;
	}
	auto now = NowUnixMs();
	return now >= last_unix_ms ? now - last_unix_ms : 0;
}

OtlpServer::SealingPlan OtlpServer::SwapBuffersForSeal(Allocator &allocator) {
	// Allocate outside the lock so the critical section is only swaps and counter resets.
	std::vector<unique_ptr<ColumnDataCollection>> fresh;
	fresh.reserve(signal_buffers.size());
	for (auto &buf : signal_buffers) {
		fresh.push_back(make_uniq<ColumnDataCollection>(allocator, buf->types));
	}

	SealingPlan plan;
	plan.signals.reserve(signal_buffers.size());
	{
		std::vector<std::unique_lock<std::mutex>> locks;
		locks.reserve(signal_buffers.size());
		for (auto &buf : signal_buffers) {
			locks.emplace_back(buf->mutex);
		}
		for (idx_t i = 0; i < signal_buffers.size(); i++) {
			auto &buf = *signal_buffers[i];
			SealingSignal state;
			state.collection = std::move(buf.collection);
			state.rows = buf.buffered_rows;
			state.admission_bytes = buf.unsealed_admission_bytes;
			plan.sealed_admission_bytes += buf.unsealed_admission_bytes;
			// Only read on restore when rows > 0.
			state.first_unsealed = buf.first_unsealed;
			plan.total_rows += state.rows;

			buf.collection = std::move(fresh[i]);
			buf.buffered_rows = 0;
			buf.unsealed_admission_bytes = 0;
			buf.have_unsealed = false;
			plan.signals.push_back(std::move(state));
		}
	}
	return plan;
}

idx_t OtlpServer::RestoreUnsealed(SealingPlan &plan, const std::vector<bool> &restore) {
	idx_t restored_rows = 0;
	std::vector<std::unique_lock<std::mutex>> locks;
	locks.reserve(signal_buffers.size());
	for (auto &buf : signal_buffers) {
		locks.emplace_back(buf->mutex);
	}
	for (idx_t i = 0; i < signal_buffers.size(); i++) {
		auto &state = plan.signals[i];
		if (state.rows == 0 || !restore[i]) {
			continue;
		}
		auto &buf = *signal_buffers[i];
		state.collection->Combine(*buf.collection); // old rows, then live rows
		buf.collection = std::move(state.collection);
		buf.buffered_rows += state.rows;
		buf.unsealed_admission_bytes += state.admission_bytes;
		// Keep the restored rows' true age: the older of their original time and the live rows' time.
		if (buf.have_unsealed) {
			buf.first_unsealed = std::min(state.first_unsealed, buf.first_unsealed);
		} else {
			buf.first_unsealed = state.first_unsealed;
		}
		buf.have_unsealed = true;
		restored_rows += state.rows;
	}
	return restored_rows;
}

void OtlpServer::RecordSealOutcome(bool success, idx_t rows_committed, idx_t admitted_bytes_committed,
                                   int64_t seal_started_unix_ms, int64_t append_duration_ms, int64_t commit_duration_ms,
                                   const string &error) {
	// Increment the counters BEFORE RecordSealEvent snapshots them (see OtlpSealEvent).
	if (success) {
		seals_total.fetch_add(1);
		committed_rows_total.fetch_add(rows_committed);
		last_seal_unix_ms.store(NowUnixMs());
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			seal_last_error.clear();
		}
	} else {
		seal_failures_total.fetch_add(1);
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			seal_last_error = error;
		}
	}
	RecordSealEvent(seal_started_unix_ms, append_duration_ms, commit_duration_ms, rows_committed,
	                admitted_bytes_committed, success, error);
}

//! The relation name a sink statement reads the sealed rows from.
static constexpr const char *SEALED_RELATION = "sealed";

//! Run `sql` with `collection` bound as the relation SEALED_RELATION.
//! Injected as a non-materialized CTE over a ColumnDataRef (as DuckDB's Appender flushes), so
//! there is no temp table and no per-row copy. `names` lets a sink address columns by name.
static void ExecuteOverSealed(Connection &connection, const string &sql, ColumnDataCollection &collection,
                              const vector<Identifier> &names) {
	Parser parser(*connection.context);
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		throw InternalException("OTLP sink must be exactly one statement: %s", sql);
	}
	auto &statement = *parser.statements[0];
	optional_ptr<CommonTableExpressionMap> cte_map;
	switch (statement.type) {
	case StatementType::INSERT_STATEMENT:
		cte_map = &statement.Cast<InsertStatement>().node->cte_map;
		break;
	case StatementType::COPY_STATEMENT: {
		auto &info = *statement.Cast<CopyStatement>().info;
		if (!info.select_statement) {
			throw InternalException("OTLP sink COPY must read from a query: %s", sql);
		}
		cte_map = &info.select_statement->cte_map;
		break;
	}
	default:
		throw InternalException("OTLP sink must be an INSERT or a COPY: %s", sql);
	}

	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());
	select->from_table = BaseAppender::GetColumnDataTableRef(collection, Identifier(SEALED_RELATION), names);
	auto cte = make_uniq<CommonTableExpressionInfo>();
	cte->query_node = std::move(select);
	cte->materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
	cte_map->map.insert(Identifier(SEALED_RELATION), std::move(cte));

	auto result = connection.Query(std::move(parser.statements[0]));
	if (!result || result->HasError()) {
		throw IOException("%s", result ? result->GetError() : string("DuckDB query failed"));
	}
}

//! Appends to `<root>/<table>/year=/month=/day=/`.
static string ParquetSinkSql(const string &table_name, const string &root) {
	auto ts = QuoteIdentifier(PartitionTimestampColumn(table_name));
	return StringUtil::Format("COPY (SELECT *, strftime(%s, '%%Y') AS year, strftime(%s, '%%m') AS month, "
	                          "strftime(%s, '%%d') AS day FROM %s) TO %s (FORMAT PARQUET, COMPRESSION ZSTD, "
	                          "PARTITION_BY (year, month, day), APPEND, FILENAME_PATTERN 'seal_{uuidv7}', "
	                          "WRITE_PARTITION_COLUMNS false)",
	                          ts, ts, ts, SEALED_RELATION, SqlQuote(ParquetDatasetDirectory(root, table_name)));
}

void OtlpServer::BuildSinks() {
	sinks.clear();
	const bool parquet = !config.parquet_export_path.empty();
	const string promote = promoter && promoter->Enabled() ? promoter->ProjectionSuffix() : string();
	for (auto &buf : signal_buffers) {
		if (parquet) {
			sinks.push_back(ParquetSinkSql(buf->table_name, config.parquet_export_path));
			continue;
		}
		// BY NAME: table columns beyond the buffer's (earlier promotions, operator additions) are NULL-filled.
		sinks.push_back("INSERT INTO " + QualifiedTable(config.catalog_name, config.schema_name, buf->table_name) +
		                " BY NAME SELECT *" + promote + " FROM " + SEALED_RELATION);
	}
}

OtlpIngestResult OtlpServer::SealOnce() {
	std::lock_guard<std::mutex> writer_lock(writer_mutex);
	if (!writer_con) {
		return OtlpIngestResult {};
	}
	auto db = db_ptr.lock();
	if (!db) {
		return OtlpIngestResult {}; // database closed; nothing we can durably write
	}
	auto &allocator = Allocator::Get(*db);
	auto seal_started_unix_ms = NowUnixMs();

	auto plan = SwapBuffersForSeal(allocator);
	if (plan.total_rows == 0) {
		ReleaseAdmission(plan.sealed_admission_bytes);
		last_seal_unix_ms.store(NowUnixMs());
		return OtlpIngestResult {};
	}

	// A catalog seal is one transaction: a failure rolls back and re-buffers everything. A Parquet
	// COPY cannot be rolled back, so export is at-least-once: a failure re-buffers only signals not
	// yet exported, and a COPY that throws part-way may leave duplicates for readers to tolerate.
	const bool transactional = config.parquet_export_path.empty();
	auto &sealing = plan.signals;
	std::vector<bool> sealed(signal_buffers.size(), false);
	OtlpIngestResult result;
	int64_t append_duration_ms = 0;
	int64_t commit_duration_ms = 0;
	idx_t failed_signal = 0;
	try {
		if (transactional) {
			RunSQL(*writer_con, "BEGIN TRANSACTION");
		}
		auto append_started = std::chrono::steady_clock::now();
		for (idx_t i = 0; i < signal_buffers.size(); i++) {
			failed_signal = i;
			if (sealing[i].rows == 0) {
				continue;
			}
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
			if (!transactional && parquet_seal_fault_hook_for_test) {
				parquet_seal_fault_hook_for_test(i);
			}
#endif
			auto &buf = *signal_buffers[i];
			ExecuteOverSealed(*writer_con, sinks[i], *sealing[i].collection, buf.names);
			sealed[i] = true;
			result.rows += sealing[i].rows;
			result.batches += sealing[i].collection->ChunkCount();
			if (!transactional && config.parquet_views) {
				// The export is durable; the view is best-effort and never fails it.
				EnsureParquetView(i, buf.table_name);
			}
		}
		append_duration_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - append_started)
		        .count();
		if (transactional) {
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
			if (seal_fault_hook_for_test) {
				seal_fault_hook_for_test();
			}
#endif
			auto commit_started = std::chrono::steady_clock::now();
			RunSQL(*writer_con, "COMMIT");
			commit_duration_ms =
			    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - commit_started)
			        .count();
		}
	} catch (...) {
		// catch(...) so a non-std throw cannot std::terminate the host and the buffers are restored.
		string msg;
		try {
			throw;
		} catch (std::exception &ex) {
			msg = ex.what();
		} catch (...) {
			msg = "unknown (non-std) exception during seal";
		}
		if (transactional) {
			std::fill(sealed.begin(), sealed.end(), false);
			bool rollback_ok = true;
			try {
				RunSQL(*writer_con, "ROLLBACK");
			} catch (...) {
				rollback_ok = false;
			}
			if (!rollback_ok) {
				// The connection may be wedged in an aborted transaction; rebuild it so BEGIN can succeed.
				try {
					writer_con = make_uniq<Connection>(*db);
					writer_con->context->config.enable_progress_bar = false;
				} catch (...) {
				}
			}
		}
		// Each restored signal puts back exactly its own admission bytes and original age, so repeated
		// partial failures cannot drift the budget.
		std::vector<bool> restore(signal_buffers.size());
		idx_t sealed_rows = 0;
		idx_t sealed_admission = plan.sealed_admission_bytes;
		for (idx_t i = 0; i < signal_buffers.size(); i++) {
			restore[i] = !sealed[i];
			if (restore[i]) {
				sealed_admission -= sealing[i].admission_bytes;
			} else {
				sealed_rows += sealing[i].rows;
			}
		}
		RestoreUnsealed(plan, restore);
		ReleaseAdmission(sealed_admission);
		if (sealed_rows > 0) {
			// A partial Parquet export is durable: it advances the seal clock and committed_rows_total
			// (hence success=false with rows_committed > 0). Bumped before RecordSealOutcome snapshots it.
			last_seal_unix_ms.store(NowUnixMs());
			committed_rows_total.fetch_add(sealed_rows);
		}
		LogServerEvent(StringUtil::Format("seal failed at %s (%llu/%llu rows sealed): %s",
		                                  signal_buffers[failed_signal]->table_name, static_cast<uint64_t>(sealed_rows),
		                                  static_cast<uint64_t>(plan.total_rows), msg),
		               LogLevel::LOG_WARNING);
		RecordSealOutcome(false, sealed_rows, sealed_admission, seal_started_unix_ms, append_duration_ms,
		                  commit_duration_ms, msg);
		throw IOException("seal failed: %s", msg);
	}

	ReleaseAdmission(plan.sealed_admission_bytes);
	// DEBUG: otlp_seal_list() keeps the history.
	LogServerEvent(StringUtil::Format("seal: target=%s rows=%llu batches=%llu", SealTargetDescription(),
	                                  static_cast<uint64_t>(result.rows), static_cast<uint64_t>(result.batches)),
	               LogLevel::LOG_DEBUG);
	RecordSealOutcome(true, result.rows, plan.sealed_admission_bytes, seal_started_unix_ms, append_duration_ms,
	                  commit_duration_ms, "");
	catalog_maintenance_due = true;
	return result;
}

string OtlpServer::SealTargetDescription() const {
	if (!config.parquet_export_path.empty()) {
		return config.parquet_export_path;
	}
	return config.catalog_name.empty() ? config.schema_name : config.catalog_name + "." + config.schema_name;
}

void OtlpServer::EnsureParquetView(idx_t signal_index, const string &table_name) {
	// Created after files exist so the view binds a schema. Binding reads every file's footer, and
	// DuckDB binds a CREATE VIEW before checking IF NOT EXISTS, so the catalog is checked first and
	// each signal is settled once: a failure is logged, never retried against a growing archive.
	if (parquet_view_settled[signal_index]) {
		return;
	}
	parquet_view_settled[signal_index] = true;
	// information_schema.tables lists views too (the IF NOT EXISTS namespace) and reads only the catalog.
	auto catalog = config.catalog_name.empty() ? string("current_database()") : SqlQuote(config.catalog_name);
	auto existing = writer_con->Query("SELECT 1 FROM information_schema.tables WHERE table_catalog = " + catalog +
	                                  " AND table_schema = " + SqlQuote(config.schema_name) +
	                                  " AND table_name = " + SqlQuote(table_name));
	if (existing && !existing->HasError() && existing->RowCount() > 0) {
		return;
	}
	auto view = QualifiedTable(config.catalog_name, config.schema_name, table_name);
	try {
		RunSQL(*writer_con, "CREATE VIEW IF NOT EXISTS " + view + " AS " +
		                        ParquetDatasetSelect(config.parquet_export_path, table_name));
	} catch (std::exception &ex) {
		LogServerEvent(StringUtil::Format("parquet inspection view %s not created: %s", view, ex.what()),
		               LogLevel::LOG_WARNING);
	}
}

bool OtlpServer::CatalogMaintenanceEnabled() const {
	return !config.catalog_name.empty() && config.parquet_export_path.empty() &&
	       catalog_maintenance_state != CatalogMaintenanceState::DISABLED && writer_con;
}

void OtlpServer::MaybeRunCatalogMaintenance() {
	// Checked before taking writer_mutex: the target is fixed and only this thread touches the time.
	auto now = std::chrono::steady_clock::now();
	if (config.catalog_name.empty() || !config.parquet_export_path.empty() ||
	    now - catalog_maintenance_last_attempt < std::chrono::milliseconds(config.maintenance_interval_ms)) {
		return;
	}
	std::lock_guard<std::mutex> writer_lock(writer_mutex);
	if (!catalog_maintenance_due || !CatalogMaintenanceEnabled()) {
		return;
	}
	// Reset before the attempt, so a contended or failed pass waits a full interval too.
	catalog_maintenance_due = false;
	catalog_maintenance_last_attempt = now;
	try {
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
		if (maintenance_fault_hook_for_test) {
			maintenance_fault_hook_for_test();
		}
#endif
		RunSQL(*writer_con, "CHECKPOINT " + QuoteIdentifier(config.catalog_name));
		catalog_maintenance_state = CatalogMaintenanceState::SUPPORTED;
		maintenance_runs_total.fetch_add(1);
		last_maintenance_unix_ms.store(NowUnixMs());
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			maintenance_last_error.clear();
		}
		LogServerEvent(StringUtil::Format("catalog maintenance checkpoint succeeded: catalog=%s", config.catalog_name));
		SweepOrphanedFiles();
	} catch (...) {
		string msg;
		try {
			throw;
		} catch (std::exception &ex) {
			msg = ex.what();
		} catch (...) {
			msg = "unknown (non-std) exception during catalog maintenance checkpoint";
		}
		if (IsContendedCatalogMaintenanceError(msg)) {
			maintenance_contended_total.fetch_add(1);
			LogServerEvent(StringUtil::Format("catalog maintenance checkpoint skipped: another writer checkpointed "
			                                  "catalog=%s first: %s",
			                                  config.catalog_name, msg));
			return;
		}
		maintenance_failures_total.fetch_add(1);
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			maintenance_last_error = msg;
		}
		if (IsUnsupportedCatalogMaintenanceError(msg)) {
			catalog_maintenance_state = CatalogMaintenanceState::DISABLED;
			LogServerEvent(
			    StringUtil::Format("catalog maintenance checkpoint unsupported for catalog=%s; disabling automatic "
			                       "catalog maintenance for this server: %s",
			                       config.catalog_name, msg));
		} else {
			LogServerEvent(StringUtil::Format("catalog maintenance checkpoint failed: catalog=%s error=%s",
			                                  config.catalog_name, msg),
			               LogLevel::LOG_WARNING);
		}
	}
}

void OtlpServer::SweepOrphanedFiles() {
	if (orphan_sweep_state == CatalogMaintenanceState::DISABLED) {
		return;
	}
	// Only after a successful CHECKPOINT; a sweep error is logged, never counted as a failure.
	try {
		if (orphan_sweep_state == CatalogMaintenanceState::PENDING) {
			// Other catalogs that accept CHECKPOINT (e.g. a plain DuckDB file) never sweep.
			auto result = writer_con->Query("SELECT type FROM duckdb_databases() WHERE database_name = " +
			                                SqlQuote(config.catalog_name));
			if (!result || result->HasError()) {
				throw IOException("%s", result ? result->GetError() : string("catalog type lookup failed"));
			}
			auto chunk = result->Fetch();
			bool is_ducklake =
			    chunk && chunk->size() > 0 && StringUtil::Lower(chunk->GetValue(0, 0).ToString()) == "ducklake";
			orphan_sweep_state = is_ducklake ? CatalogMaintenanceState::SUPPORTED : CatalogMaintenanceState::DISABLED;
			if (!is_ducklake) {
				return;
			}
		}
		// Same retention window as expire_older_than/delete_older_than, so an in-flight seal or
		// checkpoint's Parquet (here or in another process) is never a candidate.
		auto retention_s = std::max<int64_t>(1, config.maintenance_retention_ms / 1000);
		auto result = writer_con->Query(StringUtil::Format(
		    "SELECT count(*) FROM ducklake_delete_orphaned_files(%s, older_than => now() - INTERVAL %lld SECOND)",
		    SqlQuote(config.catalog_name), static_cast<int64_t>(retention_s)));
		if (!result || result->HasError()) {
			throw IOException("%s", result ? result->GetError() : string("DuckDB query failed"));
		}
		auto chunk = result->Fetch();
		auto deleted = chunk && chunk->size() > 0 ? chunk->GetValue(0, 0).GetValue<int64_t>() : 0;
		if (deleted > 0) {
			LogServerEvent(StringUtil::Format("catalog maintenance deleted %lld orphaned data files: catalog=%s",
			                                  static_cast<int64_t>(deleted), config.catalog_name));
		}
	} catch (std::exception &ex) {
		auto msg = string(ex.what());
		if (IsContendedCatalogMaintenanceError(msg)) {
			LogServerEvent(StringUtil::Format(
			    "catalog maintenance orphan sweep skipped: another writer is maintaining catalog=%s: %s",
			    config.catalog_name, msg));
			return;
		}
		LogServerEvent(StringUtil::Format("catalog maintenance orphan sweep failed: catalog=%s error=%s",
		                                  config.catalog_name, msg),
		               LogLevel::LOG_WARNING);
	}
}

void OtlpServer::ConfigureCatalogMaintenanceOptions() {
	// target_file_size bounds merge output (at-target files are left alone, so O(new) per cycle).
	if (config.catalog_name.empty() || !writer_con) {
		return;
	}
	auto target_mb = std::max<idx_t>(1, config.target_file_size / (1024ULL * 1024ULL));
	auto retention_s = std::max<int64_t>(1, config.maintenance_retention_ms / 1000);
	auto quoted = QuoteIdentifier(config.catalog_name);
	try {
		RunSQL(*writer_con, StringUtil::Format("CALL %s.set_option('target_file_size', '%lluMB')", quoted,
		                                       static_cast<uint64_t>(target_mb)));
		RunSQL(*writer_con, StringUtil::Format("CALL %s.set_option('expire_older_than', '%llds')", quoted,
		                                       static_cast<long long>(retention_s)));
		RunSQL(*writer_con, StringUtil::Format("CALL %s.set_option('delete_older_than', '%llds')", quoted,
		                                       static_cast<long long>(retention_s)));
		LogServerEvent(StringUtil::Format(
		    "catalog maintenance options set: catalog=%s target_file_size=%lluMB retention=%llds", config.catalog_name,
		    static_cast<uint64_t>(target_mb), static_cast<long long>(retention_s)));
	} catch (...) {
		// Not DuckLake: keep defaults; maintenance DISABLEs itself if CHECKPOINT fails. catch(...) so a
		// non-std throw cannot escape this startup path.
		string msg;
		try {
			throw;
		} catch (std::exception &ex) {
			msg = ex.what();
		} catch (...) {
			msg = "unknown (non-std) exception during catalog maintenance option setup";
		}
		LogServerEvent(
		    StringUtil::Format("catalog maintenance options not applied for catalog=%s: %s", config.catalog_name, msg));
	}
}

void OtlpServer::SealerLoop() {
	// Poll at most every seal_max_age_ms (floored) so a sub-second max_age is honored.
	auto poll_ms = std::max<int64_t>(50, std::min<int64_t>(config.seal_max_age_ms, 1000));
	while (!sealer_stop.load()) {
		{
			std::unique_lock<std::mutex> lock(sealer_mutex);
			sealer_cv.wait_for(lock, std::chrono::milliseconds(poll_ms),
			                   [this] { return sealer_stop.load() || flush_requested.load(); });
		}
		if (sealer_stop.load()) {
			break;
		}
		bool due = flush_requested.exchange(false);
		if (!due) {
			due = SealAgeDue();
		}
		if (due) {
			try {
				SealOnce();
			} catch (...) {
				// SealOnce already logged and restored the buffers; back off.
				std::this_thread::sleep_for(std::chrono::milliseconds(250));
			}
		}
		try {
			MaybeRunCatalogMaintenance();
		} catch (...) {
		}
	}
}

bool OtlpServer::SealAgeDue() const {
	auto now = std::chrono::steady_clock::now();
	bool have_oldest = false;
	std::chrono::steady_clock::time_point oldest;
	for (auto &buf : signal_buffers) {
		std::lock_guard<std::mutex> lock(buf->mutex);
		if (!buf->have_unsealed) {
			continue;
		}
		if (!have_oldest || buf->first_unsealed < oldest) {
			oldest = buf->first_unsealed;
			have_oldest = true;
		}
	}
	if (!have_oldest) {
		return false;
	}
	auto age = std::chrono::duration_cast<std::chrono::milliseconds>(now - oldest).count();
	return age >= config.seal_max_age_ms;
}

OtlpIngestResult OtlpServer::FlushNow() {
	return SealOnce();
}

idx_t OtlpServer::ShutdownIngest() {
	if (ingest_shutdown_done.exchange(true)) {
		return 0; // idempotent: Close() and ~OtlpServer() may both call this; nothing left to drain
	}
	sealer_stop.store(true);
	sealer_cv.notify_all();
	if (sealer_thread.joinable()) {
		sealer_thread.join();
	}
	// Final drain. On the implicit DB-teardown path (~OtlpStorageExtensionInfo) db_ptr is already
	// expired, so SealOnce is a no-op and buffered rows are dropped: callers must otlp_stop first.
	for (int attempt = 0; attempt < 3 && BufferedRows() > 0; attempt++) {
		try {
			SealOnce();
		} catch (...) {
			LogServerEvent("final seal attempt failed during shutdown", LogLevel::LOG_WARNING);
		}
	}
	auto remaining_rows = BufferedRows();
	if (remaining_rows > 0) {
		shutdown_dropped_rows.store(remaining_rows);
		// A live database means the final seals failed; an expired one means the teardown path.
		string cause;
		if (db_ptr.lock()) {
			auto last_error = SealLastError();
			cause = last_error.empty() ? "repeated seal failure" : "repeated seal failure: " + last_error;
		} else {
			cause = "database closed without graceful otlp_stop";
		}
		LogServerEvent(StringUtil::Format("dropping %llu buffered rows on shutdown (%s)",
		                                  static_cast<uint64_t>(remaining_rows), cause),
		               LogLevel::LOG_WARNING);
	}
	{
		std::lock_guard<std::mutex> writer_lock(writer_mutex);
		writer_con.reset();
		arrow_con.reset();
	}
	return remaining_rows;
}

} // namespace duckdb
