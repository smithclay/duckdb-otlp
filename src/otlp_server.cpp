#include "otlp_server.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/uhugeint.hpp"
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
#include "otlp_sql_util.hpp"

#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>

namespace duckdb {

//! Thrown while staging a request the server refuses for a known reason (a payload that does not
//! parse). OtlpServer::Admit turns it into an OtlpIngestOutcome; it never reaches a transport.
class OtlpIngestRefused : public std::exception {
public:
	OtlpIngestRefused(OtlpIngestRefusal refusal_p, string message_p)
	    : refusal(refusal_p), message(std::move(message_p)) {
	}

	const char *what() const noexcept override {
		return message.c_str();
	}

	OtlpIngestRefusal refusal;
	string message;
};

//! The schema, importer and sink for one OTLP signal table.
struct OtlpSignal {
	const string table_name;
	const vector<LogicalType> types;
	//! Column names parallel to `types`: what the sink statement addresses the sealed rows by.
	const vector<Identifier> names;
	//! Identity projection (0..n_columns-1), precomputed for the per-chunk append path.
	const vector<column_t> identity_column_ids;
	//! How a decoded batch of this signal is imported. Its types are the importer's; `types` above
	//! differ only where the buffer narrows them (TIMESTAMP_NS -> TIMESTAMP, see BufferColumnType).
	const unique_ptr<OtlpArrowTable> arrow_table;
	string sink_sql;                   // set once by BuildSinks before ingest starts
	bool parquet_view_settled = false; // under writer_mutex

	OtlpSignal(string table_name_p, vector<LogicalType> types_p, vector<Identifier> names_p,
	           unique_ptr<OtlpArrowTable> arrow_table_p)
	    : table_name(std::move(table_name_p)), types(std::move(types_p)), names(std::move(names_p)),
	      identity_column_ids(MakeIdentityColumnIds(types.size())), arrow_table(std::move(arrow_table_p)) {
	}

private:
	static vector<column_t> MakeIdentityColumnIds(idx_t count) {
		vector<column_t> ids;
		ids.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			ids.push_back(i);
		}
		return ids;
	}
};

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

//! The input format for a Content-Type, or false when the server does not decode it. `media_type`
//! receives the normalized type either way, for the refusal message.
static bool FormatFromContentType(const string &content_type, OtlpInputFormat &format, string &media_type) {
	media_type = content_type;
	auto semicolon = media_type.find(';');
	if (semicolon != string::npos) {
		media_type = media_type.substr(0, semicolon);
	}
	StringUtil::Trim(media_type);
	media_type = StringUtil::Lower(media_type);
	if (media_type == "application/json" || media_type == "application/otlp+json") {
		format = OTLP_FORMAT_JSON;
		return true;
	}
	if (media_type == "application/x-ndjson" || media_type == "application/jsonl") {
		format = OTLP_FORMAT_JSONL;
		return true;
	}
	if (media_type == "application/x-protobuf" || media_type == "application/protobuf" ||
	    media_type == "application/otlp") {
		format = OTLP_FORMAT_PROTOBUF;
		return true;
	}
	return false;
}

static OtlpIngestOutcome Refused(OtlpIngestRefusal refusal, string message) {
	OtlpIngestOutcome outcome;
	outcome.refusal = refusal;
	outcome.message = std::move(message);
	return outcome;
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
	for (auto signal_type : OTLP_SIGNALS) {
		auto arrow_table = OtlpArrowTable::ForSignal(*arrow_con->context, signal_type);
		vector<LogicalType> types;
		for (auto &type : arrow_table->types) {
			types.push_back(BufferColumnType(type));
		}
		vector<Identifier> identifiers(arrow_table->names.begin(), arrow_table->names.end());
		signals.push_back(make_uniq<OtlpSignal>(OTLP_SIGNAL_TABLES[signal_type], std::move(types),
		                                        std::move(identifiers), std::move(arrow_table)));
	}
	live = MakeEmptyBuffers(Allocator::Get(*db));
}

OtlpServer::OtlpBuffers OtlpServer::MakeEmptyBuffers(Allocator &allocator) const {
	OtlpBuffers buffers;
	buffers.rows.reserve(signals.size());
	for (auto &signal : signals) {
		buffers.rows.push_back(make_uniq<ColumnDataCollection>(allocator, signal->types));
	}
	return buffers;
}

int64_t OtlpServer::LastSealAgeMs() const {
	return AgeMsSince(last_seal_unix_ms.load());
}

int64_t OtlpServer::LastMaintenanceAgeMs() const {
	return AgeMsSince(last_maintenance_unix_ms.load());
}

idx_t OtlpServer::OtlpBuffers::Rows() const {
	idx_t count = 0;
	for (auto &collection : rows) {
		count += collection->Count();
	}
	return count;
}

idx_t OtlpServer::BufferedRows() const {
	std::lock_guard<std::mutex> lock(buffer_mutex);
	return live.Rows();
}

idx_t OtlpServer::BufferedBytes() const {
	std::lock_guard<std::mutex> lock(buffer_mutex);
	idx_t bytes = 0;
	for (auto &collection : live.rows) {
		bytes += collection->SizeInBytes();
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
	std::lock_guard<std::mutex> lock(buffer_mutex);
	if (live.Rows() == 0) {
		return -1;
	}
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - live.oldest)
	    .count();
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
    : db_ptr(context.db), config(config_p),
      maintains_catalog(!config.catalog_name.empty() && config.parquet_export_path.empty()) {
	if (!config.disable_auth) {
		ValidateToken(config.token);
	}
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	arrow_con = make_uniq<Connection>(*db);
	InitBuffers();
	// The sealer thread is the only thing that commits, so concurrent DuckLake writes never conflict.
	writer_con = make_uniq<Connection>(*db);
	writer_con->context->config.enable_progress_bar = false;
	EnsureTargetTables();
	if (maintains_catalog) {
		try {
			auto result = RunSQL(*writer_con, "SELECT 1 FROM duckdb_databases() WHERE type = 'ducklake' AND "
			                                  "database_name COLLATE NOCASE = " +
			                                      SqlQuote(config.catalog_name));
			is_ducklake = result->RowCount() > 0;
		} catch (...) {
			LogServerEvent(StringUtil::Format("catalog type lookup failed for catalog=%s: %s", config.catalog_name,
			                                  CurrentExceptionMessage("catalog type lookup")),
			               LogLevel::LOG_WARNING);
		}
	}
	// Parquet-export mode has no table to ALTER.
	if (config.parquet_export_path.empty()) {
		string failure;
		promote_projection = OtlpPromoteColumns(*writer_con, config.promote, config.catalog_name, config.schema_name,
		                                        promoted_columns, failure);
		if (!failure.empty()) {
			LogServerEvent("attribute promotion disabled: " + failure);
		} else if (promoted_columns > 0) {
			LogServerEvent(StringUtil::Format("attribute promotion enabled: %llu column(s) per signal",
			                                  static_cast<uint64_t>(promoted_columns)));
		}
	}
	BuildSinks();
	sealer_thread = std::thread([this] { SealerLoop(); });

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
	if (!config.parquet_export_path.empty()) {
		// COPY creates only its partition subdirectories. Remote stores need no directory.
		if (config.parquet_export_path.find("://") == string::npos) {
			std::error_code ec;
			std::filesystem::create_directories(config.parquet_export_path, ec);
		}
		return;
	}
	for (auto &signal : signals) {
		CreateOrValidateTable(*signal);
	}
}

void OtlpServer::CreateOrValidateTable(const OtlpSignal &signal) {
	const auto &expected_types = signal.types;
	const auto &expected_names = signal.arrow_table->names;
	auto qualified = QualifiedTable(config.catalog_name, config.schema_name, signal.table_name);
	if (config.create_tables) {
		string sql = "CREATE TABLE IF NOT EXISTS " + qualified + " (";
		for (idx_t i = 0; i < expected_names.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			sql += QuoteIdentifier(expected_names[i]) + " " + expected_types[i].ToString();
		}
		sql += ")";
		RunSQL(*writer_con, sql);
	}

	auto result = RunSQL(*writer_con, "SELECT * FROM " + qualified + " LIMIT 0",
	                     "Target table " + qualified + " is not available: ");
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

OtlpServer::RequestGuard::RequestGuard(OtlpServer &server_p) : server(server_p) {
	server.active_requests++;
	server.total_requests++;
}

OtlpServer::RequestGuard::~RequestGuard() {
	server.active_requests--;
}

OtlpIngestOutcome OtlpServer::Ingest(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
                                     const string &body) {
	if (body.size() > config.max_body_bytes) {
		return Refused(OtlpIngestRefusal::PAYLOAD_TOO_LARGE,
		               StringUtil::Format("payload has %llu bytes; max is %llu", static_cast<uint64_t>(body.size()),
		                                  static_cast<uint64_t>(config.max_body_bytes)));
	}
	if (IsUnsupportedEncoding(content_encoding)) {
		return Refused(OtlpIngestRefusal::UNSUPPORTED_MEDIA,
		               StringUtil::Format("unsupported content-encoding %s", content_encoding));
	}
	OtlpInputFormat format;
	string media_type;
	if (!FormatFromContentType(content_type, format, media_type)) {
		return Refused(
		    OtlpIngestRefusal::UNSUPPORTED_MEDIA,
		    StringUtil::Format("unsupported content-type %s", media_type.empty() ? "<missing>" : media_type));
	}
	// `body` is already decompressed, so peak memory is ~max_buffered_bytes + http_threads * max_body_bytes.
	return Admit(body.size(), [&](OtlpIngestResult &counts) -> vector<StagedSignal> {
		switch (kind) {
		case OtlpRequestKind::LOGS:
			return StageSignal(OTLP_SIGNAL_LOGS, body, format);
		case OtlpRequestKind::TRACES:
			return StageSignal(OTLP_SIGNAL_TRACES, body, format);
		case OtlpRequestKind::METRICS:
			return StageMetrics(body, format, counts);
		default:
			throw InternalException("Unknown OTLP request kind");
		}
	});
}

OtlpIngestOutcome OtlpServer::IngestArrow(OtlpSignalType signal_type, idx_t input_bytes, ArrowArray &array,
                                          ArrowSchema &schema) {
	return Admit(input_bytes, [&](OtlpIngestResult &) {
		// array/schema are borrowed from Rust: StageArrowBatch copies the rows and never releases them.
		vector<StagedSignal> staged;
		StageArrowBatch(signal_type, array, schema, staged);
		return staged;
	});
}

OtlpIngestOutcome OtlpServer::Admit(idx_t input_bytes, const StageFn &stage) {
	// Reserve before staging so concurrent bursts cannot all pass the check; released immediately on
	// a refusal, otherwise only after the rows seal.
	auto reservation_bytes = MaxValue<idx_t>(input_bytes, 1024);
	idx_t admitted_before = 0;
	if (!TryReserveAdmission(reservation_bytes, admitted_before)) {
		return Refused(
		    OtlpIngestRefusal::BUFFER_FULL,
		    StringUtil::Format("ingest buffer full (%llu admitted bytes, request would add %llu); retry later",
		                       static_cast<uint64_t>(admitted_before), static_cast<uint64_t>(reservation_bytes)));
	}

	OtlpIngestOutcome outcome;
	idx_t unclaimed_admission = reservation_bytes;
	try {
		auto staged = stage(outcome.counts);
		CommitStaged(staged, outcome.counts, unclaimed_admission);
	} catch (...) {
		ReleaseAdmission(unclaimed_admission);
		return RefusalFromException();
	}
	ReleaseAdmission(unclaimed_admission);
	total_rows.fetch_add(outcome.counts.rows);
	return outcome;
}

OtlpIngestOutcome OtlpServer::RefusalFromException() const {
	try {
		throw;
	} catch (OtlpIngestRefused &ex) {
		return Refused(ex.refusal, ex.message);
	} catch (InvalidInputException &) {
		return Refused(OtlpIngestRefusal::MALFORMED, CurrentExceptionMessage());
	} catch (IOException &) {
		auto message = CurrentExceptionMessage();
		LogServerEvent("ingest I/O error: " + message, LogLevel::LOG_WARNING);
		return Refused(OtlpIngestRefusal::INTERNAL, std::move(message));
	} catch (...) {
		// catch(...) so a non-std throw cannot cross into a transport (or into Rust, for gRPC).
		auto message = CurrentExceptionMessage("ingest");
		LogServerEvent("ingest internal error: " + message, LogLevel::LOG_WARNING);
		return Refused(OtlpIngestRefusal::INTERNAL, std::move(message));
	}
}

vector<OtlpServer::StagedSignal> OtlpServer::StageSignal(OtlpSignalType signal_type, const string &body,
                                                         OtlpInputFormat format) {
	const auto &signal = *signals[signal_type];
	ArrowSchemaWrapper schema;
	ArrowArrayWrapper array;
	OtlpStatus status = otlp_transform_with_profile(signal_type, format, OTLP_ARROW_PROFILE_DUCKDB,
	                                                reinterpret_cast<const uint8_t *>(body.data()), body.size(),
	                                                &array.arrow_array, &schema.arrow_schema);
	if (status != OTLP_OK) {
		throw OtlpIngestRefused(
		    OtlpIngestRefusal::MALFORMED,
		    StringUtil::Format("OTLP parse failed for %s: %s", signal.table_name, otlp_status_message(status)));
	}

	vector<StagedSignal> staged;
	StageArrowBatch(signal_type, array.arrow_array, schema.arrow_schema, staged);
	return staged;
}

vector<OtlpServer::StagedSignal> OtlpServer::StageMetrics(const string &body, OtlpInputFormat format,
                                                          OtlpIngestResult &counts) {
	OtlpMetricsArrowBatches batches = {};
	struct BatchGuard {
		OtlpMetricsArrowBatches &batches;
		~BatchGuard() {
			ReleaseOtlpMetricBatches(batches);
		}
	} guard {batches};
	OtlpStatus status = otlp_transform_metrics_all_with_profile(
	    format, OTLP_ARROW_PROFILE_DUCKDB, reinterpret_cast<const uint8_t *>(body.data()), body.size(), &batches);

	if (status != OTLP_OK) {
		throw OtlpIngestRefused(OtlpIngestRefusal::MALFORMED,
		                        StringUtil::Format("OTLP parse failed for metrics: %s", otlp_status_message(status)));
	}

	// Stage every metric shape before any reaches the live buffers (Admit -> CommitStaged), so a
	// failure on a later shape cannot make a client retry double-buffer the earlier ones.
	vector<StagedSignal> staged;
	for (auto signal_type : OTLP_SIGNALS) {
		if (signal_type < OTLP_SIGNAL_METRICS_GAUGE) {
			continue;
		}
		auto &shape = OtlpMetricShape(batches, signal_type);
		if (shape.present) {
			StageArrowBatch(signal_type, shape.array, shape.schema, staged);
		}
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
		if (metrics_stage_fault_hook_for_test && signal_type == OTLP_SIGNAL_METRICS_GAUGE) {
			metrics_stage_fault_hook_for_test();
		}
#endif
	}

	// The skipped-counters describe what the Rust transform dropped, not what was buffered; a
	// refused request reports none of them (Admit returns a fresh outcome).
	counts.skipped_summaries += batches.skipped_summaries;
	counts.skipped_nan_values += batches.skipped_nan_values;
	counts.skipped_infinity_values += batches.skipped_infinity_values;
	counts.skipped_missing_values += batches.skipped_missing_values;
	return staged;
}

void OtlpServer::StageArrowBatch(OtlpSignalType signal_type, ArrowArray &array, ArrowSchema &schema,
                                 vector<StagedSignal> &staged) {
	if (array.length <= 0) {
		return;
	}
	const auto &signal = *signals[signal_type];
	auto db = db_ptr.lock();
	if (!db) {
		throw IOException("Database was closed");
	}
	auto &table = *signal.arrow_table;
	table.CheckBatchSchema(schema);
	auto &allocator = Allocator::Get(*db);
	StagedSignal stage {signal_type, make_uniq<ColumnDataCollection>(allocator, signal.types)};
	// `imported` may reference the borrowed Arrow buffers (zero-copy); Append copies out of them.
	auto batch = OtlpBorrowArrowBatch(array);
	DataChunk imported;
	imported.Initialize(allocator, table.types, APPEND_CHUNK_SIZE);
	DataChunk chunk;
	chunk.Initialize(allocator, signal.types, APPEND_CHUNK_SIZE);
	for (idx_t offset = 0; offset < static_cast<idx_t>(array.length); offset += APPEND_CHUNK_SIZE) {
		auto count = MinValue<idx_t>(APPEND_CHUNK_SIZE, static_cast<idx_t>(array.length) - offset);
		imported.Reset();
		chunk.Reset();
		OtlpArrowScan(*arrow_con->context, table.arrow_table.GetColumns(), batch, signal.identity_column_ids, offset,
		              count, imported);
		for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
			if (imported.data[col].GetType() == chunk.data[col].GetType()) {
				chunk.data[col].Reference(imported.data[col]);
			} else {
				TimestampNsToTimestamp(imported.data[col], chunk.data[col], count);
			}
		}
		chunk.SetChildCardinality(count);
		stage.collection->Append(chunk);
		stage.rows += count;
		stage.batches++;
	}
	staged.push_back(std::move(stage));
}

void OtlpServer::CommitStaged(vector<StagedSignal> &staged, OtlpIngestResult &result, idx_t &admission_bytes) {
	if (staged.empty()) {
		// Nothing buffered: Admit releases the unclaimed admission.
		return;
	}
	{
		std::lock_guard<std::mutex> lock(buffer_mutex);
		if (live.Rows() == 0) {
			live.oldest = std::chrono::steady_clock::now();
		}
		for (auto &signal : staged) {
			live.rows[signal.signal_type]->Combine(*signal.collection);
			result.rows += signal.rows;
			result.batches += signal.batches;
		}
		live.admission_bytes += admission_bytes;
		admission_bytes = 0;
	}
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

void OtlpServer::RestoreUnsealed(OtlpBuffers &plan, const std::vector<bool> &sealed, idx_t restored_admission) {
	std::lock_guard<std::mutex> lock(buffer_mutex);
	live.oldest = live.Rows() == 0 ? plan.oldest : std::min(plan.oldest, live.oldest);
	for (idx_t i = 0; i < plan.rows.size(); i++) {
		if (!sealed[i] && plan.rows[i]->Count() > 0) {
			plan.rows[i]->Combine(*live.rows[i]); // old rows, then live rows
			std::swap(plan.rows[i], live.rows[i]);
		}
	}
	live.admission_bytes += restored_admission;
}

void OtlpServer::RecordSealOutcome(bool success, idx_t rows_committed, const string &error) {
	if (success) {
		seals_total.fetch_add(1);
		committed_rows_total.fetch_add(rows_committed);
		last_seal_unix_ms.store(NowUnixMs());
	} else {
		seal_failures_total.fetch_add(1);
	}
	std::lock_guard<std::mutex> lock(seal_error_mutex);
	seal_last_error = error;
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

	RunSQL(connection, std::move(parser.statements[0]));
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

//! Fix one INSERT or partitioned COPY statement per signal before ingest starts.
void OtlpServer::BuildSinks() {
	const bool parquet = !config.parquet_export_path.empty();
	for (auto &signal : signals) {
		if (parquet) {
			signal->sink_sql = ParquetSinkSql(signal->table_name, config.parquet_export_path);
			continue;
		}
		// BY NAME: table columns beyond the buffer's (earlier promotions, operator additions) are NULL-filled.
		signal->sink_sql = "INSERT INTO " +
		                   QualifiedTable(config.catalog_name, config.schema_name, signal->table_name) +
		                   " BY NAME SELECT *" + promote_projection + " FROM " + SEALED_RELATION;
	}
}

idx_t OtlpServer::SealOnce() {
	std::lock_guard<std::mutex> writer_lock(writer_mutex);
	if (!writer_con) {
		return 0;
	}
	auto db = db_ptr.lock();
	if (!db) {
		return 0; // database closed; nothing we can durably write
	}
	auto &allocator = Allocator::Get(*db);
	const auto seal_started = std::chrono::steady_clock::now();
	const auto elapsed_ms = [](std::chrono::steady_clock::time_point started) {
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started)
		    .count();
	};

	// Workers fill the replacement buffers while the writer owns the detached plan.
	auto plan = MakeEmptyBuffers(allocator);
	std::vector<bool> sealed(signals.size(), false);
	{
		std::lock_guard<std::mutex> lock(buffer_mutex);
		std::swap(live, plan);
	}
	const auto total_rows = plan.Rows();
	if (total_rows == 0) {
		ReleaseAdmission(plan.admission_bytes);
		last_seal_unix_ms.store(NowUnixMs());
		return 0;
	}

	// A catalog seal is one transaction: a failure rolls back and re-buffers everything. A Parquet
	// COPY cannot be rolled back, so export is at-least-once: a failure re-buffers only signals not
	// yet exported, and a COPY that throws part-way may leave duplicates for readers to tolerate.
	const bool transactional = config.parquet_export_path.empty();
	idx_t written_rows = 0;
	idx_t written_batches = 0;
	int64_t append_duration_ms = 0;
	int64_t commit_duration_ms = 0;
	auto phase_started = seal_started;
	bool appending = true;
	idx_t failed_signal = 0;
	try {
		if (transactional) {
			RunSQL(*writer_con, "BEGIN TRANSACTION");
		}
		phase_started = std::chrono::steady_clock::now();
		for (idx_t i = 0; i < signals.size(); i++) {
			failed_signal = i;
			auto &collection = *plan.rows[i];
			if (collection.Count() == 0) {
				continue;
			}
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
			if (!transactional && parquet_seal_fault_hook_for_test) {
				parquet_seal_fault_hook_for_test(i);
			}
#endif
			auto &signal = *signals[i];
			ExecuteOverSealed(*writer_con, signal.sink_sql, collection, signal.names);
			sealed[i] = true;
			written_rows += collection.Count();
			written_batches += collection.ChunkCount();
			if (!transactional && config.parquet_views) {
				// The export is durable; the view is best-effort and never fails it.
				EnsureParquetView(signal);
			}
		}
		append_duration_ms = elapsed_ms(phase_started);
		if (transactional) {
			appending = false;
			phase_started = std::chrono::steady_clock::now();
#ifdef DUCKDB_OTLP_ENABLE_TEST_SEAM
			if (seal_fault_hook_for_test) {
				seal_fault_hook_for_test();
			}
#endif
			RunSQL(*writer_con, "COMMIT");
			commit_duration_ms = elapsed_ms(phase_started);
		}
	} catch (...) {
		auto &failed_phase_ms = appending ? append_duration_ms : commit_duration_ms;
		failed_phase_ms = elapsed_ms(phase_started);
		// catch(...) so a non-std throw cannot std::terminate the host and the buffers are restored.
		auto msg = CurrentExceptionMessage("seal");
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
		const auto sealed_rows = transactional ? 0 : written_rows;
		const auto restored_rows = total_rows - sealed_rows;
		// Use a wide product and release the complement so rounding preserves the budget exactly.
		const auto restored_admission =
		    static_cast<idx_t>(uhugeint_t(plan.admission_bytes) * restored_rows / total_rows);
		if (restored_rows > 0) {
			RestoreUnsealed(plan, sealed, restored_admission);
		}
		ReleaseAdmission(plan.admission_bytes - restored_admission);
		if (sealed_rows > 0) {
			// A partial Parquet export is durable: it advances the seal clock and committed_rows_total
			// even though the seal failed overall.
			last_seal_unix_ms.store(NowUnixMs());
			committed_rows_total.fetch_add(sealed_rows);
		}
		LogServerEvent(
		    StringUtil::Format("seal failed at %s (%llu/%llu rows sealed) duration_ms=%lld append_duration_ms=%lld "
		                       "commit_duration_ms=%lld: %s",
		                       signals[failed_signal]->table_name, static_cast<uint64_t>(sealed_rows),
		                       static_cast<uint64_t>(total_rows), elapsed_ms(seal_started), append_duration_ms,
		                       commit_duration_ms, msg),
		    LogLevel::LOG_WARNING);
		RecordSealOutcome(false, 0, msg);
		throw IOException("seal failed: %s", msg);
	}

	ReleaseAdmission(plan.admission_bytes);
	auto target = config.parquet_export_path;
	if (transactional) {
		target = config.catalog_name.empty() ? config.schema_name : config.catalog_name + "." + config.schema_name;
	}
	LogServerEvent(StringUtil::Format("seal: target=%s rows=%llu batches=%llu duration_ms=%lld append_duration_ms=%lld "
	                                  "commit_duration_ms=%lld",
	                                  target, static_cast<uint64_t>(written_rows),
	                                  static_cast<uint64_t>(written_batches), elapsed_ms(seal_started),
	                                  append_duration_ms, commit_duration_ms),
	               LogLevel::LOG_DEBUG);
	RecordSealOutcome(true, written_rows, "");
	catalog_maintenance_due = true;
	return written_rows;
}

void OtlpServer::EnsureParquetView(OtlpSignal &signal) {
	// Created after files exist so the view binds a schema. Binding reads every file's footer, and
	// DuckDB binds a CREATE VIEW before checking IF NOT EXISTS, so the catalog is checked first and
	// each signal is settled once: a failure is logged, never retried against a growing archive.
	if (signal.parquet_view_settled) {
		return;
	}
	signal.parquet_view_settled = true;
	const auto &table_name = signal.table_name;
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
	} catch (std::exception &) {
		LogServerEvent(
		    StringUtil::Format("parquet inspection view %s not created: %s", view, CurrentExceptionMessage()),
		    LogLevel::LOG_WARNING);
	}
}

void OtlpServer::MaybeRunCatalogMaintenance() {
	// The target is fixed, and only the sealer thread changes the schedule and support flag.
	auto now = std::chrono::steady_clock::now();
	if (!maintains_catalog || maintenance_unsupported ||
	    now - catalog_maintenance_last_attempt < std::chrono::milliseconds(config.maintenance_interval_ms)) {
		return;
	}
	std::lock_guard<std::mutex> writer_lock(writer_mutex);
	if (!catalog_maintenance_due || !writer_con) {
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
		maintenance_runs_total.fetch_add(1);
		last_maintenance_unix_ms.store(NowUnixMs());
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			maintenance_last_error.clear();
		}
		LogServerEvent(StringUtil::Format("catalog maintenance checkpoint succeeded: catalog=%s", config.catalog_name));
		if (is_ducklake) {
			SweepOrphanedFiles();
		}
	} catch (...) {
		auto msg = CurrentExceptionMessage("catalog maintenance checkpoint");
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
			maintenance_unsupported = true;
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
	// A contended CHECKPOINT can leave files with no catalog entry. Sweep after a successful
	// CHECKPOINT using DuckLake's retention policy. Sweep errors do not count as checkpoint failures.
	try {
		auto result = RunSQL(*writer_con, "SELECT count(*) FROM ducklake_delete_orphaned_files(" +
		                                      SqlQuote(config.catalog_name) + ")");
		auto chunk = result->Fetch();
		auto deleted = chunk && chunk->size() > 0 ? chunk->GetValue(0, 0).GetValue<int64_t>() : 0;
		if (deleted > 0) {
			LogServerEvent(StringUtil::Format("catalog maintenance deleted %lld orphaned data files: catalog=%s",
			                                  static_cast<int64_t>(deleted), config.catalog_name));
		}
	} catch (std::exception &) {
		auto msg = CurrentExceptionMessage();
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
	return OldestBufferedAgeMs() >= config.seal_max_age_ms;
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
	// Final drain. On the implicit DB-teardown path (~OtlpServerRegistry) db_ptr is already
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
