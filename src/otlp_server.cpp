#include "otlp_server.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

#include "otlp_arrow.hpp"
#include "otlp_log.hpp"
#include "otlp_server_internal.hpp"
#include "otlp_sql_util.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <sstream>

namespace duckdb {

static int64_t NowUnixMs();

//! Milliseconds elapsed since `last_unix_ms` (a NowUnixMs() timestamp), or -1 if
//! it was never set (0). Clamps to 0 if the clock appears to have gone backwards.
static int64_t AgeMsSince(int64_t last_unix_ms);

namespace {

static constexpr idx_t TOKEN_BYTES = 16;
static constexpr idx_t APPEND_CHUNK_SIZE = STANDARD_VECTOR_SIZE;
//! Internal best-effort maintenance cadence. Catalog-native CHECKPOINT can add its
//! own commit for catalogs that implement it, so keep it well below seal cadence.
static constexpr idx_t CATALOG_MAINTENANCE_ROW_SEAL_INTERVAL = 32;
static constexpr int64_t CATALOG_MAINTENANCE_MIN_INTERVAL_MS = 5 * 60 * 1000;
static constexpr int64_t CATALOG_MAINTENANCE_HEADROOM_MS = 60 * 1000;
static constexpr double CATALOG_MAINTENANCE_HEADROOM_FRACTION = 0.5;
static constexpr double CATALOG_MAINTENANCE_RATE_EWMA_ALPHA = 0.5;

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

static string QualifiedTable(const string &catalog_name, const string &schema_name, const string &table_name) {
	string qualified;
	if (!catalog_name.empty()) {
		qualified += QuoteIdentifier(catalog_name) + ".";
	}
	qualified += QuoteIdentifier(schema_name) + "." + QuoteIdentifier(table_name);
	return qualified;
}

static string ExportRootForTable(const string &root, const string &table_name) {
	if (root.empty()) {
		return "";
	}
	auto value = root;
	while (!value.empty() && value[value.size() - 1] == '/') {
		value = value.substr(0, value.size() - 1);
	}
	return value + "/" + table_name;
}

static const char *PartitionTimestampColumn(const string &table_name) {
	if (table_name == "otlp_traces") {
		return "start_time_unix_nano";
	}
	return "time_unix_nano";
}

static string BuildParquetExportSql(const string &temp_table, const string &table_name, const string &root) {
	auto timestamp_column = PartitionTimestampColumn(table_name);
	auto quoted_temp = QuoteIdentifier(temp_table);
	std::ostringstream sql;
	sql << "COPY (SELECT *, "
	    << "strftime(" << QuoteIdentifier(timestamp_column) << ", '%Y') AS year, "
	    << "strftime(" << QuoteIdentifier(timestamp_column) << ", '%m') AS month, "
	    << "strftime(" << QuoteIdentifier(timestamp_column) << ", '%d') AS day "
	    << "FROM " << quoted_temp << ") TO " << SqlQuote(ExportRootForTable(root, table_name)) << " ("
	    << "FORMAT PARQUET, "
	    << "COMPRESSION ZSTD, "
	    << "PARTITION_BY (year, month, day), "
	    << "APPEND, "
	    << "FILENAME_PATTERN 'seal_{uuidv7}', "
	    << "WRITE_PARTITION_COLUMNS false"
	    << ")";
	return sql.str();
}

static bool IsUnsupportedCatalogMaintenanceError(const string &message) {
	auto lower = StringUtil::Lower(message);
	return (lower.find("checkpoint") != string::npos &&
	        (lower.find("not implemented") != string::npos || lower.find("not supported") != string::npos ||
	         lower.find("does not support") != string::npos || lower.find("unsupported") != string::npos)) ||
	       lower.find("checkpoint is only supported") != string::npos;
}

static bool CatalogMaintenanceTestOverridesEnabled() {
	auto value = std::getenv("DUCKDB_OTLP_TEST_CATALOG_MAINTENANCE");
	return value && string(value) == "1";
}

template <class T>
static T GetCatalogMaintenanceTestOverride(const char *name, T fallback, bool allow_zero = false) {
	if (!CatalogMaintenanceTestOverridesEnabled()) {
		return fallback;
	}
	auto value = std::getenv(name);
	if (!value || value[0] == '\0') {
		return fallback;
	}
	try {
		size_t pos = 0;
		auto parsed = std::stoull(value, &pos);
		if (pos != string(value).size() || (!allow_zero && parsed == 0)) {
			return fallback;
		}
		return static_cast<T>(parsed);
	} catch (...) {
		return fallback;
	}
}

static void RunSQL(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (!result || result->HasError()) {
		auto error = result ? result->GetError() : string("DuckDB query failed");
		throw IOException("%s", error);
	}
}

static void DropTableIfExists(Connection &connection, const string &table_name) {
	RunSQL(connection, "DROP TABLE IF EXISTS " + QuoteIdentifier(table_name));
}

static idx_t AppendCollectionToTable(Connection &connection, ColumnDataCollection &collection,
                                     const string &catalog_name, const string &schema_name, const string &table_name) {
	unique_ptr<Appender> appender;
	if (schema_name.empty()) {
		appender = make_uniq<Appender>(connection, table_name);
	} else if (catalog_name.empty()) {
		appender = make_uniq<Appender>(connection, schema_name, table_name);
	} else {
		appender = make_uniq<Appender>(connection, catalog_name, schema_name, table_name);
	}
	idx_t batches = 0;
	ColumnDataScanState scan;
	collection.InitializeScan(scan);
	DataChunk chunk;
	collection.InitializeScanChunk(chunk);
	while (collection.Scan(scan, chunk)) {
		appender->AppendDataChunk(chunk);
		batches++;
	}
	appender->Close();
	return batches;
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

} // namespace

void OtlpServer::GetSignalColumns(OtlpSignalType signal_type, vector<LogicalType> &types, vector<string> &names) {
	ArrowSchema arrow_schema;
	OtlpStatus status = otlp_get_schema(signal_type, &arrow_schema);
	if (status != OTLP_OK) {
		throw IOException("Failed to get OTLP schema: %s", otlp_status_message(status));
	}
	try {
		OtlpArrowSchemaOptions options;
		options.timestamp_ns_as_timestamp = true;
		GetArrowSchemaColumns(arrow_schema, types, names, options);
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

void OtlpServer::InitBuffers() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto &allocator = Allocator::Get(*db);
	for (auto &target : TARGET_TABLES) {
		vector<LogicalType> types;
		vector<string> names;
		GetSignalColumns(target.signal_type, types, names);
		auto collection = make_uniq<ColumnDataCollection>(allocator, types);
		signal_buffers.push_back(make_uniq<OtlpSignalBuffer>(target.signal_type, target.table_name, std::move(types),
		                                                     std::move(collection)));
	}
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
		auto next = current > bytes ? current - bytes : 0;
		if (admitted_bytes.compare_exchange_weak(current, next)) {
			return;
		}
	}
}

void OtlpServer::ClaimUnsealedAdmission(idx_t &bytes) {
	if (bytes == 0) {
		return;
	}
	std::lock_guard<std::mutex> lock(admission_mutex);
	unsealed_admission_bytes += bytes;
	bytes = 0;
}

void OtlpServer::EnsureTargetTables() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	// Local Parquet export: pre-create the root directory so the seal's COPY (which only
	// creates its own partition subdirectories) has an existing base path. Remote/object
	// stores (s3://, gs://, ...) need no directory creation; a real permission/path
	// problem surfaces on the first seal.
	if (!config.parquet_export_path.empty() && config.parquet_export_path.find("://") == string::npos) {
		std::error_code ec;
		std::filesystem::create_directories(config.parquet_export_path, ec);
	}
	// Runs once at construction (single-threaded), so a throwaway connection is fine.
	Connection con(*db);
	con.context->config.enable_progress_bar = false;
	for (auto &target : TARGET_TABLES) {
		CreateOrValidateTable(con, target.signal_type, target.table_name);
	}
}

void OtlpServer::CreateOrValidateTable(Connection &con, OtlpSignalType signal_type, const string &table_name) {
	if (!config.parquet_export_path.empty()) {
		// Parquet mode keeps no persistent destination table: the durable store is the
		// Parquet dataset, and inspection is a lazily-created view over it (see SealOnce).
		return;
	}
	vector<LogicalType> expected_types;
	vector<string> expected_names;
	GetSignalColumns(signal_type, expected_types, expected_names);

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
			                            expected_names[i], result->types[i].ToString(), expected_types[i].ToString());
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

	// Backpressure: reserve payload bytes before parse/transform so concurrent bursts
	// cannot all pass the check and allocate work beyond max_buffered_bytes. The
	// reservation moves into the unsealed-row counter when rows are buffered and is
	// released only after those rows seal; parse/validation failures release it immediately.
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
	for (auto &buf : signal_buffers) {
		if (buf->signal_type == signal_type) {
			return *buf;
		}
	}
	throw InternalException("No OTLP buffer for signal type");
}

void OtlpServer::BufferSignal(OtlpSignalType signal_type, const string &body, OtlpInputFormat format,
                              OtlpIngestResult &result, idx_t &admission_bytes) {
	auto &buf = BufferFor(signal_type);
	ArrowArray array;
	ArrowSchema schema;
	OtlpStatus status = otlp_transform(signal_type, format, reinterpret_cast<const uint8_t *>(body.data()), body.size(),
	                                   &array, &schema);
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
	try {
		AppendArrowBatch(buf, array, schema, result, admission_bytes);
	} catch (...) {
		release();
		throw;
	}
	release();
}

void OtlpServer::BufferMetrics(const string &body, OtlpInputFormat format, OtlpIngestResult &result,
                               idx_t &admission_bytes) {
	OtlpMetricsArrowBatches batches = {};
	OtlpStatus status =
	    otlp_transform_metrics_all(format, reinterpret_cast<const uint8_t *>(body.data()), body.size(), &batches);

	auto release_all = [&]() {
		ReleaseOtlpArrowBatch(batches.gauge);
		ReleaseOtlpArrowBatch(batches.sum);
		ReleaseOtlpArrowBatch(batches.histogram);
		ReleaseOtlpArrowBatch(batches.exp_histogram);
	};

	if (status != OTLP_OK) {
		release_all();
		throw OtlpHttpError(400, StringUtil::Format("OTLP parse failed for metrics: %s", otlp_status_message(status)));
	}

	result.skipped_summaries += batches.skipped_summaries;
	result.skipped_nan_values += batches.skipped_nan_values;
	result.skipped_infinity_values += batches.skipped_infinity_values;
	result.skipped_missing_values += batches.skipped_missing_values;

	try {
		if (batches.gauge.present) {
			AppendArrowBatch(BufferFor(OTLP_SIGNAL_METRICS_GAUGE), batches.gauge.array, batches.gauge.schema, result,
			                 admission_bytes);
		}
		if (batches.sum.present) {
			AppendArrowBatch(BufferFor(OTLP_SIGNAL_METRICS_SUM), batches.sum.array, batches.sum.schema, result,
			                 admission_bytes);
		}
		if (batches.histogram.present) {
			AppendArrowBatch(BufferFor(OTLP_SIGNAL_METRICS_HISTOGRAM), batches.histogram.array,
			                 batches.histogram.schema, result, admission_bytes);
		}
		if (batches.exp_histogram.present) {
			AppendArrowBatch(BufferFor(OTLP_SIGNAL_METRICS_EXP_HISTOGRAM), batches.exp_histogram.array,
			                 batches.exp_histogram.schema, result, admission_bytes);
		}
	} catch (...) {
		release_all();
		throw;
	}
	release_all();
}

void OtlpServer::AppendArrowBatch(OtlpSignalBuffer &buf, ArrowArray &array, ArrowSchema &schema,
                                  OtlpIngestResult &result, idx_t &admission_bytes) {
	if (array.length <= 0) {
		return;
	}
	DataChunk chunk;
	auto db = db_ptr.lock();
	if (!db) {
		throw IOException("Database was closed");
	}
	chunk.Initialize(Allocator::Get(*db), buf.types, APPEND_CHUNK_SIZE);
	for (idx_t offset = 0; offset < static_cast<idx_t>(array.length); offset += APPEND_CHUNK_SIZE) {
		auto count = MinValue<idx_t>(APPEND_CHUNK_SIZE, static_cast<idx_t>(array.length) - offset);
		chunk.Reset();
		CopyArrowStructToDataChunk(array, schema, chunk, offset, count);
		BufferAppend(buf, chunk, admission_bytes);
		result.rows += count;
		result.batches++;
	}
}

void OtlpServer::BufferAppend(OtlpSignalBuffer &buf, DataChunk &chunk, idx_t &admission_bytes) {
	auto rows = chunk.size();
	if (rows == 0) {
		return;
	}
	{
		std::lock_guard<std::mutex> lock(buf.mutex);
		buf.collection->Append(chunk);
		if (!buf.have_unsealed) {
			buf.have_unsealed = true;
			buf.first_unsealed = std::chrono::steady_clock::now();
		}
		buf.buffered_rows += rows;
		ClaimUnsealedAdmission(admission_bytes);
	}
	// The single real byte counter (admitted-and-not-yet-sealed body bytes) drives the
	// size trigger; the exact COPY size is whatever the buffered chunks hold at seal time.
	if (admitted_bytes.load() >= config.seal_target_bytes) {
		RequestSeal();
	}
}

void OtlpServer::RequestSeal() {
	flush_requested.store(true);
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

OtlpIngestResult OtlpServer::SealOnce(bool allow_maintenance) {
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
	int64_t append_duration_ms = 0;
	int64_t commit_duration_ms = 0;

	struct SealingBuffer {
		unique_ptr<ColumnDataCollection> collection;
		idx_t rows = 0;
	};

	std::vector<unique_ptr<ColumnDataCollection>> fresh;
	fresh.reserve(signal_buffers.size());
	for (auto &buf : signal_buffers) {
		fresh.push_back(make_uniq<ColumnDataCollection>(allocator, buf->types));
	}

	// Swap each buffer out for a fresh empty collection so workers keep filling during
	// the (potentially slow) COPY. The moved-out collections are owned solely here.
	std::vector<SealingBuffer> sealing;
	idx_t total = 0;
	idx_t sealed_admission_bytes = 0;
	{
		std::vector<std::unique_lock<std::mutex>> locks;
		locks.reserve(signal_buffers.size());
		for (auto &buf : signal_buffers) {
			locks.emplace_back(buf->mutex);
		}
		sealing.reserve(signal_buffers.size());
		std::lock_guard<std::mutex> admission_lock(admission_mutex);
		sealed_admission_bytes = unsealed_admission_bytes;
		unsealed_admission_bytes = 0;
		for (idx_t i = 0; i < signal_buffers.size(); i++) {
			auto &buf = *signal_buffers[i];
			SealingBuffer state;
			state.collection = std::move(buf.collection);
			state.rows = buf.buffered_rows;
			total += state.rows;

			buf.collection = std::move(fresh[i]);
			buf.buffered_rows = 0;
			buf.have_unsealed = false;
			sealing.push_back(std::move(state));
		}
	}

	OtlpIngestResult result;
	if (total == 0) {
		ReleaseAdmission(sealed_admission_bytes);
		last_seal_unix_ms.store(NowUnixMs());
		return result;
	}

	if (!config.parquet_export_path.empty()) {
		// Plain Parquet export path. Each signal is exported independently — there is no
		// shared transaction and no local destination table, so nothing is double-written
		// (the Parquet dataset is the only durable store). A COPY is a filesystem/object-
		// store side effect that cannot be rolled back, so export is at-least-once: on a
		// signal failure we re-buffer ONLY the signals that have not exported yet, so an
		// already-exported signal is never re-written (no cross-signal duplication). A
		// single signal whose COPY fails mid-write can still be re-exported on retry, so
		// downstream readers must tolerate duplicate rows.
		auto seal_id = seals_total.load() + 1;
		auto seal_attempt_id = seal_failures_total.load() + 1;
		std::vector<bool> exported(signal_buffers.size(), false);
		idx_t exported_rows = 0;
		bool failed = false;
		string failure_msg;
		idx_t failed_signal = 0;
		for (idx_t i = 0; i < signal_buffers.size(); i++) {
			if (sealing[i].rows == 0) {
				exported[i] = true; // nothing buffered for this signal
				continue;
			}
			auto &buf = *signal_buffers[i];
			auto staging =
			    StringUtil::Format("__otlp_seal_%s_%llu_%llu_%llu", buf.table_name, static_cast<uint64_t>(seal_id),
			                       static_cast<uint64_t>(seal_attempt_id), static_cast<uint64_t>(i));
			try {
				// Stage this seal's rows in a TEMP table whose schema is derived from the
				// signal definition (not a persistent destination), then COPY only those
				// rows to the partitioned dataset.
				vector<LogicalType> staging_types;
				vector<string> staging_names;
				GetSignalColumns(buf.signal_type, staging_types, staging_names);
				string ddl = "CREATE TEMP TABLE " + QuoteIdentifier(staging) + " (";
				for (idx_t c = 0; c < staging_names.size(); c++) {
					ddl += (c > 0 ? string(", ") : string()) + QuoteIdentifier(staging_names[c]) + " " +
					       staging_types[c].ToString();
				}
				ddl += ")";
				RunSQL(*writer_con, ddl);
				result.batches +=
				    AppendCollectionToTable(*writer_con, *sealing[i].collection, string(), string(), staging);
				RunSQL(*writer_con, BuildParquetExportSql(staging, buf.table_name, config.parquet_export_path));
			} catch (std::exception &ex) {
				failed = true;
				failure_msg = ex.what();
				failed_signal = i;
				try {
					DropTableIfExists(*writer_con, staging);
				} catch (...) {
				}
				break;
			} catch (...) {
				failed = true;
				failure_msg = "unknown (non-std) exception during parquet seal";
				failed_signal = i;
				try {
					DropTableIfExists(*writer_con, staging);
				} catch (...) {
				}
				break;
			}
			// COPY returned: these rows are durable in Parquet. The staging drop and the
			// inspection view are best-effort and must never turn a durable export into a
			// failure (that would re-buffer and re-export the rows).
			exported[i] = true;
			exported_rows += sealing[i].rows;
			result.rows += sealing[i].rows;
			try {
				DropTableIfExists(*writer_con, staging);
			} catch (...) {
			}
			try {
				// Lazily (re)create a read-only view over the exported files so Quack can
				// inspect the data without keeping a second local copy. Created here, after
				// files exist, so the view binds a resolvable schema.
				auto view = QualifiedTable(config.catalog_name, config.schema_name, buf.table_name);
				auto glob = ExportRootForTable(config.parquet_export_path, buf.table_name) + "/**/*.parquet";
				RunSQL(*writer_con, "CREATE VIEW IF NOT EXISTS " + view + " AS SELECT * FROM read_parquet(" +
				                        SqlQuote(glob) + ", hive_partitioning=false, union_by_name=true)");
			} catch (...) {
				// Inspection view is a convenience; retried on the next successful seal.
			}
		}

		if (failed) {
			// Re-buffer only the signals that did NOT export, ahead of any rows workers
			// appended during the COPY (old rows first, preserving order).
			idx_t restored_rows = total - exported_rows;
			{
				std::vector<std::unique_lock<std::mutex>> locks;
				locks.reserve(signal_buffers.size());
				for (auto &buf : signal_buffers) {
					locks.emplace_back(buf->mutex);
				}
				for (idx_t i = 0; i < signal_buffers.size(); i++) {
					if (exported[i] || sealing[i].rows == 0) {
						continue;
					}
					auto &buf = *signal_buffers[i];
					sealing[i].collection->Combine(*buf.collection); // old rows, then live rows
					buf.collection = std::move(sealing[i].collection);
					buf.buffered_rows += sealing[i].rows;
					buf.have_unsealed = true;
					buf.first_unsealed = std::chrono::steady_clock::now();
				}
				// Split the aggregate admission proportionally by rows: keep the un-exported
				// share in the unsealed pool and release the exported share below.
				// admitted_bytes is an approximate throughput proxy, so this is sufficient.
				idx_t restored_admission =
				    static_cast<idx_t>(static_cast<double>(sealed_admission_bytes) *
				                       static_cast<double>(restored_rows) / static_cast<double>(total));
				if (restored_admission > sealed_admission_bytes) {
					restored_admission = sealed_admission_bytes;
				}
				std::lock_guard<std::mutex> admission_lock(admission_mutex);
				unsealed_admission_bytes += restored_admission;
				sealed_admission_bytes -= restored_admission;
			}
			ReleaseAdmission(sealed_admission_bytes); // release only the exported share
			if (exported_rows > 0) {
				// Some signals were durably written; advance the seal clock for them but do
				// not count this as a fully successful seal.
				last_seal_unix_ms.store(NowUnixMs());
			}
			seal_failures_total.fetch_add(1);
			// Parquet COPY is not transactional, so the exported share is durable even though the
			// seal failed overall. Count it in committed_rows_total (it reflects rows on disk, not
			// successful seals); this is why otlp_seal_list() can show success=false with
			// rows_committed > 0. The transaction path below adds nothing on failure (rollback).
			committed_rows_total.fetch_add(exported_rows);
			{
				std::lock_guard<std::mutex> elock(seal_error_mutex);
				seal_last_error = failure_msg;
			}
			LogServerEvent(StringUtil::Format("parquet seal failed at signal %llu (%llu/%llu rows exported): %s",
			                                  static_cast<uint64_t>(failed_signal),
			                                  static_cast<uint64_t>(exported_rows), static_cast<uint64_t>(total),
			                                  failure_msg),
			               LogLevel::LOG_WARNING);
			RecordSealEvent(seal_started_unix_ms, append_duration_ms, commit_duration_ms, exported_rows,
			                sealed_admission_bytes, false, failure_msg);
			throw IOException(StringUtil::Format("parquet seal failed: %s", failure_msg));
		}

		ReleaseAdmission(sealed_admission_bytes);
		seals_total.fetch_add(1);
		committed_rows_total.fetch_add(result.rows);
		last_seal_unix_ms.store(NowUnixMs());
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			seal_last_error.clear();
		}
		LogServerEvent(StringUtil::Format("parquet seal: path=%s rows=%llu batches=%llu", config.parquet_export_path,
		                                  static_cast<uint64_t>(result.rows), static_cast<uint64_t>(result.batches)));
		RecordSealEvent(seal_started_unix_ms, append_duration_ms, commit_duration_ms, result.rows,
		                sealed_admission_bytes, true, "");
		return result;
	}

	try {
		RunSQL(*writer_con, "BEGIN TRANSACTION");
		auto append_started = std::chrono::steady_clock::now();
		for (idx_t i = 0; i < signal_buffers.size(); i++) {
			if (sealing[i].rows == 0) {
				continue;
			}
			auto &buf = *signal_buffers[i];
			result.batches += AppendCollectionToTable(*writer_con, *sealing[i].collection, config.catalog_name,
			                                          config.schema_name, buf.table_name);
			result.rows += sealing[i].rows;
		}
		append_duration_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - append_started)
		        .count();
		auto commit_started = std::chrono::steady_clock::now();
		RunSQL(*writer_con, "COMMIT");
		commit_duration_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - commit_started)
		        .count();
	} catch (...) {
		// catch(...) — not just std::exception — so a non-std throw can never escape and
		// std::terminate the host process, and the buffer is always restored.
		string msg;
		try {
			throw;
		} catch (std::exception &ex) {
			msg = ex.what();
		} catch (...) {
			msg = "unknown (non-std) exception during seal";
		}
		bool rollback_ok = true;
		try {
			RunSQL(*writer_con, "ROLLBACK");
		} catch (...) {
			rollback_ok = false;
		}
		if (!rollback_ok) {
			// ROLLBACK failed: the writer connection may be wedged in an aborted
			// transaction. Rebuild it so the next seal can recover instead of failing
			// forever on BEGIN.
			try {
				writer_con = make_uniq<Connection>(*db);
				writer_con->context->config.enable_progress_bar = false;
			} catch (...) {
			}
		}
		// Restore the un-sealed rows ahead of whatever workers buffered during the COPY,
		// so nothing is lost and order is preserved. Only the old (swapped-out) rows are
		// re-counted; the live rows were already counted as they were appended. Resetting
		// first_unsealed to now loses precise row age across consecutive seal failures,
		// which is acceptable — the age trigger just restarts its clock.
		{
			std::vector<std::unique_lock<std::mutex>> locks;
			locks.reserve(signal_buffers.size());
			for (auto &buf : signal_buffers) {
				locks.emplace_back(buf->mutex);
			}
			for (idx_t i = 0; i < signal_buffers.size(); i++) {
				auto &state = sealing[i];
				if (state.rows == 0) {
					continue;
				}
				auto &buf = *signal_buffers[i];
				state.collection->Combine(*buf.collection); // old rows, then live rows
				buf.collection = std::move(state.collection);
				buf.buffered_rows += state.rows;
				buf.have_unsealed = true;
				buf.first_unsealed = std::chrono::steady_clock::now();
			}
			std::lock_guard<std::mutex> admission_lock(admission_mutex);
			unsealed_admission_bytes += sealed_admission_bytes;
		}
		seal_failures_total.fetch_add(1);
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			seal_last_error = msg;
		}
		LogServerEvent(StringUtil::Format("seal failed: %s", msg), LogLevel::LOG_WARNING);
		RecordSealEvent(seal_started_unix_ms, append_duration_ms, commit_duration_ms, 0, 0, false, msg);
		throw;
	}

	ReleaseAdmission(sealed_admission_bytes);
	seals_total.fetch_add(1);
	committed_rows_total.fetch_add(result.rows);
	last_seal_unix_ms.store(NowUnixMs());
	{
		std::lock_guard<std::mutex> elock(seal_error_mutex);
		seal_last_error.clear();
	}
	LogServerEvent(StringUtil::Format("seal: catalog=%s rows=%llu batches=%llu", config.catalog_name,
	                                  static_cast<uint64_t>(result.rows), static_cast<uint64_t>(result.batches)));
	RecordSealEvent(seal_started_unix_ms, append_duration_ms, commit_duration_ms, result.rows, sealed_admission_bytes,
	                true, "");
	if (allow_maintenance) {
		MaybeRunCatalogMaintenance(result.rows, sealed_admission_bytes);
	}
	return result;
}

void OtlpServer::MaybeRunCatalogMaintenance(idx_t sealed_rows, idx_t sealed_admission_bytes) {
	if (sealed_rows == 0 || config.catalog_name.empty() ||
	    catalog_maintenance_state == CatalogMaintenanceState::DISABLED || !writer_con) {
		return;
	}
	catalog_maintenance_row_seals_since_attempt++;
	auto now = std::chrono::steady_clock::now();
	auto seal_elapsed_ms = std::max<int64_t>(
	    1, std::chrono::duration_cast<std::chrono::milliseconds>(now - catalog_maintenance_last_row_seal).count());
	catalog_maintenance_last_row_seal = now;
	auto seal_rate_bytes_per_ms = static_cast<double>(sealed_admission_bytes) / static_cast<double>(seal_elapsed_ms);
	catalog_maintenance_ingress_rate_bytes_per_ms =
	    catalog_maintenance_ingress_rate_bytes_per_ms == 0
	        ? seal_rate_bytes_per_ms
	        : (CATALOG_MAINTENANCE_RATE_EWMA_ALPHA * seal_rate_bytes_per_ms) +
	              ((1 - CATALOG_MAINTENANCE_RATE_EWMA_ALPHA) * catalog_maintenance_ingress_rate_bytes_per_ms);

	auto elapsed_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(now - catalog_maintenance_last_attempt).count();
	auto row_seal_interval = GetCatalogMaintenanceTestOverride<idx_t>(
	    "DUCKDB_OTLP_TEST_CATALOG_MAINTENANCE_ROW_SEAL_INTERVAL", CATALOG_MAINTENANCE_ROW_SEAL_INTERVAL);
	auto min_interval_ms = GetCatalogMaintenanceTestOverride<int64_t>(
	    "DUCKDB_OTLP_TEST_CATALOG_MAINTENANCE_MIN_INTERVAL_MS", CATALOG_MAINTENANCE_MIN_INTERVAL_MS, true);
	if (catalog_maintenance_row_seals_since_attempt < row_seal_interval || elapsed_ms < min_interval_ms) {
		return;
	}
	auto pending_bytes = admitted_bytes.load();
	auto headroom_bytes = pending_bytes >= config.max_buffered_bytes ? 0 : config.max_buffered_bytes - pending_bytes;
	auto projected_bytes = catalog_maintenance_ingress_rate_bytes_per_ms * CATALOG_MAINTENANCE_HEADROOM_MS;
	if (projected_bytes > static_cast<double>(headroom_bytes) * CATALOG_MAINTENANCE_HEADROOM_FRACTION) {
		// Maintenance is best-effort. Only run it when recent ingress would still leave
		// ample admission headroom during a long catalog CHECKPOINT.
		return;
	}

	catalog_maintenance_row_seals_since_attempt = 0;
	catalog_maintenance_last_attempt = now;
	try {
		RunSQL(*writer_con, "CHECKPOINT " + QuoteIdentifier(config.catalog_name));
		catalog_maintenance_state = CatalogMaintenanceState::SUPPORTED;
		maintenance_runs_total.fetch_add(1);
		last_maintenance_unix_ms.store(NowUnixMs());
		{
			std::lock_guard<std::mutex> elock(seal_error_mutex);
			maintenance_last_error.clear();
		}
		LogServerEvent(StringUtil::Format("catalog maintenance checkpoint succeeded: catalog=%s", config.catalog_name));
	} catch (...) {
		string msg;
		try {
			throw;
		} catch (std::exception &ex) {
			msg = ex.what();
		} catch (...) {
			msg = "unknown (non-std) exception during catalog maintenance checkpoint";
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

void OtlpServer::ConfigureCatalogMaintenanceOptions() {
	// DuckLake's CHECKPOINT (the post-seal maintenance call) reads these catalog options:
	// target_file_size bounds merge_adjacent_files' output (so already-at-target files are
	// left alone -> O(new)/cycle, not O(total)); expire_older_than/delete_older_than gate how
	// old snapshots/files must be before they are expired and deleted. Set once at startup on
	// the writer connection. Best-effort: the default catalog and non-DuckLake catalogs (e.g.
	// Iceberg) reject set_option -> ignore here; the maintenance pass DISABLEs itself anyway.
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
	} catch (std::exception &ex) {
		// Not a DuckLake catalog (or options unsupported): leave defaults; maintenance will
		// DISABLE on its first CHECKPOINT if the catalog can't be checkpointed at all.
		LogServerEvent(StringUtil::Format("catalog maintenance options not applied for catalog=%s: %s",
		                                  config.catalog_name, ex.what()));
	}
}

void OtlpServer::SealerLoop() {
	// Poll at most every seal_max_age_ms (floored) so a small max_age is honored, rather
	// than a fixed 1s that would delay seals configured to be faster than 1s.
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
				SealOnce(true);
			} catch (...) {
				// Already logged + buffers restored (SealOnce catches all); back off.
				std::this_thread::sleep_for(std::chrono::milliseconds(250));
			}
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
	return SealOnce(false);
}

void OtlpServer::ShutdownIngest() {
	if (ingest_shutdown_done.exchange(true)) {
		return; // idempotent: Close() and ~OtlpServer() may both call this
	}
	sealer_stop.store(true);
	sealer_cv.notify_all();
	if (sealer_thread.joinable()) {
		sealer_thread.join();
	}
	// Final drain. Explicit otlp_flush is optional for normal ingest: the background
	// sealer handles size/age triggers, and otlp_stop lands here while the database is
	// still alive. On the implicit DB-teardown path (~OtlpStorageExtensionInfo), db_ptr
	// is already expired, so SealOnce is a no-op and buffered rows are dropped.
	// Callers should use otlp_stop before closing the database, or otlp_flush when they
	// need durable rows immediately while the server keeps running.
	for (int attempt = 0; attempt < 3 && BufferedRows() > 0; attempt++) {
		try {
			SealOnce(false);
		} catch (...) {
			LogServerEvent("final seal attempt failed during shutdown", LogLevel::LOG_WARNING);
		}
	}
	auto remaining_rows = BufferedRows();
	if (remaining_rows > 0) {
		LogServerEvent(
		    StringUtil::Format("dropping %llu buffered rows on shutdown (database closed without graceful otlp_stop, "
		                       "or repeated seal failure)",
		                       static_cast<uint64_t>(remaining_rows)),
		    LogLevel::LOG_WARNING);
	}
	{
		std::lock_guard<std::mutex> writer_lock(writer_mutex);
		writer_con.reset();
	}
}

} // namespace duckdb
