#include "otlp_server.hpp"

#include "duckdb/common/allocator.hpp"
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

#include <chrono>

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

static string QualifiedTable(const string &catalog_name, const string &schema_name, const string &table_name) {
	string qualified;
	if (!catalog_name.empty()) {
		qualified += QuoteIdentifier(catalog_name) + ".";
	}
	qualified += QuoteIdentifier(schema_name) + "." + QuoteIdentifier(table_name);
	return qualified;
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
	OtlpHttpError(int status_p, string message_p, int retry_after_seconds_p = 0)
	    : status(status_p), retry_after_seconds(retry_after_seconds_p), message(std::move(message_p)) {
	}

	const char *what() const noexcept override {
		return message.c_str();
	}

	int status;
	int retry_after_seconds;
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

//! In-memory buffer for one OTLP signal table. The metadata fields are immutable
//! after construction (so workers may read `types` lock-free); mutable rows live
//! behind this signal's own mutex so independent signals don't contend on append.
struct OtlpSignalBuffer {
	const OtlpSignalType signal_type;
	const string table_name;
	const vector<LogicalType> types;
	std::mutex mutex;
	unique_ptr<ColumnDataCollection> collection;
	idx_t buffered_rows = 0;
	bool have_unsealed = false;
	std::chrono::steady_clock::time_point first_unsealed;

	OtlpSignalBuffer(OtlpSignalType signal_type_p, string table_name_p, vector<LogicalType> types_p,
	                 unique_ptr<ColumnDataCollection> collection_p)
	    : signal_type(signal_type_p), table_name(std::move(table_name_p)), types(std::move(types_p)),
	      collection(std::move(collection_p)) {
	}
};

void OtlpServer::InitBuffers() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto &allocator = Allocator::Get(*db);
	for (auto &target : TARGET_TABLES) {
		ArrowSchema arrow_schema;
		OtlpStatus status = otlp_get_schema(target.signal_type, &arrow_schema);
		if (status != OTLP_OK) {
			throw IOException("Failed to get OTLP schema: %s", otlp_status_message(status));
		}
		vector<LogicalType> types;
		try {
			vector<string> names;
			GetArrowSchemaColumns(arrow_schema, types, names);
		} catch (...) {
			if (arrow_schema.release) {
				arrow_schema.release(&arrow_schema);
			}
			throw;
		}
		if (arrow_schema.release) {
			arrow_schema.release(&arrow_schema);
		}
		auto collection = make_uniq<ColumnDataCollection>(allocator, types);
		signal_buffers.push_back(make_uniq<OtlpSignalBuffer>(target.signal_type, target.table_name, std::move(types),
		                                                     std::move(collection)));
	}
}

void OtlpServer::StartSealer() {
	sealer_thread = std::thread([this] { SealerLoop(); });
}

int64_t OtlpServer::LastSealAgeMs() const {
	auto last = last_seal_unix_ms.load();
	if (last == 0) {
		return -1;
	}
	auto now =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	        .count();
	return now >= last ? now - last : 0;
}

idx_t OtlpServer::BufferedRows() const {
	idx_t rows = 0;
	for (auto &buf : signal_buffers) {
		std::lock_guard<std::mutex> lock(buf->mutex);
		rows += buf->buffered_rows;
	}
	return rows;
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

idx_t OtlpServer::AdmissionReservationBytes(idx_t body_size) const {
	static constexpr idx_t MIN_RESERVATION_BYTES = 1024;
	auto base = MaxValue<idx_t>(body_size, MIN_RESERVATION_BYTES);
	return base;
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

	// Backpressure: reserve payload bytes before parse/transform so concurrent bursts
	// cannot all pass the check and allocate work beyond max_buffered_bytes. The
	// reservation moves into the unsealed-row counter when rows are buffered and is
	// released only after those rows seal; parse/validation failures release it immediately.
	auto reservation_bytes = AdmissionReservationBytes(body.size());
	idx_t admitted_before = 0;
	if (!TryReserveAdmission(reservation_bytes, admitted_before)) {
		throw OtlpHttpError(
		    503, StringUtil::Format("ingest buffer full (%llu admitted bytes, request would add %llu); retry later",
		                            static_cast<uint64_t>(admitted_before), static_cast<uint64_t>(reservation_bytes)));
	}

	OtlpIngestResult result;
	idx_t unclaimed_admission = reservation_bytes;
	try {
		TransformAndBuffer(kind, body, format, result, unclaimed_admission);
	} catch (...) {
		ReleaseAdmission(unclaimed_admission);
		throw;
	}
	ReleaseAdmission(unclaimed_admission);
	total_rows.fetch_add(result.rows);
	return result;
}

void OtlpServer::TransformAndBuffer(OtlpRequestKind request_kind, const string &body, OtlpInputFormat format,
                                    OtlpIngestResult &result, idx_t &admission_bytes) {
	switch (request_kind) {
	case OtlpRequestKind::LOGS:
		BufferSignal(OTLP_SIGNAL_LOGS, body, format, result, admission_bytes);
		break;
	case OtlpRequestKind::TRACES:
		BufferSignal(OTLP_SIGNAL_TRACES, body, format, result, admission_bytes);
		break;
	case OtlpRequestKind::METRICS:
		BufferMetrics(body, format, result, admission_bytes);
		break;
	default:
		throw InternalException("Unknown OTLP request kind");
	}
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

	auto release_batch = [](OtlpArrowBatch &batch) {
		if (!batch.present) {
			return;
		}
		if (batch.array.release) {
			batch.array.release(&batch.array);
		}
		if (batch.schema.release) {
			batch.schema.release(&batch.schema);
		}
		batch.present = 0;
	};
	auto release_all = [&]() {
		release_batch(batches.gauge);
		release_batch(batches.sum);
		release_batch(batches.histogram);
		release_batch(batches.exp_histogram);
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

OtlpIngestResult OtlpServer::SealOnce() {
	std::lock_guard<std::mutex> writer_lock(writer_mutex);
	auto db = db_ptr.lock();
	if (!db) {
		return OtlpIngestResult {}; // database closed; nothing we can durably write
	}
	auto &allocator = Allocator::Get(*db);

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

	try {
		RunSQL(*writer_con, "BEGIN TRANSACTION");
		for (idx_t i = 0; i < signal_buffers.size(); i++) {
			auto &collection = *sealing[i].collection;
			if (sealing[i].rows == 0) {
				continue;
			}
			auto &buf = *signal_buffers[i];
			auto appender =
			    config.catalog_name.empty()
			        ? make_uniq<Appender>(*writer_con, config.schema_name, buf.table_name)
			        : make_uniq<Appender>(*writer_con, config.catalog_name, config.schema_name, buf.table_name);
			ColumnDataScanState scan;
			collection.InitializeScan(scan);
			DataChunk chunk;
			collection.InitializeScanChunk(chunk);
			while (collection.Scan(scan, chunk)) {
				appender->AppendDataChunk(chunk);
				result.batches++;
			}
			appender->Close();
			result.rows += sealing[i].rows;
		}
		RunSQL(*writer_con, "COMMIT");
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
		LogServerEvent(StringUtil::Format("seal failed: %s", msg));
		throw;
	}

	ReleaseAdmission(sealed_admission_bytes);
	seals_total.fetch_add(1);
	last_seal_unix_ms.store(NowUnixMs());
	{
		std::lock_guard<std::mutex> elock(seal_error_mutex);
		seal_last_error.clear();
	}
	LogServerEvent(StringUtil::Format("seal: catalog=%s rows=%llu batches=%llu", config.catalog_name,
	                                  static_cast<uint64_t>(result.rows), static_cast<uint64_t>(result.batches)));
	return result;
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
				SealOnce();
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
	return SealOnce();
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
	// Final drain. On the explicit otlp_stop / ~OtlpServer path the listener+workers
	// are already joined and the controlling thread still holds the database alive, so
	// SealOnce can write and nothing is lost. On the implicit DB-teardown path
	// (~OtlpStorageExtensionInfo) db_ptr is already expired, so SealOnce is a no-op and
	// the buffered rows are dropped — callers must otlp_stop / otlp_flush before closing
	// the database to guarantee a seal (see Known Limitations).
	for (int attempt = 0; attempt < 3 && BufferedRows() > 0; attempt++) {
		try {
			SealOnce();
		} catch (...) {
			LogServerEvent("final seal attempt failed during shutdown");
		}
	}
	auto remaining_rows = BufferedRows();
	if (remaining_rows > 0) {
		LogServerEvent(
		    StringUtil::Format("dropping %llu buffered rows on shutdown (database closed without otlp_stop/otlp_flush, "
		                       "or repeated seal failure)",
		                       static_cast<uint64_t>(remaining_rows)));
	}
	writer_con.reset();
}

#ifndef __EMSCRIPTEN__

class OtlpServer::Impl {
public:
	unique_ptr<duckdb_httplib::Server> server;
};

static void SetJson(duckdb_httplib::Response &res, int status, const string &json) {
	res.status = status;
	res.set_content(json, "application/json");
}

static void SetError(duckdb_httplib::Response &res, int status, const string &reason, const string &message,
                     int retry_after_seconds = 0) {
	if (retry_after_seconds > 0) {
		res.set_header("Retry-After", std::to_string(retry_after_seconds));
	}
	SetJson(res, status, "{\"error\":\"" + JsonEscape(reason) + "\",\"message\":\"" + JsonEscape(message) + "\"}");
}

OtlpServer::OtlpServer(ClientContext &context, const OtlpUri &uri_p, const OtlpServerConfig &config_p)
    : db_ptr(context.db), uri(uri_p), config(config_p), impl(make_uniq<Impl>()) {
	ValidateToken(config.token);
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	EnsureTargetTables();
	InitBuffers();
	// One dedicated writer connection; the single sealer thread is the only thing
	// that ever commits, so concurrent DuckLake writes never conflict.
	writer_con = make_uniq<Connection>(*db);
	writer_con->context->config.enable_progress_bar = false;
	StartSealer();

	impl->server = make_uniq<duckdb_httplib::Server>();
	// Wide worker pool (mirrors duckdb-quack): each keep-alive connection holds a
	// worker thread for its lifetime, so we need enough threads to serve many
	// concurrent exporters at once. Workers only parse/convert and buffer rows in
	// memory (lock-free apart from a brief buffer append); a single background sealer
	// group-commits the buffer, so concurrent exporters never contend on the writer.
	impl->server->new_task_queue = [] {
		return new duckdb_httplib::ThreadPool(128);
	};
	impl->server->set_keep_alive_max_count(128);
	impl->server->set_keep_alive_timeout(10);
	impl->server->set_tcp_nodelay(true);
	impl->server->set_payload_max_length(static_cast<size_t>(config.max_body_bytes));

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
					// 202 Accepted: rows are parsed + buffered in memory, not yet durable.
					// They commit at the next seal (otlp_flush / otlp_stop force one).
					auto response =
					    StringUtil::Format("{\"status\":\"buffered\",\"rows\":%llu,\"batches\":%llu",
					                       static_cast<uint64_t>(result.rows), static_cast<uint64_t>(result.batches));
					if (kind == OtlpRequestKind::METRICS || result.HasSkipped()) {
						response += StringUtil::Format(
						    ",\"skipped\":{\"summaries\":%llu,\"nan_values\":%llu,\"infinity_values\":%llu,"
						    "\"missing_values\":%llu}",
						    static_cast<uint64_t>(result.skipped_summaries),
						    static_cast<uint64_t>(result.skipped_nan_values),
						    static_cast<uint64_t>(result.skipped_infinity_values),
						    static_cast<uint64_t>(result.skipped_missing_values));
					}
					response += "}";
					SetJson(res, 202, response);
				}
			} catch (OtlpHttpError &ex) {
				SetError(res, ex.status, "request_failed", ex.what(), ex.retry_after_seconds);
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

	// The sealer thread is already running. If bind fails (e.g. EADDRINUSE) this object
	// is not fully constructed, so ~OtlpServer() won't run — stop the sealer here so a
	// joinable std::thread is never destroyed (which would std::terminate the process).
	try {
		if (!impl->server->is_valid()) {
			throw IOException("Failed to instantiate OTLP HTTP server at %s / %s", uri.Uri(), uri.Http());
		}
		// Bind synchronously here so that bind() failures (e.g. EADDRINUSE) propagate
		// to the caller of otlp_serve() rather than being lost on the listener thread.
		if (!impl->server->bind_to_port(uri.Host(), uri.Port())) {
			throw IOException(
			    "Failed to bind OTLP HTTP server to %s (address in use, permission denied, or invalid host/"
			    "port)",
			    uri.Http());
		}
	} catch (...) {
		ShutdownIngest();
		throw;
	}
	is_running.store(true);
	listen_threads.emplace_back(ListenThread, this);
}

void OtlpServer::StopAccepting() {
	// Closes the listening socket only. Idempotent. Safe to call from a
	// request-handler thread — does not wait on httplib's task queue.
	if (is_running.exchange(false)) {
		impl->server->stop();
	}
}

void OtlpServer::Close() {
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
	// Workers are now joined: stop the sealer and drain the remaining buffer before
	// the writer connection / database go away. Safe here (controlling thread, not a
	// worker); idempotent with the ~OtlpServer() safety-net call.
	ShutdownIngest();
}

OtlpServer::~OtlpServer() {
	try {
		Close();
	} catch (std::exception &) {
	}
}

void OtlpServer::ListenThread(OtlpServer *server) {
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

class OtlpServer::Impl {};

OtlpServer::OtlpServer(ClientContext &context, const OtlpUri &uri_p, const OtlpServerConfig &config_p)
    : db_ptr(context.db), uri(uri_p), config(config_p), impl(make_uniq<Impl>()) {
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
}

void OtlpServer::StopAccepting() {
}

void OtlpServer::Close() {
}

OtlpServer::~OtlpServer() {
}

void OtlpServer::ListenThread(OtlpServer *server) {
}

#endif

} // namespace duckdb
