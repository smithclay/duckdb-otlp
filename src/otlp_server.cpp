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

#include <chrono>

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
	if (!writer_con) {
		return OtlpIngestResult {};
	}
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
		// Each buffered ColumnDataCollection is scanned chunk-by-chunk into the writer's
		// Appender, which re-buffers into its own collection before flushing (one extra
		// copy beyond the Arrow->DataChunk and DataChunk->buffer copies on the request
		// path). Kept deliberately: benchmarked (logs, in-memory) single-connection ingest
		// matches read-path throughput (~106 MB/s), so the DataChunk->buffer copy is
		// negligible next to Rust parse + Arrow->DataChunk conversion, and this seal runs
		// off the request path on the background sealer (sustained >300 MB/s aggregate over
		// 16 connections without becoming the limiter). The bottleneck is the essential
		// parse/convert work, which already scales with concurrency, so a more direct
		// collection->table insert is not worth the added complexity at v0.
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
	{
		std::lock_guard<std::mutex> writer_lock(writer_mutex);
		writer_con.reset();
	}
}

} // namespace duckdb
