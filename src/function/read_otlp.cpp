#include "function/read_otlp.hpp"
#include "schema/otlp_logs_schema.hpp"
#include "schema/otlp_metrics_schemas.hpp"
#include "schema/otlp_metrics_union_schema.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "parsers/format_detector.hpp"
#include "parsers/protobuf_parser.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/function/table_function.hpp"

#ifndef __EMSCRIPTEN__
#include <google/protobuf/io/zero_copy_stream_impl.h>
#endif

#include <cstring>
#include <vector>
#include <numeric>
#include <iterator>
#include <algorithm>
#include <utility>

namespace duckdb {

namespace {

static constexpr idx_t JSON_SNIFF_BYTES = 8192;
static constexpr idx_t STREAM_READ_BYTES = 64 * 1024;
static constexpr int64_t DEFAULT_MAX_DOCUMENT_BYTES = READ_OTLP_DEFAULT_MAX_DOCUMENT_BYTES;
static constexpr idx_t MAX_QUEUED_CHUNKS = 256;

struct OTLPScanStats {
	idx_t error_records = 0;
	idx_t error_documents = 0;
};

static mutex otlp_stats_mutex;
static unordered_map<connection_t, OTLPScanStats> otlp_latest_stats;

static connection_t GetStatsKey(ClientContext &context) {
	auto connection_id = context.GetConnectionId();
	if (connection_id != DConstants::INVALID_INDEX) {
		return connection_id;
	}
	return static_cast<connection_t>(reinterpret_cast<uintptr_t>(&context));
}

static void UpdateOTLPStats(ClientContext &context, const ReadOTLPGlobalState &state) {
	lock_guard<mutex> lock(otlp_stats_mutex);
	otlp_latest_stats[GetStatsKey(context)] = OTLPScanStats {state.error_records.load(), state.error_documents.load()};
}

static ReadOTLPOnError ParseOnErrorOption(named_parameter_map_t &named_parameters) {
	const auto entry = named_parameters.find("on_error");
	if (entry == named_parameters.end()) {
		return ReadOTLPOnError::FAIL;
	}
	const auto &value = entry->second;
	if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_otlp on_error must be one of 'fail', 'skip', or 'nullify'");
	}
	auto option = StringUtil::Lower(value.GetValue<string>());
	if (option == "fail") {
		return ReadOTLPOnError::FAIL;
	}
	if (option == "skip") {
		return ReadOTLPOnError::SKIP;
	}
	if (option == "nullify") {
		return ReadOTLPOnError::NULLIFY;
	}
	throw BinderException("read_otlp on_error must be one of 'fail', 'skip', or 'nullify'");
}

static int64_t ParseMaxDocumentBytes(named_parameter_map_t &named_parameters) {
	const auto entry = named_parameters.find("max_document_bytes");
	if (entry == named_parameters.end()) {
		return DEFAULT_MAX_DOCUMENT_BYTES;
	}
	const auto &value = entry->second;
	if (value.IsNull()) {
		return DEFAULT_MAX_DOCUMENT_BYTES;
	}
	if (value.type().id() != LogicalTypeId::BIGINT) {
		throw BinderException("read_otlp max_document_bytes must be a BIGINT");
	}
	auto limit = value.GetValue<int64_t>();
	if (limit <= 0) {
		throw BinderException("read_otlp max_document_bytes must be greater than 0");
	}
	return limit;
}

static bool ReadLine(FileHandle &file_handle, string &buffer, idx_t &buffer_offset, string &line) {
	line.clear();

	while (true) {
		if (buffer_offset >= buffer.size()) {
			buffer.resize(STREAM_READ_BYTES);
			idx_t bytes_read = file_handle.Read((void *)buffer.data(), STREAM_READ_BYTES);
			if (bytes_read == 0) {
				return !line.empty();
			}
			buffer.resize(bytes_read);
			buffer_offset = 0;
		}

		for (idx_t i = buffer_offset; i < buffer.size(); i++) {
			if (buffer[i] == '\n') {
				line.append(buffer.data() + buffer_offset, i - buffer_offset);
				buffer_offset = i + 1;
				return true;
			}
		}

		line.append(buffer.data() + buffer_offset, buffer.size() - buffer_offset);
		buffer_offset = buffer.size();
	}
}

static string ReadSample(FileHandle &handle, idx_t max_bytes) {
	string sample;
	sample.resize(max_bytes);
	idx_t read_bytes = handle.Read((void *)sample.data(), max_bytes);
	sample.resize(read_bytes);
	return sample;
}

static string ReadEntireFile(FileHandle &handle, const string &path, int64_t limit_bytes) {
	string contents;
	vector<char> buffer(STREAM_READ_BYTES);
	while (true) {
		idx_t read_bytes = handle.Read(buffer.data(), buffer.size());
		if (read_bytes == 0) {
			break;
		}
		if (limit_bytes >= 0 && static_cast<int64_t>(contents.size() + read_bytes) > limit_bytes) {
			throw IOException("Input file '%s' exceeds maximum supported size of %lld bytes", path.c_str(),
			                  static_cast<long long>(limit_bytes));
		}
		contents.append(buffer.data(), buffer.data() + read_bytes);
	}
	return contents;
}

static void ResetLocalFileState(ReadOTLPLocalState &state) {
	state.current_handle.reset();
	state.current_format = OTLPFormat::UNKNOWN;
	state.is_json_lines = false;
	state.doc_consumed = false;
	state.current_path.clear();
	state.line_buffer.clear();
	state.buffer_offset = 0;
	state.current_line = 0;
	state.approx_line = 0;
	state.chunk_queue.clear();
	state.active_chunk.reset();
}

static bool DetectJsonLinesFromSample(string sample) {
	if (sample.empty()) {
		return false;
	}
	OTLPJSONParser parser;
	StringUtil::Trim(sample);
	if (sample.empty()) {
		return false;
	}

	idx_t objects_on_separate_lines = 0;
	idx_t position = 0;
	while (position < sample.size()) {
		idx_t line_end = sample.find_first_of("\r\n", position);
		idx_t length = (line_end == string::npos) ? sample.size() - position : line_end - position;
		string line = sample.substr(position, length);
		StringUtil::Trim(line);
		if (!line.empty()) {
			if ((line.front() == '{' || line.front() == '[') && parser.IsValidOTLPJSON(line) &&
			    parser.DetectSignalType(line) != OTLPJSONParser::SignalType::UNKNOWN) {
				objects_on_separate_lines++;
			}
		}
		if (line_end == string::npos) {
			break;
		}
		position = line_end + 1;
		if (line_end + 1 < sample.size() && sample[line_end] == '\r' && sample[line_end + 1] == '\n') {
			position++;
		}
		if (objects_on_separate_lines >= 2) {
			return true;
		}
	}
	return objects_on_separate_lines >= 2;
}

#ifndef __EMSCRIPTEN__
class FileHandleCopyingInputStream : public google::protobuf::io::CopyingInputStream {
public:
	FileHandleCopyingInputStream(FileHandle &handle, int64_t byte_limit_p)
	    : handle(handle), byte_limit(byte_limit_p), bytes_read(0) {
	}

	int Read(void *buffer, int size) override {
		if (size <= 0) {
			return 0;
		}
		if (byte_limit >= 0) {
			if (bytes_read >= byte_limit) {
				return 0;
			}
			size = static_cast<int>(MinValue<int64_t>(byte_limit - bytes_read, size));
		}
		idx_t read_count = handle.Read(buffer, size);
		bytes_read += read_count;
		return static_cast<int>(read_count);
	}

	int Skip(int count) override {
		vector<char> temp(STREAM_READ_BYTES);
		int total_skipped = 0;
		while (total_skipped < count) {
			int to_read = MinValue<int>(count - total_skipped, static_cast<int>(temp.size()));
			int read_now = Read(temp.data(), to_read);
			if (read_now == 0) {
				break;
			}
			total_skipped += read_now;
		}
		return total_skipped;
	}

	int64_t BytesRead() const {
		return bytes_read;
	}

private:
	FileHandle &handle;
	int64_t byte_limit;
	int64_t bytes_read;
};
#endif

static const vector<LogicalType> &GetColumnTypesForTable(OTLPTableType table_type) {
	switch (table_type) {
	case OTLPTableType::TRACES: {
		static const vector<LogicalType> types = OTLPTracesSchema::GetColumnTypes();
		return types;
	}
	case OTLPTableType::LOGS: {
		static const vector<LogicalType> types = OTLPLogsSchema::GetColumnTypes();
		return types;
	}
	case OTLPTableType::METRICS_UNION: {
		static const vector<LogicalType> types = OTLPMetricsUnionSchema::GetColumnTypes();
		return types;
	}
	case OTLPTableType::METRICS_GAUGE: {
		static const vector<LogicalType> types = OTLPMetricsGaugeSchema::GetColumnTypes();
		return types;
	}
	case OTLPTableType::METRICS_SUM: {
		static const vector<LogicalType> types = OTLPMetricsSumSchema::GetColumnTypes();
		return types;
	}
	case OTLPTableType::METRICS_HISTOGRAM: {
		static const vector<LogicalType> types = OTLPMetricsHistogramSchema::GetColumnTypes();
		return types;
	}
	case OTLPTableType::METRICS_EXP_HISTOGRAM: {
		static const vector<LogicalType> types = OTLPMetricsExpHistogramSchema::GetColumnTypes();
		return types;
	}
	case OTLPTableType::METRICS_SUMMARY: {
		static const vector<LogicalType> types = OTLPMetricsSummarySchema::GetColumnTypes();
		return types;
	}
	default:
		throw InternalException("Unsupported table type for OTLP column lookup");
	}
}

static void InitializeProjection(ReadOTLPGlobalState &state, const vector<column_t> &requested_ids) {
	const auto &all_types = state.all_types;
	vector<column_t> effective_ids;
	if (requested_ids.empty()) {
		effective_ids.resize(all_types.size());
		std::iota(effective_ids.begin(), effective_ids.end(), 0);
	} else {
		effective_ids = requested_ids;
	}

	state.output_columns.clear();
	state.chunk_column_ids.clear();
	state.chunk_types.clear();

	unordered_map<column_t, idx_t> chunk_index_map;

	for (auto col_id : effective_ids) {
		ReadOTLPGlobalState::OutputColumnInfo info;
		info.requested_id = col_id;
		if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
			info.is_row_id = true;
			info.chunk_index = DConstants::INVALID_INDEX;
		} else {
			info.is_row_id = false;
			auto existing = chunk_index_map.find(col_id);
			if (existing == chunk_index_map.end()) {
				idx_t new_index = state.chunk_column_ids.size();
				chunk_index_map.emplace(col_id, new_index);
				state.chunk_column_ids.emplace_back(col_id);
				if (col_id >= all_types.size()) {
					throw BinderException("Projection references column index %d outside range", col_id);
				}
				state.chunk_types.emplace_back(all_types[col_id]);
				info.chunk_index = new_index;
			} else {
				info.chunk_index = existing->second;
			}
		}
		state.output_columns.push_back(info);
	}

	if (state.output_columns.empty()) {
		// Fallback to full schema if nothing selected (should not happen)
		for (idx_t col_idx = 0; col_idx < all_types.size(); col_idx++) {
			ReadOTLPGlobalState::OutputColumnInfo info;
			info.requested_id = static_cast<column_t>(col_idx);
			info.is_row_id = false;
			auto existing = chunk_index_map.find(col_idx);
			if (existing == chunk_index_map.end()) {
				idx_t new_index = state.chunk_column_ids.size();
				chunk_index_map.emplace(col_idx, new_index);
				state.chunk_column_ids.emplace_back(col_idx);
				state.chunk_types.emplace_back(all_types[col_idx]);
				info.chunk_index = new_index;
			} else {
				info.chunk_index = existing->second;
			}
			state.output_columns.push_back(info);
		}
	}
}

// Forward declaration needed by MakeNullRow
static string MetricTypeToString(OTLPMetricType filter);

static vector<Value> MakeNullRow(const ReadOTLPGlobalState &state) {
	if (state.metric_filter) {
		const auto &union_types = OTLPMetricsUnionSchema::GetColumnTypes();
		vector<Value> row;
		row.reserve(union_types.size());
		for (auto &type : union_types) {
			row.emplace_back(Value(type));
		}
		row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value(MetricTypeToString(*state.metric_filter));
		return row;
	}
	vector<Value> row;
	row.reserve(state.all_types.size());
	for (auto &type : state.all_types) {
		row.emplace_back(Value(type));
	}
	return row;
}

static bool HandleParseError(ClientContext &context, ReadOTLPGlobalState &gstate, ReadOTLPLocalState &lstate,
                             const string &message, const string &error, vector<vector<Value>> &rows,
                             bool is_document) {
	if (is_document) {
		gstate.error_documents.fetch_add(1);
	} else {
		gstate.error_records.fetch_add(1);
	}
	UpdateOTLPStats(context, gstate);
	switch (gstate.on_error) {
	case ReadOTLPOnError::FAIL: {
		string full_message = message;
		if (!error.empty()) {
			full_message += ": " + error;
		}
		throw IOException(full_message);
	}
	case ReadOTLPOnError::SKIP:
		return false;
	case ReadOTLPOnError::NULLIFY:
		rows.emplace_back(MakeNullRow(gstate));
		return true;
	default:
		throw InternalException("Unhandled on_error mode");
	}
}

static bool HandleParseErrorRecord(ClientContext &context, ReadOTLPGlobalState &gstate, ReadOTLPLocalState &lstate,
                                   const string &message, const string &error, vector<vector<Value>> &rows) {
	return HandleParseError(context, gstate, lstate, message, error, rows, false);
}

static bool HandleParseErrorDocument(ClientContext &context, ReadOTLPGlobalState &gstate, ReadOTLPLocalState &lstate,
                                     const string &message, const string &error, vector<vector<Value>> &rows) {
	return HandleParseError(context, gstate, lstate, message, error, rows, true);
}

static bool RowPassesFilters(const TableFilterSet *filters, const vector<Value> &row) {
	if (!filters) {
		return true;
	}
	for (auto &entry : filters->filters) {
		auto base_idx = entry.first;
		if (base_idx >= row.size()) {
			continue;
		}
		auto &filter = *entry.second;
		const auto &val = row[base_idx];
		switch (filter.filter_type) {
		case TableFilterType::IS_NULL:
			if (!val.IsNull()) {
				return false;
			}
			break;
		case TableFilterType::IS_NOT_NULL:
			if (val.IsNull()) {
				return false;
			}
			break;
		case TableFilterType::CONSTANT_COMPARISON: {
			auto &cf = filter.Cast<ConstantFilter>();
			if (val.IsNull() || cf.constant.IsNull()) {
				if (cf.comparison_type == ExpressionType::COMPARE_EQUAL ||
				    cf.comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
					if (!(val.IsNull() && cf.constant.IsNull())) {
						return false;
					}
				} else if (cf.comparison_type == ExpressionType::COMPARE_NOTEQUAL ||
				           cf.comparison_type == ExpressionType::COMPARE_DISTINCT_FROM) {
					if (val.IsNull() != cf.constant.IsNull()) {
						continue;
					}
					return false;
				}
				continue;
			}
			bool match = true;
			switch (cf.comparison_type) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				match = ValueOperations::Equals(val, cf.constant);
				break;
			case ExpressionType::COMPARE_NOTEQUAL:
			case ExpressionType::COMPARE_DISTINCT_FROM:
				match = ValueOperations::NotEquals(val, cf.constant);
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
				match = ValueOperations::GreaterThan(val, cf.constant);
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				match = ValueOperations::GreaterThanEquals(val, cf.constant);
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				match = ValueOperations::LessThan(val, cf.constant);
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				match = ValueOperations::LessThanEquals(val, cf.constant);
				break;
			default:
				match = true;
				break;
			}
			if (!match) {
				return false;
			}
			break;
		}
		default:
			break;
		}
	}
	return true;
}

static std::optional<OTLPMetricType> MetricTypeFromString(const string &metric_type) {
	if (metric_type == "gauge") {
		return OTLPMetricType::GAUGE;
	}
	if (metric_type == "sum") {
		return OTLPMetricType::SUM;
	}
	if (metric_type == "histogram") {
		return OTLPMetricType::HISTOGRAM;
	}
	if (metric_type == "exp_histogram" || metric_type == "exponential_histogram") {
		return OTLPMetricType::EXPONENTIAL_HISTOGRAM;
	}
	if (metric_type == "summary") {
		return OTLPMetricType::SUMMARY;
	}
	return std::nullopt;
}

static string MetricTypeToString(OTLPMetricType filter) {
	switch (filter) {
	case OTLPMetricType::GAUGE:
		return "gauge";
	case OTLPMetricType::SUM:
		return "sum";
	case OTLPMetricType::HISTOGRAM:
		return "histogram";
	case OTLPMetricType::EXPONENTIAL_HISTOGRAM:
		return "exponential_histogram";
	case OTLPMetricType::SUMMARY:
		return "summary";
	default:
		throw InternalException("Unhandled metric filter");
	}
}

static bool RowMatchesMetricFilter(const vector<Value> &row, OTLPMetricType filter) {
	if (row.size() <= OTLPMetricsUnionSchema::COL_METRIC_TYPE) {
		return false;
	}
	const auto &metric_val = row[OTLPMetricsUnionSchema::COL_METRIC_TYPE];
	if (metric_val.IsNull()) {
		return false;
	}
	auto metric_opt = MetricTypeFromString(metric_val.ToString());
	return metric_opt && *metric_opt == filter;
}

static vector<Value> ProjectMetricRow(OTLPMetricType filter, const vector<Value> &row) {
	switch (filter) {
	case OTLPMetricType::GAUGE: {
		vector<Value> result;
		result.reserve(OTLPMetricsGaugeSchema::COLUMN_COUNT);
		for (idx_t idx = 0; idx < OTLPMetricsBaseSchema::BASE_COLUMN_COUNT; idx++) {
			result.emplace_back(row[idx]);
		}
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_VALUE]);
		return result;
	}
	case OTLPMetricType::SUM: {
		vector<Value> result;
		result.reserve(OTLPMetricsSumSchema::COLUMN_COUNT);
		for (idx_t idx = 0; idx < OTLPMetricsBaseSchema::BASE_COLUMN_COUNT; idx++) {
			result.emplace_back(row[idx]);
		}
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_VALUE]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC]);
		return result;
	}
	case OTLPMetricType::HISTOGRAM: {
		vector<Value> result;
		result.reserve(OTLPMetricsHistogramSchema::COLUMN_COUNT);
		for (idx_t idx = 0; idx < OTLPMetricsBaseSchema::BASE_COLUMN_COUNT; idx++) {
			result.emplace_back(row[idx]);
		}
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_COUNT]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_SUM]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_MIN]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_MAX]);
		return result;
	}
	case OTLPMetricType::EXPONENTIAL_HISTOGRAM: {
		vector<Value> result;
		result.reserve(OTLPMetricsExpHistogramSchema::COLUMN_COUNT);
		for (idx_t idx = 0; idx < OTLPMetricsBaseSchema::BASE_COLUMN_COUNT; idx++) {
			result.emplace_back(row[idx]);
		}
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_COUNT]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_SUM]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_SCALE]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_ZERO_COUNT]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_MIN]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_MAX]);
		return result;
	}
	case OTLPMetricType::SUMMARY: {
		vector<Value> result;
		result.reserve(OTLPMetricsSummarySchema::COLUMN_COUNT);
		for (idx_t idx = 0; idx < OTLPMetricsBaseSchema::BASE_COLUMN_COUNT; idx++) {
			result.emplace_back(row[idx]);
		}
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_COUNT]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_SUM]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES]);
		result.emplace_back(row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES]);
		return result;
	}
	default:
		throw InternalException("Unsupported metric filter for projection");
	}
}

static void FinishCurrentFile(ReadOTLPGlobalState &gstate, ReadOTLPLocalState &lstate) {
	if (lstate.current_handle) {
		lstate.current_handle.reset();
		gstate.active_workers.fetch_sub(1);
	}
	lstate.current_format = OTLPFormat::UNKNOWN;
	lstate.is_json_lines = false;
	lstate.doc_consumed = false;
	lstate.current_path.clear();
	lstate.line_buffer.clear();
	lstate.buffer_offset = 0;
	lstate.current_line = 0;
	lstate.approx_line = 0;
}

static bool EmitChunk(ClientContext &context, ReadOTLPGlobalState &gstate, ReadOTLPLocalState &lstate,
                      DataChunk &output) {
	while (!lstate.chunk_queue.empty()) {
		auto chunk = std::move(lstate.chunk_queue.front());
		lstate.chunk_queue.pop_front();
		if (!chunk || chunk->size() == 0) {
			continue;
		}
		idx_t emit_count = chunk->size();
		for (idx_t col_idx = 0; col_idx < gstate.output_columns.size(); col_idx++) {
			auto &info = gstate.output_columns[col_idx];
			auto &vec = output.data[col_idx];
			if (info.is_row_id) {
				vec.SetVectorType(VectorType::FLAT_VECTOR);
				auto data = FlatVector::GetData<int64_t>(vec);
				auto base = gstate.next_row_id.fetch_add(emit_count);
				for (idx_t i = 0; i < emit_count; i++) {
					data[i] = base + static_cast<int64_t>(i);
				}
			} else {
				vec.Reference(chunk->data[info.chunk_index]);
			}
		}
		output.SetCardinality(emit_count);
		lstate.active_chunk = std::move(chunk);
		return true;
	}
	lstate.active_chunk.reset();
	output.SetCardinality(0);
	return false;
}

static bool AcquireNextFile(ClientContext &context, ReadOTLPGlobalState &gstate, ReadOTLPLocalState &lstate) {
	auto &fs = FileSystem::GetFileSystem(context);
	while (true) {
		auto file_idx = gstate.next_file.fetch_add(1);
		if (file_idx >= gstate.files.size()) {
			return false;
		}
		const auto &path = gstate.files[file_idx];
		ResetLocalFileState(lstate);
		lstate.current_path = path;
		auto handle = fs.OpenFile(path, FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ));
		auto sample = ReadSample(*handle, JSON_SNIFF_BYTES);
		auto format = FormatDetector::DetectFormat(sample.data(), sample.size());
		if (format == OTLPFormat::UNKNOWN) {
			handle.reset();
			throw IOException("Unable to detect OTLP format (expected JSON or Protobuf) in file: %s", path.c_str());
		}
		bool json_lines = false;
		if (format == OTLPFormat::JSON) {
			json_lines = DetectJsonLinesFromSample(sample);
			auto lower_path = StringUtil::Lower(path);
			if (!json_lines &&
			    (StringUtil::EndsWith(lower_path, ".jsonl") || StringUtil::EndsWith(lower_path, ".ndjson"))) {
				json_lines = true;
			}
			if (!lstate.json_parser) {
				lstate.json_parser = make_uniq<OTLPJSONParser>();
			}
		}
#ifndef __EMSCRIPTEN__
		else {
			if (!lstate.protobuf_parser) {
				lstate.protobuf_parser = make_uniq<OTLPProtobufParser>();
			}
		}
#endif
		if (handle->CanSeek()) {
			handle->Seek(0);
		} else {
			handle.reset();
			handle = fs.OpenFile(path, FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ));
		}
		lstate.current_handle = std::move(handle);
		lstate.current_format = format;
		lstate.is_json_lines = json_lines;
		gstate.active_workers.fetch_add(1);
		return true;
	}
}

static void EnqueueRows(ClientContext &context, ReadOTLPGlobalState &gstate, ReadOTLPLocalState &lstate,
                        vector<vector<Value>> &&rows) {
	if (rows.empty()) {
		return;
	}
	vector<vector<Value>> filtered_rows;
	filtered_rows.reserve(rows.size());
	if (gstate.metric_filter) {
		for (auto &row : rows) {
			if (!RowMatchesMetricFilter(row, *gstate.metric_filter)) {
				continue;
			}
			auto projected = ProjectMetricRow(*gstate.metric_filter, row);
			if (!gstate.filters || RowPassesFilters(gstate.filters.get(), projected)) {
				filtered_rows.emplace_back(std::move(projected));
			}
		}
	} else {
		for (auto &row : rows) {
			if (!gstate.filters || RowPassesFilters(gstate.filters.get(), row)) {
				filtered_rows.emplace_back(std::move(row));
			}
		}
	}
	if (filtered_rows.empty()) {
		return;
	}
	auto &processed_rows = filtered_rows;
	auto &allocator = Allocator::Get(context);
	idx_t position = 0;
	while (position < processed_rows.size()) {
		idx_t emit_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, processed_rows.size() - position);
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(allocator, gstate.chunk_types);
		for (idx_t col_idx = 0; col_idx < gstate.chunk_column_ids.size(); col_idx++) {
			auto &vec = chunk->data[col_idx];
			vec.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t row_idx = 0; row_idx < emit_count; row_idx++) {
			auto &row = processed_rows[position + row_idx];
			for (idx_t col_idx = 0; col_idx < gstate.chunk_column_ids.size(); col_idx++) {
				auto source_idx = gstate.chunk_column_ids[col_idx];
				chunk->data[col_idx].SetValue(row_idx, row[source_idx]);
			}
		}
		chunk->SetCardinality(emit_count);
		if (lstate.chunk_queue.size() >= MAX_QUEUED_CHUNKS) {
			throw IOException("OTLP chunk queue overflow - input produced too many buffered chunks");
		}
		lstate.chunk_queue.push_back(std::move(chunk));
		position += emit_count;
	}
}

struct OTLPStatsBindData : public TableFunctionData {
	idx_t error_records;
	idx_t error_documents;
	bool emitted;
	OTLPStatsBindData(idx_t records, idx_t documents)
	    : error_records(records), error_documents(documents), emitted(false) {
	}
};

static unique_ptr<FunctionData> OTLPStatsBind(ClientContext &context, TableFunctionBindInput &,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	OTLPScanStats stats;
	{
		lock_guard<mutex> lock(otlp_stats_mutex);
		auto it = otlp_latest_stats.find(GetStatsKey(context));
		if (it != otlp_latest_stats.end()) {
			stats = it->second;
		}
	}
	return_types = {LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"error_records", "error_documents"};
	return make_uniq<OTLPStatsBindData>(stats.error_records, stats.error_documents);
}

struct OTLPStatsGlobalState : public GlobalTableFunctionState {
	bool done;
	OTLPStatsGlobalState() : done(false) {
	}
};

static unique_ptr<GlobalTableFunctionState> OTLPStatsInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<OTLPStatsGlobalState>();
}

static void OTLPStatsScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<OTLPStatsBindData>();
	auto &gstate = data.global_state->Cast<OTLPStatsGlobalState>();
	if (gstate.done) {
		return;
	}
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value::BIGINT(bind.error_records));
	output.data[1].SetValue(0, Value::BIGINT(bind.error_documents));
	gstate.done = true;
}

struct OTLPOptionsBindData : public TableFunctionData {};

struct OTLPOptionsGlobalState : public GlobalTableFunctionState {
	bool emitted = false;
};

static unique_ptr<FunctionData> OTLPOptionsBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                                vector<string> &names) {
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"option_name", "allowed_values", "default_value", "description"};
	return make_uniq<OTLPOptionsBindData>();
}

static unique_ptr<GlobalTableFunctionState> OTLPOptionsInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<OTLPOptionsGlobalState>();
}

static void OTLPOptionsScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<OTLPOptionsGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}

	output.SetCardinality(3);

	for (idx_t col = 0; col < output.ColumnCount(); col++) {
		output.data[col].SetVectorType(VectorType::FLAT_VECTOR);
	}

	const string option_names[] = {"on_error", "max_document_bytes", "read_otlp_scan_stats"};
	const string allowed_values[] = {"fail | skip | nullify", "> 0", "SELECT * FROM read_otlp_scan_stats()"};
	const string default_values[] = {"fail", std::to_string(READ_OTLP_DEFAULT_MAX_DOCUMENT_BYTES), "n/a"};
	const string descriptions[] = {
	    "Controls how read_otlp_* handles parse failures: fail (default), skip row, or emit NULL columns.",
	    "Maximum bytes buffered per file for JSON or protobuf documents before aborting (default 100 MB).",
	    "Expose counters from the most recent read_otlp_* scan in the current connection."};

	for (idx_t row = 0; row < 3; row++) {
		FlatVector::GetData<string_t>(output.data[0])[row] = StringVector::AddString(output.data[0], option_names[row]);
		FlatVector::GetData<string_t>(output.data[1])[row] =
		    StringVector::AddString(output.data[1], allowed_values[row]);
		FlatVector::GetData<string_t>(output.data[2])[row] =
		    StringVector::AddString(output.data[2], default_values[row]);
		FlatVector::GetData<string_t>(output.data[3])[row] = StringVector::AddString(output.data[3], descriptions[row]);
	}

	state.emitted = true;
}

static unique_ptr<FunctionData> BindTraces(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_traces requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPTracesSchema::GetColumnTypes();
	names = OTLPTracesSchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::TRACES);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	return bind;
}

static unique_ptr<FunctionData> BindLogs(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_logs requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPLogsSchema::GetColumnTypes();
	names = OTLPLogsSchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::LOGS);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	return bind;
}

static unique_ptr<FunctionData> BindMetrics(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_metrics requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPMetricsUnionSchema::GetColumnTypes();
	names = OTLPMetricsUnionSchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_UNION);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	return bind;
}

static unique_ptr<FunctionData> BindMetricsGauge(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_metrics_gauge requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPMetricsGaugeSchema::GetColumnTypes();
	names = OTLPMetricsGaugeSchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_GAUGE);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	bind->metric_filter = OTLPMetricType::GAUGE;
	return bind;
}

static unique_ptr<FunctionData> BindMetricsSum(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_metrics_sum requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPMetricsSumSchema::GetColumnTypes();
	names = OTLPMetricsSumSchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_SUM);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	bind->metric_filter = OTLPMetricType::SUM;
	return bind;
}

static unique_ptr<FunctionData> BindMetricsHistogram(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_metrics_histogram requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPMetricsHistogramSchema::GetColumnTypes();
	names = OTLPMetricsHistogramSchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_HISTOGRAM);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	bind->metric_filter = OTLPMetricType::HISTOGRAM;
	return bind;
}

static unique_ptr<FunctionData> BindMetricsExpHistogram(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_metrics_exp_histogram requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPMetricsExpHistogramSchema::GetColumnTypes();
	names = OTLPMetricsExpHistogramSchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_EXP_HISTOGRAM);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	bind->metric_filter = OTLPMetricType::EXPONENTIAL_HISTOGRAM;
	return bind;
}

static unique_ptr<FunctionData> BindMetricsSummary(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_metrics_summary requires exactly one argument (file path)");
	}
	auto file_path = input.inputs[0].ToString();
	return_types = OTLPMetricsSummarySchema::GetColumnTypes();
	names = OTLPMetricsSummarySchema::GetColumnNames();
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_SUMMARY);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	bind->max_document_bytes = ParseMaxDocumentBytes(input.named_parameters);
	bind->metric_filter = OTLPMetricType::SUMMARY;
	return bind;
}

} // namespace

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &, TableFunctionInitInput &,
                                                     GlobalTableFunctionState *);

TableFunction ReadOTLPTableFunction::GetTracesFunction() {
	TableFunction func("read_otlp_traces", {LogicalType::VARCHAR}, Scan, BindTraces, Init);
	func.name = "read_otlp_traces";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetLogsFunction() {
	TableFunction func("read_otlp_logs", {LogicalType::VARCHAR}, Scan, BindLogs, Init);
	func.name = "read_otlp_logs";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsFunction() {
	TableFunction func("read_otlp_metrics", {LogicalType::VARCHAR}, Scan, BindMetrics, Init);
	func.name = "read_otlp_metrics";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsGaugeFunction() {
	TableFunction func("read_otlp_metrics_gauge", {LogicalType::VARCHAR}, Scan, BindMetricsGauge, Init);
	func.name = "read_otlp_metrics_gauge";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsSumFunction() {
	TableFunction func("read_otlp_metrics_sum", {LogicalType::VARCHAR}, Scan, BindMetricsSum, Init);
	func.name = "read_otlp_metrics_sum";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsHistogramFunction() {
	TableFunction func("read_otlp_metrics_histogram", {LogicalType::VARCHAR}, Scan, BindMetricsHistogram, Init);
	func.name = "read_otlp_metrics_histogram";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsExpHistogramFunction() {
	TableFunction func("read_otlp_metrics_exp_histogram", {LogicalType::VARCHAR}, Scan, BindMetricsExpHistogram, Init);
	func.name = "read_otlp_metrics_exp_histogram";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsSummaryFunction() {
	TableFunction func("read_otlp_metrics_summary", {LogicalType::VARCHAR}, Scan, BindMetricsSummary, Init);
	func.name = "read_otlp_metrics_summary";
	func.init_local = InitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	func.named_parameters["on_error"] = LogicalType::VARCHAR;
	func.named_parameters["max_document_bytes"] = LogicalType::BIGINT;
	return func;
}

TableFunction ReadOTLPTableFunction::GetStatsFunction() {
	TableFunction func("read_otlp_scan_stats", {}, OTLPStatsScan, OTLPStatsBind, OTLPStatsInit);
	func.name = "read_otlp_scan_stats";
	return func;
}

TableFunction ReadOTLPTableFunction::GetOptionsFunction() {
	TableFunction func("read_otlp_options", {}, OTLPOptionsScan, OTLPOptionsBind, OTLPOptionsInit);
	func.name = "read_otlp_options";
	return func;
}

unique_ptr<GlobalTableFunctionState> ReadOTLPTableFunction::Init(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadOTLPBindData>();
	auto state = make_uniq<ReadOTLPGlobalState>();
	state->table_type = bind_data.table_type;
	state->all_types = GetColumnTypesForTable(state->table_type);
	state->on_error = bind_data.on_error;
	state->next_file.store(0);
	state->next_row_id.store(0);
	state->error_records.store(0);
	state->error_documents.store(0);
	state->active_workers.store(0);
	state->stats_reported.store(false);
	state->metric_filter = bind_data.metric_filter;
	state->max_document_bytes = bind_data.max_document_bytes;
	InitializeProjection(*state, input.column_ids);
	if (input.filters) {
		state->filters = make_shared_ptr<TableFilterSet>();
		for (auto &entry : input.filters->filters) {
			column_t base_idx = entry.first;
			if (!input.column_ids.empty() && entry.first < input.column_ids.size()) {
				base_idx = input.column_ids[entry.first];
			}
			state->filters->filters.emplace(base_idx, entry.second->Copy());
		}
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto matches = fs.GlobFiles(bind_data.pattern, context, FileGlobOptions::DISALLOW_EMPTY);
	for (auto &match : matches) {
		state->files.emplace_back(match.path);
	}

	if (state->files.empty()) {
		throw IOException("No files matched pattern '%s'", bind_data.pattern.c_str());
	}

	return state;
}

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &, TableFunctionInitInput &,
                                                     GlobalTableFunctionState *) {
	return make_uniq<ReadOTLPLocalState>();
}

void ReadOTLPTableFunction::Scan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ReadOTLPGlobalState>();
	auto &lstate = data.local_state->Cast<ReadOTLPLocalState>();
	lstate.context = &context;
	lstate.active_chunk.reset();
	output.SetCardinality(0);

	while (true) {
		if (EmitChunk(context, gstate, lstate, output)) {
			return;
		}

		if (!lstate.current_handle) {
			if (!AcquireNextFile(context, gstate, lstate)) {
				if (!lstate.reported_completion) {
					if (gstate.active_workers.load() == 0 && !gstate.stats_reported.exchange(true)) {
						UpdateOTLPStats(context, gstate);
					}
					lstate.reported_completion = true;
				}
				return;
			}
			lstate.reported_completion = false;
			continue;
		}

		if (lstate.current_format == OTLPFormat::JSON) {
			if (lstate.is_json_lines) {
				string line;
				if (!ReadLine(*lstate.current_handle, lstate.line_buffer, lstate.buffer_offset, line)) {
					FinishCurrentFile(gstate, lstate);
					continue;
				}
				lstate.approx_line++;
				StringUtil::Trim(line);
				if (line.empty()) {
					continue;
				}
				vector<vector<Value>> parsed_rows;
				bool success = false;
				switch (gstate.table_type) {
				case OTLPTableType::TRACES:
					success = lstate.json_parser->ParseTracesToTypedRows(line, parsed_rows);
					break;
				case OTLPTableType::LOGS:
					success = lstate.json_parser->ParseLogsToTypedRows(line, parsed_rows);
					break;
				case OTLPTableType::METRICS_UNION:
				case OTLPTableType::METRICS_GAUGE:
				case OTLPTableType::METRICS_SUM:
				case OTLPTableType::METRICS_HISTOGRAM:
				case OTLPTableType::METRICS_EXP_HISTOGRAM:
				case OTLPTableType::METRICS_SUMMARY:
					success = lstate.json_parser->ParseMetricsToTypedRows(line, parsed_rows);
					break;
				default:
					throw IOException("Unsupported table type for JSON parsing");
				}
				if (!success) {
					auto error = lstate.json_parser->GetLastError();
					if (!HandleParseErrorRecord(
					        context, gstate, lstate,
					        StringUtil::Format("Failed to parse OTLP JSON data in file '%s' on line (approx) %llu",
					                           lstate.current_path.c_str(), static_cast<long long>(lstate.approx_line)),
					        error, parsed_rows)) {
						continue;
					}
				}
				if (parsed_rows.empty()) {
					continue;
				}
				EnqueueRows(context, gstate, lstate, std::move(parsed_rows));
				continue;
			}

			auto contents = ReadEntireFile(*lstate.current_handle, lstate.current_path, gstate.max_document_bytes);
			vector<vector<Value>> parsed_rows;
			bool success = false;
			switch (gstate.table_type) {
			case OTLPTableType::TRACES:
				success = lstate.json_parser->ParseTracesToTypedRows(contents, parsed_rows);
				break;
			case OTLPTableType::LOGS:
				success = lstate.json_parser->ParseLogsToTypedRows(contents, parsed_rows);
				break;
			case OTLPTableType::METRICS_UNION:
			case OTLPTableType::METRICS_GAUGE:
			case OTLPTableType::METRICS_SUM:
			case OTLPTableType::METRICS_HISTOGRAM:
			case OTLPTableType::METRICS_EXP_HISTOGRAM:
			case OTLPTableType::METRICS_SUMMARY:
				success = lstate.json_parser->ParseMetricsToTypedRows(contents, parsed_rows);
				break;
			default:
				throw IOException("Unsupported table type for JSON parsing");
			}
			if (!success || parsed_rows.empty()) {
				auto error = success ? string("Produced no rows") : lstate.json_parser->GetLastError();
				if (!HandleParseErrorDocument(
				        context, gstate, lstate,
				        StringUtil::Format("Failed to parse OTLP JSON data in file '%s'", lstate.current_path.c_str()),
				        error, parsed_rows)) {
					FinishCurrentFile(gstate, lstate);
					continue;
				}
			}
			if (parsed_rows.empty()) {
				FinishCurrentFile(gstate, lstate);
				continue;
			}
			EnqueueRows(context, gstate, lstate, std::move(parsed_rows));
			FinishCurrentFile(gstate, lstate);
			continue;
		}

#ifndef __EMSCRIPTEN__
		if (lstate.current_format == OTLPFormat::PROTOBUF) {
			vector<vector<Value>> parsed_rows;
			FileHandleCopyingInputStream proto_stream(*lstate.current_handle, gstate.max_document_bytes);
			google::protobuf::io::CopyingInputStreamAdaptor adaptor(&proto_stream);
			adaptor.SetOwnsCopyingStream(false);
			idx_t row_count = 0;
			switch (gstate.table_type) {
			case OTLPTableType::TRACES:
				row_count = lstate.protobuf_parser->ParseTracesToTypedRows(adaptor, parsed_rows);
				break;
			case OTLPTableType::LOGS:
				row_count = lstate.protobuf_parser->ParseLogsToTypedRows(adaptor, parsed_rows);
				break;
			case OTLPTableType::METRICS_UNION:
			case OTLPTableType::METRICS_GAUGE:
			case OTLPTableType::METRICS_SUM:
			case OTLPTableType::METRICS_HISTOGRAM:
			case OTLPTableType::METRICS_EXP_HISTOGRAM:
			case OTLPTableType::METRICS_SUMMARY:
				row_count = lstate.protobuf_parser->ParseMetricsToTypedRows(adaptor, parsed_rows);
				break;
			default:
				throw IOException("Unsupported table type for protobuf parsing");
			}
			if (row_count == 0 || parsed_rows.empty()) {
				auto error = lstate.protobuf_parser->GetLastError();
				if (error.empty() && gstate.max_document_bytes > 0 &&
				    proto_stream.BytesRead() >= gstate.max_document_bytes) {
					error = StringUtil::Format("Protobuf file exceeds maximum supported size of %lld bytes",
					                           static_cast<long long>(gstate.max_document_bytes));
				}
				if (!HandleParseErrorDocument(context, gstate, lstate,
				                              StringUtil::Format("Failed to parse OTLP protobuf data in file '%s'",
				                                                 lstate.current_path.c_str()),
				                              error, parsed_rows)) {
					FinishCurrentFile(gstate, lstate);
					continue;
				}
			}
			if (parsed_rows.empty()) {
				FinishCurrentFile(gstate, lstate);
				continue;
			}
			EnqueueRows(context, gstate, lstate, std::move(parsed_rows));
			FinishCurrentFile(gstate, lstate);
			continue;
		}

#endif
		throw IOException("Unsupported OTLP format detected for file '%s'", lstate.current_path.c_str());
	}
}

} // namespace duckdb
