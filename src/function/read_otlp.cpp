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

#include <google/protobuf/io/zero_copy_stream_impl.h>

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
static constexpr int64_t MAX_JSON_DOC_BYTES = static_cast<int64_t>(100) * 1024 * 1024;
static constexpr int64_t MAX_PROTO_BYTES = static_cast<int64_t>(100) * 1024 * 1024;

struct OTLPScanStats {
	idx_t error_records = 0;
	idx_t error_documents = 0;
};

static mutex otlp_stats_mutex;
static unordered_map<ClientContext *, OTLPScanStats> otlp_latest_stats;

static void UpdateOTLPStats(ClientContext &context, const ReadOTLPGlobalState &state) {
	lock_guard<mutex> lock(otlp_stats_mutex);
	otlp_latest_stats[&context] = OTLPScanStats {state.error_records, state.error_documents};
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

static void ResetFileState(ReadOTLPGlobalState &state) {
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
			if (line.front() == '{' || line.front() == '[') {
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
	case OTLPTableType::METRICS_GAUGE:
	case OTLPTableType::METRICS_SUM:
	case OTLPTableType::METRICS_HISTOGRAM:
	case OTLPTableType::METRICS_EXP_HISTOGRAM:
	case OTLPTableType::METRICS_SUMMARY: {
		static const vector<LogicalType> types = OTLPMetricsUnionSchema::GetColumnTypes();
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

static vector<Value> MakeNullRow(const ReadOTLPGlobalState &state) {
	vector<Value> row;
	row.reserve(state.all_types.size());
	for (auto &type : state.all_types) {
		row.emplace_back(Value(type));
	}
	return row;
}

static bool HandleParseError(ReadOTLPGlobalState &state, const string &message, const string &error,
                             vector<vector<Value>> &rows, bool is_document) {
	if (is_document) {
		state.error_documents++;
	} else {
		state.error_records++;
	}
	switch (state.on_error) {
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
		rows.emplace_back(MakeNullRow(state));
		return true;
	default:
		throw InternalException("Unhandled on_error mode");
	}
}

static bool HandleParseErrorRecord(ReadOTLPGlobalState &state, const string &message, const string &error,
                                   vector<vector<Value>> &rows) {
	return HandleParseError(state, message, error, rows, false);
}

static bool HandleParseErrorDocument(ReadOTLPGlobalState &state, const string &message, const string &error,
                                     vector<vector<Value>> &rows) {
	return HandleParseError(state, message, error, rows, true);
}

static bool RowPassesFilters(const ReadOTLPGlobalState &state, const vector<Value> &row) {
	if (!state.filters) {
		return true;
	}
	for (auto &entry : state.filters->filters) {
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

static void EnqueueRows(ClientContext &context, ReadOTLPGlobalState &state, vector<vector<Value>> &&rows) {
	if (rows.empty()) {
		return;
	}
	if (state.filters && !state.filters->filters.empty()) {
		vector<vector<Value>> filtered;
		filtered.reserve(rows.size());
		for (auto &row : rows) {
			if (RowPassesFilters(state, row)) {
				filtered.emplace_back(std::move(row));
			}
		}
		rows = std::move(filtered);
	}
	if (rows.empty()) {
		return;
	}
	auto &allocator = Allocator::Get(context);
	idx_t position = 0;
	while (position < rows.size()) {
		idx_t emit_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows.size() - position);
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(allocator, state.chunk_types);
		for (idx_t col_idx = 0; col_idx < state.chunk_column_ids.size(); col_idx++) {
			auto &vec = chunk->data[col_idx];
			vec.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t row_idx = 0; row_idx < emit_count; row_idx++) {
			auto &row = rows[position + row_idx];
			for (idx_t col_idx = 0; col_idx < state.chunk_column_ids.size(); col_idx++) {
				auto source_idx = state.chunk_column_ids[col_idx];
				chunk->data[col_idx].SetValue(row_idx, row[source_idx]);
			}
		}
		chunk->SetCardinality(emit_count);
		state.chunk_queue.push_back(std::move(chunk));
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
		auto it = otlp_latest_stats.find(&context);
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
	auto bind = make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_GAUGE);
	bind->on_error = ParseOnErrorOption(input.named_parameters);
	return bind;
}

static void OpenNextFile(ClientContext &context, ReadOTLPGlobalState &state) {
	if (state.next_file >= state.files.size()) {
		state.finished = true;
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	for (; state.next_file < state.files.size(); state.next_file++) {
		const auto &path = state.files[state.next_file];
		ResetFileState(state);
		state.current_path = path;

		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		auto sample = ReadSample(*handle, JSON_SNIFF_BYTES);
		state.current_format = FormatDetector::DetectFormat(sample.data(), sample.size());

		if (state.current_format == OTLPFormat::UNKNOWN) {
			handle.reset();
			throw IOException("Unable to detect OTLP format (expected JSON or Protobuf) in file: %s", path.c_str());
		}

		bool json_lines = false;
		if (state.current_format == OTLPFormat::JSON) {
			json_lines = DetectJsonLinesFromSample(sample);
			auto lower_path = StringUtil::Lower(path);
			if (!json_lines &&
			    (StringUtil::EndsWith(lower_path, ".jsonl") || StringUtil::EndsWith(lower_path, ".ndjson"))) {
				json_lines = true;
			}
			if (!state.json_parser) {
				state.json_parser = make_uniq<OTLPJSONParser>();
			}
		} else {
			if (!state.protobuf_parser) {
				state.protobuf_parser = make_uniq<OTLPProtobufParser>();
			}
		}

		if (handle->CanSeek()) {
			handle->Seek(0);
		} else {
			// reopen to get fresh stream
			handle.reset();
			handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		}

		state.current_handle = std::move(handle);
		state.is_json_lines = json_lines;
		state.next_file++;
		return;
	}
	state.finished = true;
}

} // namespace

TableFunction ReadOTLPTableFunction::GetTracesFunction() {
	TableFunction func("read_otlp_traces", {LogicalType::VARCHAR}, Scan, BindTraces, Init);
	func.name = "read_otlp_traces";
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	return func;
}

TableFunction ReadOTLPTableFunction::GetLogsFunction() {
	TableFunction func("read_otlp_logs", {LogicalType::VARCHAR}, Scan, BindLogs, Init);
	func.name = "read_otlp_logs";
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsFunction() {
	TableFunction func("read_otlp_metrics", {LogicalType::VARCHAR}, Scan, BindMetrics, Init);
	func.name = "read_otlp_metrics";
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	return func;
}

TableFunction ReadOTLPTableFunction::GetStatsFunction() {
	TableFunction func("read_otlp_scan_stats", {}, OTLPStatsScan, OTLPStatsBind, OTLPStatsInit);
	func.name = "read_otlp_scan_stats";
	return func;
}

unique_ptr<GlobalTableFunctionState> ReadOTLPTableFunction::Init(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadOTLPBindData>();
	auto state = make_uniq<ReadOTLPGlobalState>();
	state->table_type = bind_data.table_type;
	state->all_types = GetColumnTypesForTable(state->table_type);
	state->on_error = bind_data.on_error;
	InitializeProjection(*state, input.column_ids);
	if (input.filters) {
		state->filters = make_uniq<TableFilterSet>();
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

void ReadOTLPTableFunction::Scan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ReadOTLPGlobalState>();

	state.active_chunk.reset();
	output.SetCardinality(0);

	if (state.finished) {
		if (!state.stats_reported) {
			UpdateOTLPStats(context, state);
			state.stats_reported = true;
		}
		return;
	}

	while (true) {
		if (!state.chunk_queue.empty()) {
			auto chunk = std::move(state.chunk_queue.front());
			state.chunk_queue.pop_front();
			if (!chunk || chunk->size() == 0) {
				continue;
			}

			auto emit_count = chunk->size();
			for (idx_t col_idx = 0; col_idx < state.output_columns.size(); col_idx++) {
				auto &info = state.output_columns[col_idx];
				auto &vec = output.data[col_idx];
				if (info.is_row_id) {
					vec.SetVectorType(VectorType::FLAT_VECTOR);
					for (idx_t i = 0; i < emit_count; i++) {
						vec.SetValue(i, Value::BIGINT(state.row_id_base + i));
					}
				} else {
					vec.Reference(chunk->data[info.chunk_index]);
				}
			}

			output.SetCardinality(emit_count);
			state.row_id_base += emit_count;
			state.active_chunk = std::move(chunk);
			return;
		}

		if (!state.current_handle) {
			if (state.next_file >= state.files.size()) {
				state.finished = true;
				return;
			}
			OpenNextFile(context, state);
			if (state.finished) {
				return;
			}
			continue;
		}

		if (state.current_format == OTLPFormat::JSON) {
			if (state.is_json_lines) {
				string line;
				if (!ReadLine(*state.current_handle, state.line_buffer, state.buffer_offset, line)) {
					state.current_handle.reset();
					continue;
				}
				state.approx_line++;
				StringUtil::Trim(line);
				if (line.empty()) {
					continue;
				}
				vector<vector<Value>> parsed_rows;
				bool success = false;
				switch (state.table_type) {
				case OTLPTableType::TRACES:
					success = state.json_parser->ParseTracesToTypedRows(line, parsed_rows);
					break;
				case OTLPTableType::LOGS:
					success = state.json_parser->ParseLogsToTypedRows(line, parsed_rows);
					break;
				case OTLPTableType::METRICS_GAUGE:
					success = state.json_parser->ParseMetricsToTypedRows(line, parsed_rows);
					break;
				default:
					throw IOException("Unsupported table type for JSON parsing");
				}
				if (!success) {
					auto error = state.json_parser->GetLastError();
					if (!HandleParseErrorRecord(
					        state,
					        StringUtil::Format("Failed to parse OTLP JSON data in file '%s' on line (approx) %llu",
					                           state.current_path.c_str(), static_cast<long long>(state.approx_line)),
					        error, parsed_rows)) {
						continue;
					}
				}
				if (parsed_rows.empty()) {
					continue;
				}
				EnqueueRows(context, state, std::move(parsed_rows));
				continue;
			}

			if (state.doc_consumed) {
				state.current_handle.reset();
				continue;
			}
			auto contents = ReadEntireFile(*state.current_handle, state.current_path, MAX_JSON_DOC_BYTES);
			vector<vector<Value>> parsed_rows;
			bool success = false;
			switch (state.table_type) {
			case OTLPTableType::TRACES:
				success = state.json_parser->ParseTracesToTypedRows(contents, parsed_rows);
				break;
			case OTLPTableType::LOGS:
				success = state.json_parser->ParseLogsToTypedRows(contents, parsed_rows);
				break;
			case OTLPTableType::METRICS_GAUGE:
				success = state.json_parser->ParseMetricsToTypedRows(contents, parsed_rows);
				break;
			default:
				throw IOException("Unsupported table type for JSON parsing");
			}
			if (!success || parsed_rows.empty()) {
				auto error = success ? string("Produced no rows") : state.json_parser->GetLastError();
				if (!HandleParseErrorDocument(
				        state,
				        StringUtil::Format("Failed to parse OTLP JSON data in file '%s'", state.current_path.c_str()),
				        error, parsed_rows)) {
					state.doc_consumed = true;
					state.current_handle.reset();
					continue;
				}
			}
			if (parsed_rows.empty()) {
				state.doc_consumed = true;
				state.current_handle.reset();
				continue;
			}
			EnqueueRows(context, state, std::move(parsed_rows));
			state.doc_consumed = true;
			state.current_handle.reset();
			continue;
		}

		if (state.current_format == OTLPFormat::PROTOBUF) {
			if (state.doc_consumed) {
				state.current_handle.reset();
				continue;
			}
			vector<vector<Value>> parsed_rows;
			FileHandleCopyingInputStream proto_stream(*state.current_handle, MAX_PROTO_BYTES);
			google::protobuf::io::CopyingInputStreamAdaptor adaptor(&proto_stream);
			adaptor.SetOwnsCopyingStream(false);
			idx_t row_count = 0;
			switch (state.table_type) {
			case OTLPTableType::TRACES:
				row_count = state.protobuf_parser->ParseTracesToTypedRows(adaptor, parsed_rows);
				break;
			case OTLPTableType::LOGS:
				row_count = state.protobuf_parser->ParseLogsToTypedRows(adaptor, parsed_rows);
				break;
			case OTLPTableType::METRICS_GAUGE:
				row_count = state.protobuf_parser->ParseMetricsToTypedRows(adaptor, parsed_rows);
				break;
			default:
				throw IOException("Unsupported table type for protobuf parsing");
			}
			if (row_count == 0 || parsed_rows.empty()) {
				auto error = state.protobuf_parser->GetLastError();
				if (error.empty() && proto_stream.BytesRead() >= MAX_PROTO_BYTES) {
					error = StringUtil::Format("Protobuf file exceeds maximum supported size of %lld bytes",
					                           static_cast<long long>(MAX_PROTO_BYTES));
				}
				if (!HandleParseErrorDocument(state,
				                              StringUtil::Format("Failed to parse OTLP protobuf data in file '%s'",
				                                                 state.current_path.c_str()),
				                              error, parsed_rows)) {
					state.doc_consumed = true;
					state.current_handle.reset();
					continue;
				}
			}
			if (parsed_rows.empty()) {
				state.doc_consumed = true;
				state.current_handle.reset();
				continue;
			}
			EnqueueRows(context, state, std::move(parsed_rows));
			state.doc_consumed = true;
			state.current_handle.reset();
			continue;
		}

		throw IOException("Unsupported OTLP format detected for file '%s'", state.current_path.c_str());
	}
}

} // namespace duckdb
