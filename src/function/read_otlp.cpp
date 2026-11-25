/**
 * @file read_otlp.cpp
 * @brief DuckDB table functions using Rust otlp2records backend
 *
 * This file implements read_otlp_logs, read_otlp_traces, etc. Each is a single-file reader over the
 * Rust otlp2records library, wrapped by DuckDB's multi-file framework.
 */

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include "otlp_arrow.hpp"
#include "otlp_function_docs.hpp"
#include "otlp_ingest_limits.hpp"

#include "otlp2records.h"

namespace duckdb {

// otlp_otap_decoder_free(nullptr) is a documented no-op.
struct OtapDecoderDeleter {
	void operator()(OtlpOtapDecoder *decoder) const noexcept {
		otlp_otap_decoder_free(decoder);
	}
};
using OtapDecoderPtr = unique_ptr<OtlpOtapDecoder, OtapDecoderDeleter>;

//! What one read_ot* function reads: a signal, and whether its files are OTAP.
struct ReadOtlpInfo : public TableFunctionInfo {
	ReadOtlpInfo(OtlpSignalType signal_type_p, bool is_otap_p) : signal_type(signal_type_p), is_otap(is_otap_p) {
	}
	OtlpSignalType signal_type;
	bool is_otap;

	//! The signal's columns, resolved on first use and shared by every file of every scan: a multi-file
	//! scan binds once per file, and the schema never depends on the file.
	shared_ptr<const OtlpArrowTable> GetTable(ClientContext &context) {
		lock_guard<mutex> guard(table_lock);
		if (!table) {
			table = shared_ptr<const OtlpArrowTable>(OtlpArrowTable::ForSignal(context, signal_type));
		}
		return table;
	}

private:
	mutex table_lock;
	shared_ptr<const OtlpArrowTable> table;
};

//! A function registered only to point callers elsewhere: the error it raises.
struct ReadOtlpUnsupportedInfo : public TableFunctionInfo {
	explicit ReadOtlpUnsupportedInfo(string message_p) : message(std::move(message_p)) {
	}
	string message;
};

//! Bind data of the single-file reader: one file of a multi-file scan. Globbing, file lists and the
//! multi-file options (filename, hive_partitioning, union_by_name, ...) belong to DuckDB's multi-file
//! framework, which binds this once per file.
struct ReadOtlpFileBindData : public TableFunctionData {
	OpenFileInfo file;
	OtlpSignalType signal_type = OTLP_SIGNAL_LOGS;
	// read_otap_*: decoded with the stateful OTAP decoder; same output schema as OTLP.
	bool is_otap = false;
	shared_ptr<const OtlpArrowTable> table;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ReadOtlpFileBindData>();
		result->column_ids = column_ids;
		result->file = file;
		result->signal_type = signal_type;
		result->is_otap = is_otap;
		result->table = table;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReadOtlpFileBindData>();
		return file.path == other.file.path && signal_type == other.signal_type && is_otap == other.is_otap;
	}
};

//! What a file's scan asks otlp2records to build: only the columns it reads, and only the rows that can
//! pass its filters. The row predicates prune; the scan still applies every filter itself.
class OtlpReadRequest {
public:
	explicit OtlpReadRequest(ClientContext &context_p) : context(context_p) {
	}

	//! The signal columns to build, in the order the decoded batch holds them.
	vector<uint32_t> columns;
	vector<OtlpTimeRange> time_ranges;
	//! The filters on service_name. otlp2records asks about each resource's service, and they decide it
	//! exactly: a service_name is the same for every row of a resource.
	vector<reference<const ExpressionFilter>> service_filters;

	//! Whether no row can pass, so the file need not be decoded at all.
	bool Impossible() const {
		for (auto &range : time_ranges) {
			if (range.min > range.max) {
				return true;
			}
		}
		return false;
	}

	//! The FFI view of this request, valid while it is alive and unchanged.
	OtlpReadOptions Options() {
		// A non-NULL list even when empty: NULL would ask for every column.
		static const uint32_t NO_COLUMNS = 0;
		OtlpReadOptions options {};
		options.columns = columns.empty() ? &NO_COLUMNS : columns.data();
		options.columns_len = columns.size();
		if (!service_filters.empty()) {
			options.keep_service = KeepService;
			options.keep_service_data = this;
		}
		options.time_ranges = time_ranges.empty() ? nullptr : time_ranges.data();
		options.time_ranges_len = time_ranges.size();
		return options;
	}

private:
	//! The OtlpServiceFilter otlp2records calls once per resource, on this thread, while decoding.
	static bool KeepService(void *data, const uint8_t *name, size_t len) {
		auto &request = *static_cast<OtlpReadRequest *>(data);
		string service(const_char_ptr_cast(name), len);
		auto entry = request.decisions.find(service);
		if (entry != request.decisions.end()) {
			return entry->second;
		}
		bool keep = true;
		try {
			Value value(service);
			for (auto &filter : request.service_filters) {
				if (!filter.get().EvaluateWithConstant(request.context, value)) {
					keep = false;
					break;
				}
			}
		} catch (...) {
			// Nothing may unwind into Rust. Keeping the rows is always correct: the scan filters them anyway.
			keep = true;
		}
		request.decisions.emplace(std::move(service), keep);
		return keep;
	}

	ClientContext &context;
	//! Each service's answer, since many resources share one.
	unordered_map<string, bool> decisions;
};

//! A pushed-down filter, applied to the output column at `position`.
struct ReadOtlpScanFilter {
	idx_t position;
	unique_ptr<TableFilterState> state;
};

//! One file's scan. The function has no local state, so the multi-file framework gives each file to
//! a single thread and parallelizes across files.
struct ReadOtlpFileGlobalState : public GlobalTableFunctionState {
	explicit ReadOtlpFileGlobalState(ClientContext &context) : request(context) {
	}

	OtlpReadRequest request;
	//! Every filter DuckDB pushed into the scan; the scan must apply them exactly.
	vector<ReadOtlpScanFilter> filters;
	//! The decoded batch's columns (request.columns, in order), as the Arrow importer types them.
	arrow_column_map_t arrow_columns;
	//! The batch column each output column converts from; COLUMN_IDENTIFIER_ROW_ID (which the Arrow
	//! importer skips) where it carries file_row_number instead.
	vector<column_t> column_ids;
	//! Output columns that carry file_row_number (a virtual column of the multi-file scan).
	vector<idx_t> row_number_positions;
	bool decoded = false;
	//! The decoded file; null once emitted. Output vectors may reference its buffers (zero-copy) and
	//! hold their own reference, so dropping this one never frees memory in use.
	shared_ptr<ArrowArrayWrapper> batch;
	idx_t row_count = 0;
	idx_t offset = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> ReadOtlpUnsupportedBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	throw NotImplementedException(input.info->Cast<ReadOtlpUnsupportedInfo>().message);
}

static void ReadOtlpUnsupportedScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	throw InternalException("an unsupported read_otlp_* reader was scanned");
}

static unique_ptr<FunctionData> ReadOtlpFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &info = input.info->Cast<ReadOtlpInfo>();
	auto result = make_uniq<ReadOtlpFileBindData>();
	result->file = TableFunctionFileBindInput::GetFile(input);
	result->signal_type = info.signal_type;
	result->is_otap = info.is_otap;
	result->table = info.GetTable(context);
	return_types = result->table->types;
	for (auto &name : result->table->names) {
		names.emplace_back(name);
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Filter pushdown: what otlp2records can prune
//===--------------------------------------------------------------------===//
// A pushed filter is an expression over its column (a bound reference). These find the part of it that
// limits a timestamp column to a range, which otlp2records prunes rows by. The answer is a superset of
// the rows the filter keeps, so pruning by it is safe; anything not understood restricts nothing.

//! `column <op> constant` (either side): the constant, and the comparison as seen from the column.
static optional_ptr<const BoundConstantExpression> ColumnComparison(const Expression &expr, ExpressionType &type) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION ||
	    !BoundComparisonExpression::IsComparison(expr)) {
		return nullptr;
	}
	auto constant = ExpressionFilter::TryGetColumnConstantComparison(expr.Cast<BoundFunctionExpression>(), type);
	if (!constant || constant->GetValue().IsNull()) {
		return nullptr;
	}
	return constant;
}

//! `column IN (constants)`: the non-NULL constants (a NULL never matches).
static bool ColumnInList(const Expression &expr, vector<Value> &values) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
	    expr.GetExpressionType() != ExpressionType::COMPARE_IN) {
		return false;
	}
	auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
	if (children.empty() || !ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
		return false;
	}
	for (idx_t i = 1; i < children.size(); i++) {
		if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &value = children[i]->Cast<BoundConstantExpression>().GetValue();
		if (!value.IsNull()) {
			values.push_back(value);
		}
	}
	return true;
}

static bool IsConjunction(const Expression &expr, ExpressionType type) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION && expr.GetExpressionType() == type;
}

struct NanosecondRange {
	int64_t min = NumericLimits<int64_t>::Minimum();
	int64_t max = NumericLimits<int64_t>::Maximum();
};

static bool TimestampNs(const Value &value, int64_t &result) {
	if (value.type().id() != LogicalTypeId::TIMESTAMP_NS) {
		return false;
	}
	result = value.GetValue<timestamp_ns_t>().value;
	return true;
}

//! The inclusive range `expr` limits a TIMESTAMP_NS column to, if it limits it.
static bool RestrictTime(const Expression &expr, NanosecondRange &result) {
	ExpressionType type;
	int64_t value;
	if (auto constant = ColumnComparison(expr, type)) {
		if (!TimestampNs(constant->GetValue(), value)) {
			return false;
		}
		result = NanosecondRange();
		switch (type) {
		case ExpressionType::COMPARE_EQUAL:
			result.min = result.max = value;
			return true;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			result.min = value;
			return true;
		case ExpressionType::COMPARE_GREATERTHAN:
			if (value == NumericLimits<int64_t>::Maximum()) {
				return false;
			}
			result.min = value + 1;
			return true;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			result.max = value;
			return true;
		case ExpressionType::COMPARE_LESSTHAN:
			if (value == NumericLimits<int64_t>::Minimum()) {
				return false;
			}
			result.max = value - 1;
			return true;
		default:
			return false;
		}
	}
	vector<Value> values;
	if (ColumnInList(expr, values)) {
		// The hull of the list: a superset of its values.
		result = NanosecondRange();
		result.min = NumericLimits<int64_t>::Maximum();
		result.max = NumericLimits<int64_t>::Minimum();
		for (auto &entry : values) {
			if (!TimestampNs(entry, value)) {
				return false;
			}
			result.min = MinValue(result.min, value);
			result.max = MaxValue(result.max, value);
		}
		return true;
	}
	if (IsConjunction(expr, ExpressionType::CONJUNCTION_OR)) {
		bool first = true;
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			NanosecondRange branch;
			if (!RestrictTime(*child, branch)) {
				return false;
			}
			result.min = first ? branch.min : MinValue(result.min, branch.min);
			result.max = first ? branch.max : MaxValue(result.max, branch.max);
			first = false;
		}
		return !first;
	}
	if (IsConjunction(expr, ExpressionType::CONJUNCTION_AND)) {
		bool restricted = false;
		result = NanosecondRange();
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			NanosecondRange branch;
			if (!RestrictTime(*child, branch)) {
				continue;
			}
			result.min = MaxValue(result.min, branch.min);
			result.max = MinValue(result.max, branch.max);
			restricted = true;
		}
		return restricted;
	}
	return false;
}

//! Narrows `request` by what `filter` on signal column `column_id` limits.
static void AddFilterToRequest(const OtlpArrowTable &table, column_t column_id, const TableFilter &filter,
                               OtlpReadRequest &request) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return;
	}
	auto &expression_filter = filter.Cast<ExpressionFilter>();
	if (table.names[column_id] == "service_name") {
		request.service_filters.push_back(expression_filter);
		return;
	}
	if (table.types[column_id].id() != LogicalTypeId::TIMESTAMP_NS) {
		return;
	}
	optional_ptr<const Expression> root = expression_filter.expr.get();
	if (ExpressionFilter::IsRootOptionalFilter(filter)) {
		// A hint the plan also enforces above the scan (an IN list next to the OR it came from, a join's
		// key range): pruning by it drops only rows that would be dropped anyway.
		root = ExpressionFilter::GetOptionalFilterChild(*root);
		if (!root) {
			return;
		}
	}
	NanosecondRange range;
	if (RestrictTime(*root, range)) {
		request.time_ranges.push_back({static_cast<uint32_t>(column_id), range.min, range.max});
	}
}

static unique_ptr<GlobalTableFunctionState> ReadOtlpFileInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadOtlpFileBindData>();
	auto &file_input = TableFunctionFileInitInput::Get(input);
	auto result = make_uniq<ReadOtlpFileGlobalState>(context);
	auto &table_columns = bind_data.table->arrow_table.GetColumns();
	for (idx_t position = 0; position < input.column_ids.size(); position++) {
		auto column_id = input.column_ids[position];
		// A multi-file scan projects a virtual column under an index of its own and says which it is.
		if (file_input.virtual_columns) {
			auto entry = file_input.virtual_columns->find(column_id);
			if (entry != file_input.virtual_columns->end()) {
				column_id = entry->second;
			}
		}
		if (column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			result->row_number_positions.push_back(position);
			result->column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
			continue;
		}
		if (IsVirtualColumn(column_id)) {
			throw InternalException("read_otlp: unsupported virtual column %llu", static_cast<uint64_t>(column_id));
		}
		// otlp2records returns exactly the requested columns, in this order.
		auto batch_column = result->request.columns.size();
		result->request.columns.push_back(NumericCast<uint32_t>(column_id));
		result->arrow_columns.emplace(batch_column, table_columns.at(column_id));
		result->column_ids.push_back(batch_column);
	}
	if (input.filters) {
		if (file_input.expression_map && !file_input.expression_map->empty()) {
			// Every file of a signal has the same columns, so the multi-file scan never casts one.
			throw InternalException("read_otlp: a filter needs its column converted first");
		}
		for (auto &entry : *input.filters) {
			auto position = entry.GetIndex().GetIndex();
			auto &filter = entry.Filter();
			result->filters.push_back({position, TableFilterState::Initialize(context, filter)});
			auto batch_column = result->column_ids[position];
			if (batch_column != COLUMN_IDENTIFIER_ROW_ID) {
				AddFilterToRequest(*bind_data.table, result->request.columns[batch_column], filter, result->request);
			}
		}
	}
	return std::move(result);
}

static virtual_column_map_t ReadOtlpFileVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	virtual_column_map_t result;
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	return result;
}

// The returned schema is released here (conversion uses the bind-time types); the caller owns the array.
static ArrowArray DecodeFileToArray(const ReadOtlpFileBindData &bind_data, OtlpReadRequest &request,
                                    const uint8_t *input_bytes, idx_t input_len, const string &path) {
	auto options = request.Options();
	ArrowArray array;
	ArrowSchema schema;
	OtlpStatus status;
	if (bind_data.is_otap) {
		// One decoder per file: a file must be a self-contained message (no cross-file dictionary reuse).
		OtapDecoderPtr decoder(otlp_otap_decoder_new_with_profile(OTLP_ARROW_PROFILE_DUCKDB));
		if (!decoder) {
			throw IOException("Failed to allocate OTAP decoder for %s", path);
		}
		status = otlp_otap_decode_with_options(decoder.get(), bind_data.signal_type, &options, input_bytes, input_len,
		                                       &array, &schema);
		if (status != OTLP_OK) {
			throw IOException("OTAP decode error on %s: %s", path, otlp_status_message(status));
		}
	} else {
		// A metric signal builds only its own shape; a file without it is an empty batch.
		status = otlp_transform_with_options(bind_data.signal_type, OTLP_FORMAT_AUTO, OTLP_ARROW_PROFILE_DUCKDB,
		                                     &options, input_bytes, input_len, &array, &schema);
		if (status != OTLP_OK) {
			throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
		}
	}
	if (schema.release) {
		schema.release(&schema);
	}
	return array;
}

//! Reads and decodes the whole file into one ArrowArray (null if no row of it is wanted).
static shared_ptr<ArrowArrayWrapper> DecodeFile(ClientContext &context, const ReadOtlpFileBindData &bind_data,
                                                OtlpReadRequest &request) {
	if (request.Impossible()) {
		return nullptr;
	}
	auto &path = bind_data.file.path;
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(bind_data.file, FileOpenFlags::FILE_FLAGS_READ);

	// The whole file is read and decoded into one in-memory ArrowArray, hence the size cap.
	auto file_size = handle->GetFileSize();
	constexpr idx_t MAX_FILE_SIZE = otlp_limits::MAX_READ_FILE_BYTES;
	if (file_size > MAX_FILE_SIZE) {
		throw IOException("File %s is too large (%llu bytes). Maximum supported size is 100MB.", path,
		                  static_cast<uint64_t>(file_size));
	}
	auto buffer = Allocator::Get(context).Allocate(file_size);
	auto bytes_read = handle->Read(buffer.get(), file_size);
	if (bytes_read != static_cast<idx_t>(file_size)) {
		throw IOException("Short read on file %s: expected %lld bytes, got %llu", path, static_cast<int64_t>(file_size),
		                  static_cast<uint64_t>(bytes_read));
	}

	ArrowArray array = DecodeFileToArray(bind_data, request, buffer.get(), file_size, path);
	if (array.length < 0) {
		if (array.release) {
			array.release(&array);
		}
		throw IOException("Invalid Arrow batch: negative row count");
	}
	if (!array.release) {
		return nullptr;
	}
	if (array.length == 0) {
		array.release(&array);
		return nullptr;
	}
	return OtlpOwnArrowBatch(array);
}

//! Converts the next rows of the decoded file into `output`; returns how many.
static idx_t ReadOtlpFileEmit(ClientContext &context, const ReadOtlpFileBindData &bind_data,
                              ReadOtlpFileGlobalState &gstate, DataChunk &output) {
	const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, gstate.row_count - gstate.offset);
	OtlpArrowScan(context, gstate.arrow_columns, gstate.batch, gstate.column_ids, gstate.offset, count, output);
	for (auto position : gstate.row_number_positions) {
		output.data[position].Sequence(static_cast<int64_t>(gstate.offset), 1, count);
	}
	gstate.offset += count;
	if (gstate.offset >= gstate.row_count) {
		gstate.batch.reset();
	}
	return count;
}

//! Applies every pushed-down filter to `output` exactly; returns the rows left.
static idx_t ReadOtlpFileFilter(ReadOtlpFileGlobalState &gstate, DataChunk &output, idx_t count) {
	if (gstate.filters.empty()) {
		return count;
	}
	SelectionVector sel;
	sel.Initialize(nullptr);
	idx_t approved = count;
	for (auto &filter : gstate.filters) {
		ColumnSegment::FilterSelection(sel, output.data[filter.position], *filter.state, count, approved);
		if (approved == 0) {
			break;
		}
	}
	if (approved != count && approved != 0) {
		output.Slice(sel, approved);
	}
	return approved;
}

static void ReadOtlpFileScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<ReadOtlpFileBindData>();
	auto &gstate = data.global_state->Cast<ReadOtlpFileGlobalState>();
	if (!gstate.decoded) {
		gstate.batch = DecodeFile(context, bind_data, gstate.request);
		gstate.row_count = gstate.batch ? static_cast<idx_t>(gstate.batch->arrow_array.length) : 0;
		gstate.decoded = true;
	}
	// An empty chunk ends this file's scan, so keep going until a slice keeps a row.
	while (gstate.batch) {
		auto count = ReadOtlpFileEmit(context, bind_data, gstate, output);
		if (ReadOtlpFileFilter(gstate, output, count) > 0) {
			return;
		}
		output.Reset();
	}
	output.SetChildCardinality(0);
}

static double ReadOtlpFileProgress(ClientContext &context, const FunctionData *bind_data,
                                   const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0;
	}
	auto &gstate = global_state->Cast<ReadOtlpFileGlobalState>();
	if (!gstate.decoded) {
		return 0;
	}
	if (gstate.row_count == 0) {
		return 100;
	}
	return 100.0 * static_cast<double>(gstate.offset) / static_cast<double>(gstate.row_count);
}

//! read_<signal>(path) over one file, wrapped by DuckDB's multi-file framework: `path` may be a glob,
//! a list of files, or a file struct, and every multi-file option is accepted.
static TableFunctionSet ReadOtlpFunctionSet(const string &name, shared_ptr<ReadOtlpInfo> info) {
	const bool is_otap = info->is_otap;
	TableFunction single_file(Identifier(name + "_file"),
	                          FunctionSignature().AddPositionalOnly("file", LogicalType::ANY), ReadOtlpFileScan,
	                          ReadOtlpFileBind, ReadOtlpFileInitGlobal);
	single_file.projection_pushdown = true;
	single_file.filter_pushdown = true;
	single_file.filter_prune = true;
	single_file.table_scan_progress = ReadOtlpFileProgress;
	single_file.get_virtual_columns = ReadOtlpFileVirtualColumns;
	single_file.function_info = std::move(info);
	TableFunctionMultiFileSettings settings;
	settings.reader_type = is_otap ? "OTAP" : "OTLP";
	return TableFunctionMultiFileWrapper::CreateFunctionSet(std::move(single_file), Identifier(name),
	                                                        std::move(settings));
}

void RegisterReadOTLPRustFunctions(ExtensionLoader &loader) {
	//! Per signal, in OTLP_SIGNALS order: what a row is, the column count, and any detail worth naming.
	struct SignalDoc {
		const char *rows;
		int columns;
		const char *detail;
	};
	const SignalDoc docs[] = {
	    {"log records", 18, ""},
	    {"trace spans", 24, ", including events, links and a computed duration"},
	    {"gauge data points", 17, ""},
	    {"sum (counter) data points", 19, " with aggregation_temporality and is_monotonic"},
	    {"explicit-bucket histogram data points", 22, " with bucket bounds and counts"},
	    {"exponential histogram data points", 27, " with scale, zero bucket and positive/negative buckets"},
	};
	auto register_reader = [&](const string &name, shared_ptr<ReadOtlpInfo> info, const string &description,
	                           const vector<string> &examples, const char *protocol) {
		auto set = ReadOtlpFunctionSet(name, std::move(info));
		// One description per overload DuckDB's multi-file set has (a path or glob, a list, a file struct),
		// matched on the type of its file parameter.
		vector<FunctionDescription> docs;
		for (auto &overload : set.functions) {
			auto &file = overload->GetSignature().GetParameter(0);
			docs.push_back(OtlpDoc({file.GetType()}, {file.GetName().GetIdentifierName()}, description, examples,
			                       {"opentelemetry", protocol}));
		}
		loader.RegisterFunction(OtlpDocumented(std::move(set), std::move(docs)));
	};
	//! Registered only to raise and point at the working readers.
	auto register_unsupported = [&](const string &name, string message, string description) {
		TableFunction func(Identifier(name), FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
		                   ReadOtlpUnsupportedScan, ReadOtlpUnsupportedBind);
		func.function_info = make_shared_ptr<ReadOtlpUnsupportedInfo>(std::move(message));
		loader.RegisterFunction(OtlpDocumented(
		    std::move(func),
		    {OtlpDoc({LogicalType::VARCHAR}, {"path"}, std::move(description), {}, {"opentelemetry", "otlp"})}));
	};
	for (auto signal_type : OTLP_SIGNALS) {
		const auto &doc = docs[signal_type];
		const string table = OTLP_SIGNAL_TABLES[signal_type];
		const string otlp_name = "read_" + table;
		const string otap_name = "read_otap_" + table.substr(strlen("otlp_"));
		const bool metrics = signal_type >= OTLP_SIGNAL_METRICS_GAUGE;
		string common;
		if (metrics) {
			common += " One file can hold several metric shapes; this reader takes its own and skips the rest, so "
			          "the four metric readers all read the same file.";
		}
		common += " `path` is a file, a glob, or a list of them, resolved through DuckDB's file systems (local, S3, "
		          "HTTP(S), Azure, GCS); the multi-file options (filename, hive_partitioning, union_by_name, ...) "
		          "apply.";
		const string file = metrics ? "metrics" : table.substr(strlen("otlp_"));
		register_reader(otlp_name, make_shared_ptr<ReadOtlpInfo>(signal_type, false),
		                StringUtil::Format("Read OpenTelemetry %s from OTLP JSON, NDJSON or protobuf files into a "
		                                   "flat %d-column table%s.%s",
		                                   doc.rows, doc.columns, doc.detail, common),
		                {StringUtil::Format("SELECT * FROM %s('%s.pb');", otlp_name, file)}, "otlp");
		register_reader(otap_name, make_shared_ptr<ReadOtlpInfo>(signal_type, true),
		                StringUtil::Format("Read OpenTelemetry %s from an OTAP (OpenTelemetry Arrow Protocol) "
		                                   "BatchArrowRecords file, returning the same %d columns as %s.%s",
		                                   doc.rows, doc.columns, otlp_name, common),
		                {StringUtil::Format("SELECT * FROM %s('%s.bar');", otap_name, file)}, "otap");
	}

	// Registered only to raise and point at the working readers; no examples, since every call raises.
	register_unsupported("read_otlp_metrics",
	                     "read_otlp_metrics() is not supported yet because OTLP metrics have multiple "
	                     "shape-specific schemas. Use read_otlp_metrics_gauge(), read_otlp_metrics_sum(), "
	                     "read_otlp_metrics_histogram(), or read_otlp_metrics_exp_histogram().",
	                     "Not implemented: every call raises. OTLP metrics have shape-specific schemas, so read one of "
	                     "read_otlp_metrics_gauge, read_otlp_metrics_sum, read_otlp_metrics_histogram or "
	                     "read_otlp_metrics_exp_histogram instead.");
	register_unsupported(
	    "read_otlp_metrics_summary",
	    "Summary metrics not yet supported. Use read_otlp_metrics_gauge() or read_otlp_metrics_sum() instead.",
	    "Not implemented: every call raises. OTLP summary data points are not decoded; the other "
	    "read_otlp_metrics_* readers count them as skipped.");
}

} // namespace duckdb
