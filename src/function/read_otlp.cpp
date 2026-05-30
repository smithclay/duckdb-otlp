/**
 * @file read_otlp_rust.cpp
 * @brief DuckDB table functions using Rust otlp2records backend
 *
 * This file implements read_otlp_logs, read_otlp_traces, etc.
 * table functions that use the Rust otlp2records library for parsing.
 *
 * Requires: -DOTLP_USE_RUST=ON at build time
 */

#ifdef OTLP_RUST_BACKEND

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "otlp_arrow.hpp"

// Include the Rust FFI header
#include "otlp2records.h"

namespace duckdb {

// ============================================================================
// Bind Data
// ============================================================================

struct ReadOTLPRustBindData : public TableFunctionData {
	vector<string> files;
	OtlpSignalType signal_type;
	OtlpInputFormat format;

	// Schema from Rust (cached at bind time)
	ArrowSchema arrow_schema;
	vector<LogicalType> return_types;
	vector<string> names;
	bool schema_initialized = false;

	~ReadOTLPRustBindData() override {
		if (schema_initialized && arrow_schema.release) {
			arrow_schema.release(&arrow_schema);
		}
	}
};

// ============================================================================
// Global State
// ============================================================================

struct ReadOTLPRustGlobalState : public GlobalTableFunctionState {
	mutex lock;
	atomic<idx_t> next_file {0};

	idx_t MaxThreads() const override {
		// Start single-threaded for prototype
		return 1;
	}
};

// ============================================================================
// Local State
// ============================================================================

struct ReadOTLPRustLocalState : public LocalTableFunctionState {
	ArrowArray current_batch;
	bool batch_active = false;
	idx_t batch_offset = 0;
	string file_buffer;
	vector<column_t> column_ids;

	~ReadOTLPRustLocalState() override {
		if (batch_active && current_batch.release) {
			current_batch.release(&current_batch);
		}
	}
};

static void CopyProjectedArrowStructToDataChunk(const ArrowArray &array, const ArrowSchema &schema, DataChunk &output,
                                                const vector<column_t> &column_ids, idx_t offset, idx_t count) {
	if (column_ids.size() != output.ColumnCount()) {
		throw IOException("Projected column count mismatch: expected %llu columns, got %llu",
		                  static_cast<uint64_t>(column_ids.size()), static_cast<uint64_t>(output.ColumnCount()));
	}

	output.SetCardinality(count);
	if (column_ids.empty()) {
		return;
	}

	for (idx_t out_col_idx = 0; out_col_idx < output.ColumnCount(); out_col_idx++) {
		auto source_col_idx = column_ids[out_col_idx];
		if (source_col_idx >= static_cast<idx_t>(schema.n_children)) {
			throw IOException("Invalid projected Arrow schema column %llu: schema has %lld columns",
			                  static_cast<uint64_t>(source_col_idx), static_cast<int64_t>(schema.n_children));
		}
		if (source_col_idx >= static_cast<idx_t>(array.n_children)) {
			throw IOException("Invalid projected Arrow batch column %llu: batch has %lld columns",
			                  static_cast<uint64_t>(source_col_idx), static_cast<int64_t>(array.n_children));
		}

		auto col_array = array.children[source_col_idx];
		auto col_schema = schema.children[source_col_idx];
		if (!col_array) {
			throw IOException("Invalid Arrow batch: column %llu array is null", static_cast<uint64_t>(source_col_idx));
		}
		if (!col_schema) {
			throw IOException("Invalid Arrow batch: column %llu schema is null", static_cast<uint64_t>(source_col_idx));
		}

		ArrowArray col_view = *col_array;
		col_view.offset = col_array->offset + static_cast<int64_t>(offset);
		CopyArrowToDuckDB(col_view, *col_schema, output.data[out_col_idx], count);
	}
}

// ============================================================================
// Bind Function
// ============================================================================

static unique_ptr<FunctionData> ReadOTLPRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names,
                                                 OtlpSignalType signal_type) {
	auto result = make_uniq<ReadOTLPRustBindData>();

	// Get file pattern from first argument
	auto file_pattern = input.inputs[0].GetValue<string>();

	// Glob files via DuckDB FS
	auto &fs = FileSystem::GetFileSystem(context);
	auto matches = fs.Glob(file_pattern);
	if (matches.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", file_pattern);
	}
	for (auto &match : matches) {
		result->files.emplace_back(match.path);
	}

	result->signal_type = signal_type;
	result->format = OTLP_FORMAT_AUTO;

	// Get schema from Rust
	OtlpStatus status = otlp_get_schema(signal_type, &result->arrow_schema);
	if (status != OTLP_OK) {
		throw IOException("Failed to get OTLP schema from Rust: %s", otlp_status_message(status));
	}
	result->schema_initialized = true;

	GetArrowSchemaColumns(result->arrow_schema, return_types, names);

	result->return_types = return_types;
	result->names = names;

	return std::move(result);
}

// Signal-specific bind functions
static unique_ptr<FunctionData> ReadOTLPLogsRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return ReadOTLPRustBind(context, input, return_types, names, OTLP_SIGNAL_LOGS);
}

static unique_ptr<FunctionData> ReadOTLPTracesRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	return ReadOTLPRustBind(context, input, return_types, names, OTLP_SIGNAL_TRACES);
}

static unique_ptr<FunctionData> ReadOTLPMetricsGaugeRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	return ReadOTLPRustBind(context, input, return_types, names, OTLP_SIGNAL_METRICS_GAUGE);
}

static unique_ptr<FunctionData> ReadOTLPMetricsSumRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return ReadOTLPRustBind(context, input, return_types, names, OTLP_SIGNAL_METRICS_SUM);
}

// Unsupported metric types - throw on bind with clear error message
static unique_ptr<FunctionData> ReadOTLPMetricsUnsupportedBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types, vector<string> &names,
                                                               const string &metric_type) {
	throw NotImplementedException("%s metrics not yet supported. "
	                              "Use read_otlp_metrics_gauge() or read_otlp_metrics_sum() instead.",
	                              metric_type);
}

static unique_ptr<FunctionData> ReadOTLPMetricsUnionUnsupportedBind(ClientContext &context,
                                                                    TableFunctionBindInput &input,
                                                                    vector<LogicalType> &return_types,
                                                                    vector<string> &names) {
	throw NotImplementedException("read_otlp_metrics() is not supported yet because OTLP metrics have multiple "
	                              "shape-specific schemas. Use read_otlp_metrics_gauge(), read_otlp_metrics_sum(), "
	                              "read_otlp_metrics_histogram(), or read_otlp_metrics_exp_histogram().");
}

static unique_ptr<FunctionData> ReadOTLPMetricsHistogramRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	return ReadOTLPRustBind(context, input, return_types, names, OTLP_SIGNAL_METRICS_HISTOGRAM);
}

static unique_ptr<FunctionData> ReadOTLPMetricsExpHistogramRustBind(ClientContext &context,
                                                                    TableFunctionBindInput &input,
                                                                    vector<LogicalType> &return_types,
                                                                    vector<string> &names) {
	return ReadOTLPRustBind(context, input, return_types, names, OTLP_SIGNAL_METRICS_EXP_HISTOGRAM);
}

static unique_ptr<FunctionData> ReadOTLPMetricsSummaryRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	return ReadOTLPMetricsUnsupportedBind(context, input, return_types, names, "Summary");
}

// Dummy scan - never called since bind throws
static void ReadOTLPMetricsUnsupportedScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	// Never reached - bind throws
	output.SetCardinality(0);
}

// ============================================================================
// Init Functions
// ============================================================================

static unique_ptr<GlobalTableFunctionState> ReadOTLPRustInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_uniq<ReadOTLPRustGlobalState>();
}

static unique_ptr<LocalTableFunctionState> ReadOTLPRustInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	auto result = make_uniq<ReadOTLPRustLocalState>();
	result->column_ids = input.column_ids;
	return std::move(result);
}

// ============================================================================
// Scan Function
// ============================================================================

static void ReadOTLPRustScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<ReadOTLPRustBindData>();
	auto &gstate = data.global_state->Cast<ReadOTLPRustGlobalState>();
	auto &lstate = data.local_state->Cast<ReadOTLPRustLocalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	while (true) {
		if (lstate.batch_active) {
			if (lstate.current_batch.length < 0) {
				throw IOException("Invalid Arrow batch: negative row count");
			}
			const auto row_count = static_cast<idx_t>(lstate.current_batch.length);
			const auto remaining = row_count - lstate.batch_offset;
			const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
			CopyProjectedArrowStructToDataChunk(lstate.current_batch, bind_data.arrow_schema, output, lstate.column_ids,
			                                    lstate.batch_offset, count);
			lstate.batch_offset += count;
			if (lstate.batch_offset >= row_count) {
				lstate.current_batch.release(&lstate.current_batch);
				lstate.batch_active = false;
				lstate.batch_offset = 0;
			}
			return;
		}

		// Get next file
		idx_t file_idx = gstate.next_file.fetch_add(1);
		if (file_idx >= bind_data.files.size()) {
			// No more files
			output.SetCardinality(0);
			return;
		}

		// Read file via DuckDB FS
		auto &path = bind_data.files[file_idx];
		auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);

		// Read entire file (for prototype - streaming would be better)
		auto file_size = handle->GetFileSize();

		// Limit file size to 100MB for safety
		constexpr idx_t MAX_FILE_SIZE = 100ULL * 1024ULL * 1024ULL;
		if (file_size > MAX_FILE_SIZE) {
			throw IOException("File %s is too large (%llu bytes). Maximum supported size is 100MB.", path,
			                  static_cast<uint64_t>(file_size));
		}

		lstate.file_buffer.resize(file_size);
		auto bytes_read = handle->Read(lstate.file_buffer.data(), file_size);
		if (bytes_read != static_cast<idx_t>(file_size)) {
			throw IOException("Short read on file %s: expected %lld bytes, got %llu", path.c_str(),
			                  static_cast<int64_t>(file_size), static_cast<uint64_t>(bytes_read));
		}

		ArrowArray array;
		ArrowSchema schema;
		OtlpStatus status = otlp_transform(bind_data.signal_type, bind_data.format,
		                                   reinterpret_cast<const uint8_t *>(lstate.file_buffer.data()),
		                                   lstate.file_buffer.size(), &array, &schema);
		if (status != OTLP_OK) {
			throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
		}

		if (schema.release) {
			schema.release(&schema);
		}

		if (array.length < 0) {
			if (array.release) {
				array.release(&array);
			}
			throw IOException("Invalid Arrow batch: negative row count");
		}
		if (array.release && array.length > 0) {
			lstate.current_batch = array;
			lstate.batch_active = true;
			lstate.batch_offset = 0;
			continue;
		}
		if (array.release) {
			array.release(&array);
		}
	}
}

// ============================================================================
// Registration
// ============================================================================

void RegisterReadOTLPRustFunctions(ExtensionLoader &loader) {
	// read_otlp_logs
	TableFunction logs_func("read_otlp_logs", {LogicalType::VARCHAR}, ReadOTLPRustScan, ReadOTLPLogsRustBind,
	                        ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	logs_func.projection_pushdown = true;
	logs_func.filter_pushdown = false;
	loader.RegisterFunction(logs_func);

	// read_otlp_traces
	TableFunction traces_func("read_otlp_traces", {LogicalType::VARCHAR}, ReadOTLPRustScan, ReadOTLPTracesRustBind,
	                          ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	traces_func.projection_pushdown = true;
	traces_func.filter_pushdown = false;
	loader.RegisterFunction(traces_func);

	// read_otlp_metrics_gauge
	TableFunction gauge_func("read_otlp_metrics_gauge", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                         ReadOTLPMetricsGaugeRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	gauge_func.projection_pushdown = true;
	gauge_func.filter_pushdown = false;
	loader.RegisterFunction(gauge_func);

	// read_otlp_metrics_sum
	TableFunction sum_func("read_otlp_metrics_sum", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                       ReadOTLPMetricsSumRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	sum_func.projection_pushdown = true;
	sum_func.filter_pushdown = false;
	loader.RegisterFunction(sum_func);

	TableFunction metrics_func("read_otlp_metrics", {LogicalType::VARCHAR}, ReadOTLPMetricsUnsupportedScan,
	                           ReadOTLPMetricsUnionUnsupportedBind);
	loader.RegisterFunction(metrics_func);

	// read_otlp_metrics_exp_histogram
	TableFunction exp_histogram_func("read_otlp_metrics_exp_histogram", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                                 ReadOTLPMetricsExpHistogramRustBind, ReadOTLPRustInitGlobal,
	                                 ReadOTLPRustInitLocal);
	exp_histogram_func.projection_pushdown = true;
	exp_histogram_func.filter_pushdown = false;
	loader.RegisterFunction(exp_histogram_func);

	// read_otlp_metrics_histogram
	TableFunction histogram_func("read_otlp_metrics_histogram", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                             ReadOTLPMetricsHistogramRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	histogram_func.projection_pushdown = true;
	histogram_func.filter_pushdown = false;
	loader.RegisterFunction(histogram_func);

	TableFunction summary_func("read_otlp_metrics_summary", {LogicalType::VARCHAR}, ReadOTLPMetricsUnsupportedScan,
	                           ReadOTLPMetricsSummaryRustBind);
	loader.RegisterFunction(summary_func);
}

} // namespace duckdb

#endif // OTLP_RUST_BACKEND
