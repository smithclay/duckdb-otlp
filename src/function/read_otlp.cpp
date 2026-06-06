/**
 * @file read_otlp.cpp
 * @brief DuckDB table functions using Rust otlp2records backend
 *
 * This file implements read_otlp_logs, read_otlp_traces, etc.
 * table functions that use the Rust otlp2records library for parsing.
 */

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
	// Number of files in the glob; one file is handed to each thread via the atomic
	// next_file cursor, so the scan parallelizes across files (one file per task).
	idx_t file_count = 1;

	idx_t MaxThreads() const override {
		// One file per thread: each thread pulls a distinct file via next_file and
		// keeps fully isolated local state (file_buffer/current_batch). bind_data
		// (schema) is read-only, so multi-file globs scan in parallel.
		return MaxValue<idx_t>(1, file_count);
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
	auto result = make_uniq<ReadOTLPRustGlobalState>();
	auto &bind_data = input.bind_data->Cast<ReadOTLPRustBindData>();
	result->file_count = bind_data.files.size();
	return std::move(result);
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
			// Projection pushdown only prunes the Arrow->DuckDB copy here: the Rust transform
			// already parsed the payload and built every column of the batch (the FFI has no
			// column mask), so unselected columns are materialized in Rust regardless. Skipping
			// their copy is the smaller half of the per-column cost; a column-aware transform is
			// the lever for the larger half and waits on the streaming backend.
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

		// Whole-file materialization: the entire file is read into file_buffer and the whole
		// file is transformed into a single ArrowArray held in memory at once (input is capped
		// at MAX_FILE_SIZE below). This is the prototype approach; a streaming/chunked reader
		// that incrementally parses and yields batches is the intended future direction.
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
		const auto *input_bytes = reinterpret_cast<const uint8_t *>(lstate.file_buffer.data());
		const auto input_len = lstate.file_buffer.size();

		switch (bind_data.signal_type) {
		case OTLP_SIGNAL_LOGS:
		case OTLP_SIGNAL_TRACES: {
			// Logs/Traces have a single shape; otlp_transform is the canonical verb for them.
			OtlpStatus status =
			    otlp_transform(bind_data.signal_type, bind_data.format, input_bytes, input_len, &array, &schema);
			if (status != OTLP_OK) {
				throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
			}
			// Schema was cached at bind time; release this copy and keep only the array.
			if (schema.release) {
				schema.release(&schema);
			}
			break;
		}
		case OTLP_SIGNAL_METRICS_GAUGE:
		case OTLP_SIGNAL_METRICS_SUM:
		case OTLP_SIGNAL_METRICS_HISTOGRAM:
		case OTLP_SIGNAL_METRICS_EXP_HISTOGRAM: {
			// Metrics: one parse yields all four shapes (otlp_transform_metrics_all is the single
			// canonical metric verb). Keep only the requested shape's array; release every other
			// present batch's array+schema plus the chosen batch's schema (the cached bind-time
			// schema is used for conversion).
			OtlpMetricsArrowBatches batches = {};
			OtlpStatus status = otlp_transform_metrics_all(bind_data.format, input_bytes, input_len, &batches);
			if (status != OTLP_OK) {
				// On failure no batch is present; nothing to release.
				throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
			}

			OtlpArrowBatch *chosen = nullptr;
			switch (bind_data.signal_type) {
			case OTLP_SIGNAL_METRICS_GAUGE:
				chosen = &batches.gauge;
				break;
			case OTLP_SIGNAL_METRICS_SUM:
				chosen = &batches.sum;
				break;
			case OTLP_SIGNAL_METRICS_HISTOGRAM:
				chosen = &batches.histogram;
				break;
			default: // OTLP_SIGNAL_METRICS_EXP_HISTOGRAM
				chosen = &batches.exp_histogram;
				break;
			}

			// Keep the chosen array; release its schema (we convert via the cached bind-time
			// schema). If the chosen shape is absent, leave array unreleasable below.
			if (chosen->present) {
				if (chosen->schema.release) {
					chosen->schema.release(&chosen->schema);
				}
				array = chosen->array;
			} else {
				// No rows for this shape: synthesize an empty, no-op array.
				array = {};
			}
			// Prevent the chosen batch from being released as an "other" below.
			chosen->present = 0;

			// Release every remaining present batch (array + schema) so nothing leaks.
			ReleaseOtlpArrowBatch(batches.gauge);
			ReleaseOtlpArrowBatch(batches.sum);
			ReleaseOtlpArrowBatch(batches.histogram);
			ReleaseOtlpArrowBatch(batches.exp_histogram);
			break;
		}
		default:
			throw IOException("Unsupported OTLP signal type for read scan");
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
