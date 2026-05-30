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
	OtlpParserHandle *parser = nullptr;
	unique_ptr<FileHandle> current_file;
	ArrowArrayStream current_stream;
	ArrowArray current_batch;
	bool stream_active = false;
	bool batch_active = false;
	idx_t batch_offset = 0;
	string file_buffer;

	~ReadOTLPRustLocalState() override {
		if (batch_active && current_batch.release) {
			current_batch.release(&current_batch);
		}
		if (stream_active && current_stream.release) {
			current_stream.release(&current_stream);
		}
		if (parser) {
			otlp_parser_destroy(parser);
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
	return make_uniq<ReadOTLPRustGlobalState>();
}

static unique_ptr<LocalTableFunctionState> ReadOTLPRustInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	auto result = make_uniq<ReadOTLPRustLocalState>();
	auto &bind_data = input.bind_data->CastNoConst<ReadOTLPRustBindData>();

	// Create Rust parser handle
	OtlpStatus status = otlp_parser_create(bind_data.signal_type, bind_data.format, &result->parser);

	if (status != OTLP_OK) {
		throw IOException("Failed to create OTLP parser: %s", otlp_status_message(status));
	}
	if (!result->parser) {
		throw IOException("OTLP parser creation succeeded but handle is null - internal error");
	}

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
			CopyArrowStructToDataChunk(lstate.current_batch, bind_data.arrow_schema, output, lstate.batch_offset,
			                           count);
			lstate.batch_offset += count;
			if (lstate.batch_offset >= row_count) {
				lstate.current_batch.release(&lstate.current_batch);
				lstate.batch_active = false;
				lstate.batch_offset = 0;
			}
			return;
		}

		// If we have an active stream, try to get next batch
		if (lstate.stream_active) {
			ArrowArray batch;
			int err = lstate.current_stream.get_next(&lstate.current_stream, &batch);

			if (err != 0) {
				const char *msg = lstate.current_stream.get_last_error(&lstate.current_stream);
				throw IOException("Arrow stream error: %s", msg ? msg : "unknown");
			}

			if (batch.release != nullptr) {
				if (batch.length < 0) {
					batch.release(&batch);
					throw IOException("Invalid Arrow batch: negative row count");
				}
				if (batch.length > 0) {
					lstate.current_batch = batch;
					lstate.batch_active = true;
					lstate.batch_offset = 0;
					continue;
				}
				batch.release(&batch);
			}

			// Stream exhausted
			lstate.current_stream.release(&lstate.current_stream);
			lstate.stream_active = false;
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

		// Push to Rust parser
		OtlpStatus status = otlp_parser_push(
		    lstate.parser, reinterpret_cast<const uint8_t *>(lstate.file_buffer.data()), lstate.file_buffer.size(),
		    1 // is_final = true
		);

		if (status != OTLP_OK) {
			const char *err = otlp_parser_last_error(lstate.parser);
			throw IOException("OTLP parse error on %s: %s", path, err ? err : otlp_status_message(status));
		}

		// Drain batches from Rust
		status = otlp_parser_drain(lstate.parser, &lstate.current_stream);
		if (status != OTLP_OK) {
			throw IOException("Failed to drain OTLP batches: %s", otlp_status_message(status));
		}
		lstate.stream_active = true;
	}
}

// ============================================================================
// Registration
// ============================================================================

void RegisterReadOTLPRustFunctions(ExtensionLoader &loader) {
	// read_otlp_logs
	TableFunction logs_func("read_otlp_logs", {LogicalType::VARCHAR}, ReadOTLPRustScan, ReadOTLPLogsRustBind,
	                        ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	logs_func.projection_pushdown = false;
	logs_func.filter_pushdown = false;
	loader.RegisterFunction(logs_func);

	// read_otlp_traces
	TableFunction traces_func("read_otlp_traces", {LogicalType::VARCHAR}, ReadOTLPRustScan, ReadOTLPTracesRustBind,
	                          ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	traces_func.projection_pushdown = false;
	traces_func.filter_pushdown = false;
	loader.RegisterFunction(traces_func);

	// read_otlp_metrics_gauge
	TableFunction gauge_func("read_otlp_metrics_gauge", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                         ReadOTLPMetricsGaugeRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	gauge_func.projection_pushdown = false;
	gauge_func.filter_pushdown = false;
	loader.RegisterFunction(gauge_func);

	// read_otlp_metrics_sum
	TableFunction sum_func("read_otlp_metrics_sum", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                       ReadOTLPMetricsSumRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	sum_func.projection_pushdown = false;
	sum_func.filter_pushdown = false;
	loader.RegisterFunction(sum_func);

	TableFunction metrics_func("read_otlp_metrics", {LogicalType::VARCHAR}, ReadOTLPMetricsUnsupportedScan,
	                           ReadOTLPMetricsUnionUnsupportedBind);
	loader.RegisterFunction(metrics_func);

	// read_otlp_metrics_exp_histogram
	TableFunction exp_histogram_func("read_otlp_metrics_exp_histogram", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                                 ReadOTLPMetricsExpHistogramRustBind, ReadOTLPRustInitGlobal,
	                                 ReadOTLPRustInitLocal);
	exp_histogram_func.projection_pushdown = false;
	exp_histogram_func.filter_pushdown = false;
	loader.RegisterFunction(exp_histogram_func);

	// read_otlp_metrics_histogram
	TableFunction histogram_func("read_otlp_metrics_histogram", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                             ReadOTLPMetricsHistogramRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	histogram_func.projection_pushdown = false;
	histogram_func.filter_pushdown = false;
	loader.RegisterFunction(histogram_func);

	TableFunction summary_func("read_otlp_metrics_summary", {LogicalType::VARCHAR}, ReadOTLPMetricsUnsupportedScan,
	                           ReadOTLPMetricsSummaryRustBind);
	loader.RegisterFunction(summary_func);
}

} // namespace duckdb

#endif // OTLP_RUST_BACKEND
