/**
 * @file read_otlp_rust.cpp
 * @brief DuckDB table function using Rust otlp2records backend
 *
 * This file implements read_otlp_logs_rust, read_otlp_traces_rust, etc.
 * table functions that use the Rust otlp2records library for parsing.
 *
 * Requires: -DOTLP_USE_RUST=ON at build time
 */

#ifdef OTLP_RUST_BACKEND

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

// Include the Rust FFI header
#include "otlp2records.h"

namespace duckdb {

// ============================================================================
// Helper: Arrow C Data Interface to DuckDB type conversion
// ============================================================================

static LogicalType ArrowFormatToDuckDBType(const char *format) {
	if (!format) {
		return LogicalType::VARCHAR;
	}

	std::string fmt(format);

	// Timestamp types
	if (fmt.substr(0, 3) == "tsm") {
		// Timestamp millisecond
		return LogicalType::TIMESTAMP_MS;
	}
	if (fmt.substr(0, 3) == "tsu") {
		// Timestamp microsecond
		return LogicalType::TIMESTAMP;
	}
	if (fmt.substr(0, 3) == "tsn") {
		// Timestamp nanosecond
		return LogicalType::TIMESTAMP_NS;
	}

	// Integer types
	if (fmt == "l")
		return LogicalType::BIGINT;
	if (fmt == "i")
		return LogicalType::INTEGER;
	if (fmt == "s")
		return LogicalType::SMALLINT;
	if (fmt == "c")
		return LogicalType::TINYINT;

	// Unsigned integer types
	if (fmt == "L")
		return LogicalType::UBIGINT;
	if (fmt == "I")
		return LogicalType::UINTEGER;
	if (fmt == "S")
		return LogicalType::USMALLINT;
	if (fmt == "C")
		return LogicalType::UTINYINT;

	// Float types
	if (fmt == "g")
		return LogicalType::DOUBLE;
	if (fmt == "f")
		return LogicalType::FLOAT;

	// Boolean
	if (fmt == "b")
		return LogicalType::BOOLEAN;

	// String types
	if (fmt == "u" || fmt == "U")
		return LogicalType::VARCHAR;

	// Binary
	if (fmt == "z" || fmt == "Z")
		return LogicalType::BLOB;

	// Default to VARCHAR for unknown types (including complex types)
	return LogicalType::VARCHAR;
}

// Helper to add metric_type discriminator column
static void AddMetricTypeColumn(vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("metric_type");
	return_types.push_back(LogicalType::VARCHAR);
}

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
	bool is_union_metrics = false;

	~ReadOTLPRustBindData() {
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
	bool stream_active = false;
	string file_buffer;

	~ReadOTLPRustLocalState() {
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
	auto matches = fs.GlobFiles(file_pattern, context, FileGlobOptions::DISALLOW_EMPTY);
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

	// Convert Arrow schema to DuckDB types
	auto &schema = result->arrow_schema;
	for (int64_t i = 0; i < schema.n_children; i++) {
		auto child = schema.children[i];
		names.push_back(child->name);
		return_types.push_back(ArrowFormatToDuckDBType(child->format));
	}

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

static unique_ptr<FunctionData> ReadOTLPMetricsRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	// Use sum schema as base (has all columns)
	auto result = ReadOTLPRustBind(context, input, return_types, names, OTLP_SIGNAL_METRICS_SUM);

	// Add metric_type discriminator column
	AddMetricTypeColumn(return_types, names);

	// Store that this is union mode
	auto &bind_data = result->CastNoConst<ReadOTLPRustBindData>();
	bind_data.is_union_metrics = true;

	return result;
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

	return std::move(result);
}

// ============================================================================
// Helper: Convert Arrow array to DuckDB vector
// ============================================================================

static void CopyArrowToDuckDB(const ArrowArray &array, const ArrowSchema &schema, Vector &output, idx_t count) {
	// Get the format string
	std::string fmt(schema.format ? schema.format : "");

	// Handle null values
	const uint8_t *null_bitmap = nullptr;
	if (array.n_buffers > 0 && array.buffers[0]) {
		null_bitmap = static_cast<const uint8_t *>(array.buffers[0]);
	}

	auto &mask = FlatVector::Validity(output);

	// String types (u = utf8, U = large_utf8)
	if (fmt == "u" || fmt == "U") {
		// UTF-8 string array
		// Buffer 0: validity, Buffer 1: offsets, Buffer 2: data
		if (array.n_buffers < 3) {
			throw IOException("Invalid Arrow string array: expected 3 buffers");
		}

		const int32_t *offsets = static_cast<const int32_t *>(array.buffers[1]);
		const char *data = static_cast<const char *>(array.buffers[2]);

		auto *string_data = FlatVector::GetData<string_t>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;

			// Check null
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}

			int32_t start = offsets[array_idx];
			int32_t end = offsets[array_idx + 1];
			int32_t len = end - start;

			string_data[i] = StringVector::AddString(output, data + start, len);
		}
	}
	// Integer types
	else if (fmt == "l") {
		// INT64
		const int64_t *values = static_cast<const int64_t *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<int64_t>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = values[array_idx];
		}
	} else if (fmt == "i") {
		// INT32
		const int32_t *values = static_cast<const int32_t *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<int32_t>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = values[array_idx];
		}
	}
	// Float types
	else if (fmt == "g") {
		// DOUBLE
		const double *values = static_cast<const double *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<double>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = values[array_idx];
		}
	}
	// Boolean
	else if (fmt == "b") {
		const uint8_t *values = static_cast<const uint8_t *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<bool>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = (values[array_idx / 8] & (1 << (array_idx % 8))) != 0;
		}
	}
	// Timestamp types
	else if (fmt.substr(0, 3) == "tsm" || fmt.substr(0, 3) == "tsu" || fmt.substr(0, 3) == "tsn") {
		// Timestamps are stored as int64
		const int64_t *values = static_cast<const int64_t *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<timestamp_t>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}

			int64_t val = values[array_idx];

			// Convert based on unit
			if (fmt.substr(0, 3) == "tsm") {
				// Milliseconds to microseconds
				output_data[i] = timestamp_t(val * 1000);
			} else if (fmt.substr(0, 3) == "tsu") {
				// Already microseconds
				output_data[i] = timestamp_t(val);
			} else {
				// Nanoseconds to microseconds
				output_data[i] = timestamp_t(val / 1000);
			}
		}
	} else {
		// Unknown type - set all to null
		for (idx_t i = 0; i < count; i++) {
			mask.SetInvalid(i);
		}
	}
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
		// If we have an active stream, try to get next batch
		if (lstate.stream_active) {
			ArrowArray batch;
			int err = lstate.current_stream.get_next(&lstate.current_stream, &batch);

			if (err != 0) {
				const char *msg = lstate.current_stream.get_last_error(&lstate.current_stream);
				throw IOException("Arrow stream error: %s", msg ? msg : "unknown");
			}

			if (batch.release != nullptr) {
				// We have a batch - convert to DuckDB DataChunk
				idx_t row_count = batch.length;

				if (row_count > 0) {
					output.SetCardinality(row_count);

					// The batch is a struct array - its children are the columns
					for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
						if (col_idx < (idx_t)batch.n_children) {
							ArrowArray *col_array = batch.children[col_idx];
							ArrowSchema *col_schema = bind_data.arrow_schema.children[col_idx];

							CopyArrowToDuckDB(*col_array, *col_schema, output.data[col_idx], row_count);
						}
					}
				}

				// Release the batch
				batch.release(&batch);

				if (row_count > 0) {
					return;
				}
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
		if (file_size > 100 * 1024 * 1024) {
			throw IOException("File %s is too large (%llu bytes). Maximum supported size is 100MB.", path, file_size);
		}

		lstate.file_buffer.resize(file_size);
		handle->Read(lstate.file_buffer.data(), file_size);

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
	// read_otlp_logs_rust
	TableFunction logs_func("read_otlp_logs_rust", {LogicalType::VARCHAR}, ReadOTLPRustScan, ReadOTLPLogsRustBind,
	                        ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	logs_func.projection_pushdown = false;
	logs_func.filter_pushdown = false;
	loader.RegisterFunction(logs_func);

	// read_otlp_traces_rust
	TableFunction traces_func("read_otlp_traces_rust", {LogicalType::VARCHAR}, ReadOTLPRustScan, ReadOTLPTracesRustBind,
	                          ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	traces_func.projection_pushdown = false;
	traces_func.filter_pushdown = false;
	loader.RegisterFunction(traces_func);

	// read_otlp_metrics_gauge_rust
	TableFunction gauge_func("read_otlp_metrics_gauge_rust", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                         ReadOTLPMetricsGaugeRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	gauge_func.projection_pushdown = false;
	gauge_func.filter_pushdown = false;
	loader.RegisterFunction(gauge_func);

	// read_otlp_metrics_sum_rust
	TableFunction sum_func("read_otlp_metrics_sum_rust", {LogicalType::VARCHAR}, ReadOTLPRustScan,
	                       ReadOTLPMetricsSumRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	sum_func.projection_pushdown = false;
	sum_func.filter_pushdown = false;
	loader.RegisterFunction(sum_func);
}

} // namespace duckdb

#endif // OTLP_RUST_BACKEND
