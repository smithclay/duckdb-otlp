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

// Include the Rust FFI header
#include "otlp2records.h"

namespace duckdb {

// ============================================================================
// Helper: Arrow C Data Interface to DuckDB type conversion
// ============================================================================

static LogicalType ArrowSchemaToDuckDBType(const ArrowSchema &schema) {
	if (!schema.format) {
		throw IOException("Arrow schema has null format string - indicates FFI error");
	}

	std::string fmt(schema.format);

	// Timestamp types (format is "ts<unit>:<tz>"; we ignore tz for naive types)
	if (fmt.substr(0, 3) == "tsm") {
		return LogicalType::TIMESTAMP_MS;
	}
	if (fmt.substr(0, 3) == "tsu") {
		return LogicalType::TIMESTAMP;
	}
	if (fmt.substr(0, 3) == "tsn") {
		return LogicalType::TIMESTAMP_NS;
	}

	// Duration types ("tD<unit>") — no native DuckDB type, surface raw int64.
	if (fmt.size() >= 3 && fmt[0] == 't' && fmt[1] == 'D') {
		return LogicalType::BIGINT;
	}

	// Integer types
	if (fmt == "l") {
		return LogicalType::BIGINT;
	}
	if (fmt == "i") {
		return LogicalType::INTEGER;
	}
	if (fmt == "s") {
		return LogicalType::SMALLINT;
	}
	if (fmt == "c") {
		return LogicalType::TINYINT;
	}

	// Unsigned integer types
	if (fmt == "L") {
		return LogicalType::UBIGINT;
	}
	if (fmt == "I") {
		return LogicalType::UINTEGER;
	}
	if (fmt == "S") {
		return LogicalType::USMALLINT;
	}
	if (fmt == "C") {
		return LogicalType::UTINYINT;
	}

	// Float types
	if (fmt == "g") {
		return LogicalType::DOUBLE;
	}
	if (fmt == "f") {
		return LogicalType::FLOAT;
	}

	// Boolean
	if (fmt == "b") {
		return LogicalType::BOOLEAN;
	}

	// String types
	if (fmt == "u" || fmt == "U") {
		return LogicalType::VARCHAR;
	}

	// Variable-length binary
	if (fmt == "z" || fmt == "Z") {
		return LogicalType::BLOB;
	}

	// FixedSizeBinary ("w:N") — used by otlp2records for trace_id (16) and
	// span_id (8). We render these as hex strings so SQL can match on them
	// without an extra cast, matching the pre-0.8 schema.
	if (fmt.size() > 2 && fmt[0] == 'w' && fmt[1] == ':') {
		return LogicalType::VARCHAR;
	}

	// List types — recurse into the single child schema.
	if (fmt == "+l" || fmt == "+L") {
		if (schema.n_children != 1 || !schema.children || !schema.children[0]) {
			throw IOException("Invalid Arrow list schema: expected exactly 1 child");
		}
		return LogicalType::LIST(ArrowSchemaToDuckDBType(*schema.children[0]));
	}

	// Unknown / complex types fall back to VARCHAR; the scan will throw a
	// clear error if it cannot serialize them.
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
	bool stream_active = false;
	string file_buffer;

	~ReadOTLPRustLocalState() override {
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

	// Convert Arrow schema to DuckDB types
	auto &schema = result->arrow_schema;
	if (schema.n_children > 0 && !schema.children) {
		throw IOException("Invalid Arrow schema: children array is null");
	}
	for (int64_t i = 0; i < schema.n_children; i++) {
		auto child = schema.children[i];
		if (!child) {
			throw IOException("Invalid Arrow schema: child %lld is null", static_cast<int64_t>(i));
		}
		if (!child->name) {
			throw IOException("Invalid Arrow schema: child %lld has null name", static_cast<int64_t>(i));
		}
		names.push_back(child->name);
		return_types.push_back(ArrowSchemaToDuckDBType(*child));
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

// Unsupported metric types - throw on bind with clear error message
static unique_ptr<FunctionData> ReadOTLPMetricsUnsupportedBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types, vector<string> &names,
                                                               const string &metric_type) {
	throw NotImplementedException("%s metrics not yet supported. "
	                              "Use read_otlp_metrics_gauge() or read_otlp_metrics_sum() instead.",
	                              metric_type);
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

	// String type: utf8 (32-bit offsets)
	if (fmt == "u") {
		// UTF-8 string array
		// Buffer 0: validity, Buffer 1: offsets (int32), Buffer 2: data
		if (array.n_buffers < 3) {
			throw IOException("Invalid Arrow utf8 array: expected 3 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
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
	// String type: large_utf8 (64-bit offsets)
	else if (fmt == "U") {
		// Large UTF-8 string array
		// Buffer 0: validity, Buffer 1: offsets (int64), Buffer 2: data
		if (array.n_buffers < 3) {
			throw IOException("Invalid Arrow large_utf8 array: expected 3 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}

		const int64_t *offsets = static_cast<const int64_t *>(array.buffers[1]);
		const char *data = static_cast<const char *>(array.buffers[2]);

		auto *string_data = FlatVector::GetData<string_t>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;

			// Check null
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}

			int64_t start = offsets[array_idx];
			int64_t end = offsets[array_idx + 1];
			int64_t len = end - start;

			string_data[i] = StringVector::AddString(output, data + start, static_cast<idx_t>(len));
		}
	}
	// Integer types — signed and unsigned, all width-2 buffers (validity + values).
	else if (fmt == "l" || fmt == "L" || fmt == "i" || fmt == "I") {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow integer array (%s): expected at least 2 buffers, got %lld", fmt.c_str(),
			                  static_cast<int64_t>(array.n_buffers));
		}
		auto copy_int = [&](auto *typed_values, auto *typed_output) {
			for (idx_t i = 0; i < count; i++) {
				idx_t array_idx = i + array.offset;
				if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
					mask.SetInvalid(i);
					continue;
				}
				typed_output[i] = typed_values[array_idx];
			}
		};
		if (fmt == "l") {
			copy_int(static_cast<const int64_t *>(array.buffers[1]), FlatVector::GetData<int64_t>(output));
		} else if (fmt == "L") {
			copy_int(static_cast<const uint64_t *>(array.buffers[1]), FlatVector::GetData<uint64_t>(output));
		} else if (fmt == "i") {
			copy_int(static_cast<const int32_t *>(array.buffers[1]), FlatVector::GetData<int32_t>(output));
		} else {
			copy_int(static_cast<const uint32_t *>(array.buffers[1]), FlatVector::GetData<uint32_t>(output));
		}
	}
	// Duration types (tDs/tDm/tDu/tDn) — stored as int64; surfaced as BIGINT
	// raw value since DuckDB has no native Arrow Duration type.
	else if (fmt.size() >= 3 && fmt[0] == 't' && fmt[1] == 'D') {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow duration array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
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
	}
	// Float types
	else if (fmt == "g") {
		// DOUBLE
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow double array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
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
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow boolean array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
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
	// Timestamp types. The destination column's logical type already matches
	// the unit (set at bind time), so the int64 value is written verbatim.
	//   tsm → TIMESTAMP_MS (ms),  tsu → TIMESTAMP (μs),  tsn → TIMESTAMP_NS (ns).
	else if (fmt.substr(0, 3) == "tsm" || fmt.substr(0, 3) == "tsu" || fmt.substr(0, 3) == "tsn") {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow timestamp array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
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
	}
	// FixedSizeBinary ("w:N") — N bytes per row, rendered as lowercase hex
	// VARCHAR. Used by otlp2records for 16-byte trace_id and 8-byte span_id.
	else if (fmt.size() > 2 && fmt[0] == 'w' && fmt[1] == ':') {
		int width = std::atoi(fmt.c_str() + 2);
		if (width <= 0) {
			throw IOException("Invalid Arrow FixedSizeBinary format '%s' - bad width", fmt.c_str());
		}
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow FixedSizeBinary array: expected 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		const uint8_t *bytes = static_cast<const uint8_t *>(array.buffers[1]);
		auto *string_data = FlatVector::GetData<string_t>(output);
		static const char hex[] = "0123456789abcdef";
		std::string buf(static_cast<size_t>(width) * 2, '\0');
		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			const uint8_t *row = bytes + array_idx * static_cast<size_t>(width);
			for (int b = 0; b < width; b++) {
				buf[2 * b] = hex[row[b] >> 4];
				buf[2 * b + 1] = hex[row[b] & 0x0F];
			}
			string_data[i] = StringVector::AddString(output, buf.data(), buf.size());
		}
	}
	// List ("+l" int32 offsets) and LargeList ("+L" int64 offsets). The child
	// array carries the element values; we recurse to fill the LIST vector's
	// child entry.
	else if (fmt == "+l" || fmt == "+L") {
		const bool large = (fmt == "+L");
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow list array: expected 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		if (array.n_children != 1 || !array.children || !array.children[0]) {
			throw IOException("Invalid Arrow list array: expected exactly 1 child");
		}
		if (schema.n_children != 1 || !schema.children || !schema.children[0]) {
			throw IOException("Invalid Arrow list schema: expected exactly 1 child");
		}
		// Read the offset at logical position idx (taking array.offset into account).
		auto offset_at = [&](idx_t idx) -> int64_t {
			if (large) {
				return static_cast<const int64_t *>(array.buffers[1])[array.offset + idx];
			}
			return static_cast<const int32_t *>(array.buffers[1])[array.offset + idx];
		};

		const int64_t first_child = offset_at(0);
		const int64_t last_child = offset_at(count);
		const idx_t child_count = static_cast<idx_t>(last_child - first_child);

		// Fill the parent's list_entry_t (offset, length) pairs and validity.
		auto *entries = FlatVector::GetData<list_entry_t>(output);
		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				entries[i].offset = 0;
				entries[i].length = 0;
				continue;
			}
			int64_t start = offset_at(i);
			int64_t end = offset_at(i + 1);
			entries[i].offset = static_cast<idx_t>(start - first_child);
			entries[i].length = static_cast<idx_t>(end - start);
		}

		// Recurse into the child, using a shifted view so the child starts at
		// position first_child within its own array.
		ListVector::Reserve(output, child_count);
		auto &child_vec = ListVector::GetEntry(output);
		if (child_count > 0) {
			ArrowArray child_view = *array.children[0];
			child_view.offset = array.children[0]->offset + first_child;
			CopyArrowToDuckDB(child_view, *schema.children[0], child_vec, child_count);
		}
		ListVector::SetListSize(output, child_count);
	} else {
		throw IOException("Unsupported Arrow format '%s' - cannot convert to DuckDB type", fmt.c_str());
	}
}

// ============================================================================
// Scan Function
// ============================================================================

// Scan function that combines gauge and sum metrics
static void ReadOTLPMetricsUnionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<ReadOTLPRustBindData>();
	auto &gstate = data.global_state->Cast<ReadOTLPRustGlobalState>();
	auto &lstate = data.local_state->Cast<ReadOTLPRustLocalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	// Suppress unused variable warnings for now
	(void)bind_data;
	(void)gstate;
	(void)lstate;
	(void)fs;

	// For now, just throw - we'll implement full union later
	// TODO: Alternate between gauge and sum, add metric_type column
	throw NotImplementedException("Union metrics scan not yet implemented");
}

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
							if (!col_array) {
								throw IOException("Invalid Arrow batch: column %llu array is null",
								                  static_cast<uint64_t>(col_idx));
							}
							if (!col_schema) {
								throw IOException("Invalid Arrow batch: column %llu schema is null",
								                  static_cast<uint64_t>(col_idx));
							}
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

	// Union metrics (gauge + sum combined)
	TableFunction metrics_func("read_otlp_metrics", {LogicalType::VARCHAR}, ReadOTLPMetricsUnionScan,
	                           ReadOTLPMetricsRustBind, ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
	metrics_func.projection_pushdown = false;
	metrics_func.filter_pushdown = false;
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
