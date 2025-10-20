#include "function/read_otlp.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "schema/otlp_logs_schema.hpp"
#include "schema/otlp_metrics_schemas.hpp"
#include "schema/otlp_metrics_union_schema.hpp"
#include "parsers/format_detector.hpp"
#include "parsers/protobuf_parser.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Forward declare specialized bind functions
static unique_ptr<FunctionData> BindTraces(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);
static unique_ptr<FunctionData> BindLogs(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names);
static unique_ptr<FunctionData> BindMetrics(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names);

// Forward declare helper functions
static bool ReadLine(FileHandle &file_handle, string &buffer, idx_t &buffer_offset, string &line);
static void ProcessFile(const string &file_path, ClientContext &context, OTLPTableType table_type,
                        ReadOTLPGlobalState &state);

TableFunction ReadOTLPTableFunction::GetTracesFunction() {
	TableFunction func("read_otlp_traces", {LogicalType::VARCHAR}, Scan, BindTraces, Init);
	func.name = "read_otlp_traces";
	return func;
}

TableFunction ReadOTLPTableFunction::GetLogsFunction() {
	TableFunction func("read_otlp_logs", {LogicalType::VARCHAR}, Scan, BindLogs, Init);
	func.name = "read_otlp_logs";
	return func;
}

TableFunction ReadOTLPTableFunction::GetMetricsFunction() {
	TableFunction func("read_otlp_metrics", {LogicalType::VARCHAR}, Scan, BindMetrics, Init);
	func.name = "read_otlp_metrics";
	return func;
}

// Specialized bind functions for each table type
static unique_ptr<FunctionData> BindTraces(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_traces requires exactly one argument (file path)");
	}

	auto file_path = input.inputs[0].ToString();
	return_types = OTLPTracesSchema::GetColumnTypes();
	names = OTLPTracesSchema::GetColumnNames();

	return make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::TRACES);
}

static unique_ptr<FunctionData> BindLogs(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_logs requires exactly one argument (file path)");
	}

	auto file_path = input.inputs[0].ToString();
	return_types = OTLPLogsSchema::GetColumnTypes();
	names = OTLPLogsSchema::GetColumnNames();

	return make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::LOGS);
}

static unique_ptr<FunctionData> BindMetrics(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp_metrics requires exactly one argument (file path)");
	}

	auto file_path = input.inputs[0].ToString();
	// File reading uses union schema to support all metric types in a single table
	return_types = OTLPMetricsUnionSchema::GetColumnTypes();
	names = OTLPMetricsUnionSchema::GetColumnNames();

	return make_uniq<ReadOTLPBindData>(file_path, OTLPTableType::METRICS_GAUGE);
}

unique_ptr<GlobalTableFunctionState> ReadOTLPTableFunction::Init(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadOTLPBindData>();
	auto state = make_uniq<ReadOTLPGlobalState>();

	// Get file system and expand glob patterns
	auto &fs = FileSystem::GetFileSystem(context);

	// Use GlobFiles which handles both glob patterns and single files
	auto files = fs.GlobFiles(bind_data.pattern, context, FileGlobOptions::DISALLOW_EMPTY);

	// Process each matched file
	for (auto &file_info : files) {
		ProcessFile(file_info.path, context, bind_data.table_type, *state);
	}

	if (state->rows.empty()) {
		throw IOException("No valid OTLP data found in matched files");
	}

	state->current_row = 0;
	state->current_line = 0;
	state->finished = false;

	return std::move(state);
}

// Helper function to process a single file
static void ProcessFile(const string &file_path, ClientContext &context, OTLPTableType table_type,
                        ReadOTLPGlobalState &state) {
	auto &fs = FileSystem::GetFileSystem(context);

	// Open file for reading (GlobFiles has already validated the file exists)
	auto file_handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);

	// Read first chunk to detect format
	string detect_buffer;
	detect_buffer.resize(1024);
	idx_t bytes_read = file_handle->Read((void *)detect_buffer.data(), 1024);
	detect_buffer.resize(bytes_read);

	// Detect format
	OTLPFormat format = FormatDetector::DetectFormat(detect_buffer.data(), detect_buffer.size());

	// Reset file position to beginning
	file_handle->Seek(0);

	// Temporary rows vector for this file
	vector<vector<Value>> file_rows;

	if (format == OTLPFormat::JSON) {
		auto json_parser = make_uniq<OTLPJSONParser>();

		// Detect format based on file extension
		bool is_jsonl = StringUtil::EndsWith(StringUtil::Lower(file_path), ".jsonl");

		if (is_jsonl) {
			// JSONL format - read line by line and parse each line to strongly-typed rows
			string buffer;
			idx_t buffer_offset = 0;
			string line;
			idx_t current_line = 0;

			while (ReadLine(*file_handle, buffer, buffer_offset, line)) {
				// Skip empty lines
				StringUtil::Trim(line);
				if (line.empty()) {
					continue;
				}

				// Parse each line based on table type (v2 schema)
				bool success = false;
				if (table_type == OTLPTableType::TRACES) {
					success = json_parser->ParseTracesToTypedRows(line, file_rows);
				} else if (table_type == OTLPTableType::LOGS) {
					success = json_parser->ParseLogsToTypedRows(line, file_rows);
				} else if (table_type == OTLPTableType::METRICS_GAUGE) {
					// Parse all metric types into union schema
					success = json_parser->ParseMetricsToTypedRows(line, file_rows);
				} else {
					throw IOException("Unsupported table type for JSON parsing");
				}

				if (!success) {
					string error = json_parser->GetLastError();
					throw IOException("Failed to parse OTLP JSON data in file '" + file_path + "' on line (approx) " +
					                  std::to_string(current_line + 1) + ": " + error);
				}
				current_line++;
			}
		} else {
			// Single JSON object format - read entire file as one JSON object
			auto file_size = fs.GetFileSize(*file_handle);
			if (file_size > 0 && file_size < static_cast<int64_t>(100) * 1024 * 1024) { // Limit to 100MB
				string file_contents;
				file_contents.resize(file_size);
				idx_t total_read = file_handle->Read((void *)file_contents.data(), file_size);
				file_contents.resize(total_read);

				// Parse entire file based on table type (v2 schema)
				bool success = false;
				if (table_type == OTLPTableType::TRACES) {
					success = json_parser->ParseTracesToTypedRows(file_contents, file_rows);
				} else if (table_type == OTLPTableType::LOGS) {
					success = json_parser->ParseLogsToTypedRows(file_contents, file_rows);
				} else if (table_type == OTLPTableType::METRICS_GAUGE) {
					success = json_parser->ParseMetricsToTypedRows(file_contents, file_rows);
				} else {
					throw IOException("Unsupported table type for JSON parsing");
				}

				if (!success || file_rows.empty()) {
					string error = json_parser->GetLastError();
					throw IOException("Failed to parse OTLP JSON data in file '" + file_path + "': " + error);
				}
			} else {
				throw IOException("JSON file too large or empty (limit: 100MB): " + file_path);
			}
		}
	} else if (format == OTLPFormat::PROTOBUF) {
		// Protobuf format - read entire file and parse to strongly-typed rows (with size cap)
		auto protobuf_parser = make_uniq<OTLPProtobufParser>();

		// Read entire file into memory
		auto file_size = fs.GetFileSize(*file_handle);
		const int64_t max_proto_bytes = static_cast<int64_t>(100) * 1024 * 1024; // 100MB
		if (file_size <= 0 || file_size > max_proto_bytes) {
			throw IOException("Protobuf file too large or empty (limit: 100MB): " + file_path);
		}
		string file_contents;
		file_contents.resize(file_size);
		idx_t total_read = file_handle->Read((void *)file_contents.data(), file_size);

		// Parse based on table type (v2 schema)
		idx_t row_count = 0;
		if (table_type == OTLPTableType::TRACES) {
			row_count = protobuf_parser->ParseTracesToTypedRows(file_contents.data(), total_read, file_rows);
		} else if (table_type == OTLPTableType::LOGS) {
			row_count = protobuf_parser->ParseLogsToTypedRows(file_contents.data(), total_read, file_rows);
		} else if (table_type == OTLPTableType::METRICS_GAUGE) {
			// Parse all metric types into gauge schema
			row_count = protobuf_parser->ParseMetricsToTypedRows(file_contents.data(), total_read, file_rows);
		} else {
			throw IOException("Unsupported table type for protobuf parsing");
		}

		if (row_count == 0) {
			string error = protobuf_parser->GetLastError();
			throw IOException("Failed to parse OTLP protobuf data in file '" + file_path + "': " + error);
		}
	} else {
		throw IOException("Unable to detect OTLP format (expected JSON or Protobuf) in file: " + file_path);
	}

	// Append all rows from this file to the global state
	state.rows.insert(state.rows.end(), file_rows.begin(), file_rows.end());
}

// Helper to read next line from file handle
static bool ReadLine(FileHandle &file_handle, string &buffer, idx_t &buffer_offset, string &line) {
	line.clear();

	while (true) {
		// If buffer is empty or exhausted, read more data
		if (buffer_offset >= buffer.size()) {
			buffer.resize(8192); // 8KB buffer
			idx_t bytes_read = file_handle.Read((void *)buffer.data(), 8192);
			if (bytes_read == 0) {
				// EOF
				return !line.empty(); // Return true if we have a partial line
			}
			buffer.resize(bytes_read);
			buffer_offset = 0;
		}

		// Find newline in buffer
		for (idx_t i = buffer_offset; i < buffer.size(); i++) {
			if (buffer[i] == '\n') {
				line.append(buffer.data() + buffer_offset, i - buffer_offset);
				buffer_offset = i + 1;
				return true;
			}
		}

		// No newline found, append rest of buffer to line
		line.append(buffer.data() + buffer_offset, buffer.size() - buffer_offset);
		buffer_offset = buffer.size();
	}
}

void ReadOTLPTableFunction::Scan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ReadOTLPGlobalState>();

	if (gstate.finished) {
		return;
	}

	idx_t output_idx = 0;
	constexpr idx_t BATCH_SIZE = STANDARD_VECTOR_SIZE;

	// Output strongly-typed rows from v2 schema
	while (output_idx < BATCH_SIZE && gstate.current_row < gstate.rows.size()) {
		auto &row = gstate.rows[gstate.current_row];

		// Copy each column value from parsed row to output chunk
		for (idx_t col_idx = 0; col_idx < row.size(); col_idx++) {
			output.data[col_idx].SetValue(output_idx, row[col_idx]);
		}

		output_idx++;
		gstate.current_row++;
	}

	if (gstate.current_row >= gstate.rows.size()) {
		gstate.finished = true;
	}

	output.SetCardinality(output_idx);
}

} // namespace duckdb
