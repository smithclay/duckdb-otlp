#include "read_otlp.hpp"
#include "otlp_schema.hpp"
#include "format_detector.hpp"
#include "protobuf_parser.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

void ReadOTLPTableFunction::RegisterFunction(DatabaseInstance &db) {
	// This function is not currently used since we register via GetFunction()
	// But keeping it for future use if needed
}

TableFunction ReadOTLPTableFunction::GetFunction() {
	TableFunction read_otlp("read_otlp", {LogicalType::VARCHAR}, Scan, Bind, Init);
	read_otlp.name = "read_otlp";
	return read_otlp;
}

unique_ptr<FunctionData> ReadOTLPTableFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	// Get file path from first argument
	if (input.inputs.size() != 1) {
		throw BinderException("read_otlp requires exactly one argument (file path)");
	}

	auto file_path = input.inputs[0].ToString();

	// Set return schema to OTLP unified schema
	return_types = OTLPSchema::GetTypes();
	names = OTLPSchema::GetNames();

	return make_uniq<ReadOTLPBindData>(file_path);
}

unique_ptr<GlobalTableFunctionState> ReadOTLPTableFunction::Init(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadOTLPBindData>();
	auto state = make_uniq<ReadOTLPGlobalState>();

	// Open file using DuckDB's file system (supports S3, HTTP, etc.)
	auto &fs = FileSystem::GetFileSystem(context);

	// Check if file exists (skip for remote URLs as FileExists doesn't support them)
	bool is_remote = StringUtil::StartsWith(bind_data.file_path, "http://") ||
	                 StringUtil::StartsWith(bind_data.file_path, "https://") ||
	                 StringUtil::StartsWith(bind_data.file_path, "s3://");

	if (!is_remote && !fs.FileExists(bind_data.file_path)) {
		throw IOException("File not found: " + bind_data.file_path);
	}

	// Open file for reading (will throw if remote file doesn't exist)
	state->file_handle = fs.OpenFile(bind_data.file_path, FileFlags::FILE_FLAGS_READ);

	// Read first chunk to detect format
	string detect_buffer;
	detect_buffer.resize(1024);
	idx_t bytes_read = state->file_handle->Read((void *)detect_buffer.data(), 1024);
	detect_buffer.resize(bytes_read);

	// Detect format
	state->format = FormatDetector::DetectFormat(detect_buffer.data(), detect_buffer.size());

	// Reset file position to beginning
	state->file_handle->Seek(0);

	if (state->format == OTLPFormat::JSON) {
		// JSON format - read entire file and try to parse as single OTLP object first
		state->json_parser = make_uniq<OTLPJSONParser>();

		// Read entire file into memory to check if it's a single JSON object
		auto file_size = fs.GetFileSize(*state->file_handle);
		if (file_size > 0 &&
		    file_size < static_cast<int64_t>(100) * 1024 * 1024) { // Limit to 100MB for single object parsing
			string file_contents;
			file_contents.resize(file_size);
			idx_t total_read = state->file_handle->Read((void *)file_contents.data(), file_size);
			file_contents.resize(total_read);

			// Try parsing as single OTLP JSON object
			timestamp_t timestamp;
			string resource_json;
			string data_json;

			if (state->json_parser->ParseLine(file_contents, timestamp, resource_json, data_json)) {
				// Successfully parsed as single object - store it
				state->timestamps.push_back(timestamp);
				state->resources.push_back(resource_json);
				state->datas.push_back(data_json);
				state->is_single_json_object = true;
			} else {
				// Not a single object, fall back to line-by-line parsing
				state->file_handle->Seek(0);
				state->is_single_json_object = false;
			}
		} else {
			// File too large or empty, use line-by-line parsing
			state->is_single_json_object = false;
		}
	} else if (state->format == OTLPFormat::PROTOBUF) {
		// Protobuf format - read entire file and parse
		state->protobuf_parser = make_uniq<OTLPProtobufParser>();

		// Read entire file into memory
		auto file_size = fs.GetFileSize(*state->file_handle);
		string file_contents;
		file_contents.resize(file_size);
		idx_t total_read = state->file_handle->Read((void *)file_contents.data(), file_size);

		// Detect signal type
		auto signal_type = FormatDetector::DetectProtobufSignalType(file_contents.data(), total_read);

		// Parse based on signal type
		idx_t row_count = 0;
		if (signal_type == FormatDetector::SignalType::TRACES) {
			row_count = state->protobuf_parser->ParseTracesData(file_contents.data(), total_read, state->timestamps,
			                                                    state->resources, state->datas);
		} else if (signal_type == FormatDetector::SignalType::METRICS) {
			row_count = state->protobuf_parser->ParseMetricsData(file_contents.data(), total_read, state->timestamps,
			                                                     state->resources, state->datas);
		} else if (signal_type == FormatDetector::SignalType::LOGS) {
			row_count = state->protobuf_parser->ParseLogsData(file_contents.data(), total_read, state->timestamps,
			                                                  state->resources, state->datas);
		} else {
			throw IOException("Unable to detect OTLP signal type from protobuf data");
		}

		if (row_count == 0) {
			string error = state->protobuf_parser->GetLastError();
			throw IOException("Failed to parse OTLP protobuf data: " + error);
		}

		state->current_row = 0;
	} else {
		throw IOException("Unable to detect OTLP format (expected JSON or Protobuf)");
	}

	state->current_line = 0;
	state->finished = false;

	return std::move(state);
}

// Helper to add a row to the output chunk
static void AddRowToOutput(DataChunk &output, idx_t output_idx, timestamp_t timestamp, const string &resource,
                           const string &data) {
	// Set timestamp
	auto timestamp_data = FlatVector::GetData<timestamp_t>(output.data[OTLPSchema::TIMESTAMP_COL]);
	timestamp_data[output_idx] = timestamp;

	// Set resource JSON
	auto &resource_vector = output.data[OTLPSchema::RESOURCE_COL];
	FlatVector::GetData<string_t>(resource_vector)[output_idx] = StringVector::AddString(resource_vector, resource);

	// Set data JSON
	auto &data_vector = output.data[OTLPSchema::DATA_COL];
	FlatVector::GetData<string_t>(data_vector)[output_idx] = StringVector::AddString(data_vector, data);
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

	if (gstate.format == OTLPFormat::PROTOBUF || gstate.is_single_json_object) {
		// Protobuf: return pre-parsed data
		while (output_idx < BATCH_SIZE && gstate.current_row < gstate.timestamps.size()) {
			AddRowToOutput(output, output_idx, gstate.timestamps[gstate.current_row],
			               gstate.resources[gstate.current_row], gstate.datas[gstate.current_row]);
			output_idx++;
			gstate.current_row++;
		}

		if (gstate.current_row >= gstate.timestamps.size()) {
			gstate.finished = true;
		}
	} else {
		// JSON: Read lines until we fill the output chunk or reach EOF
		while (output_idx < BATCH_SIZE) {
			// Read next line from file
			string line;
			if (!ReadLine(*gstate.file_handle, gstate.buffer, gstate.buffer_offset, line)) {
				// EOF reached
				gstate.finished = true;
				break;
			}

			gstate.current_line++;

			// Skip empty lines
			StringUtil::Trim(line);
			if (line.empty()) {
				continue;
			}

			// Parse OTLP JSON line
			timestamp_t timestamp;
			string resource_json;
			string data_json;

			if (!gstate.json_parser->ParseLine(line, timestamp, resource_json, data_json)) {
				// Skip malformed lines and track for warning
				gstate.skipped_lines++;
				continue;
			}

			// Add parsed data to output chunk
			AddRowToOutput(output, output_idx++, timestamp, resource_json, data_json);
		}

		// Emit warning about skipped lines once at end of scan
		if (gstate.finished && gstate.skipped_lines > 0 && !gstate.warning_emitted) {
			string warning_msg = StringUtil::Format("Skipped %llu malformed or invalid OTLP JSON line%s",
			                                        gstate.skipped_lines, gstate.skipped_lines == 1 ? "" : "s");
			// Note: In DuckDB, warnings are typically handled via ErrorData
			// For now, we silently skip - full warning support can be added later
			gstate.warning_emitted = true;
		}
	}

	output.SetCardinality(output_idx);
}

} // namespace duckdb
