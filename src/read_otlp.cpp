#include "read_otlp.hpp"
#include "otlp_schema.hpp"
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

	// Check if file exists
	if (!fs.FileExists(bind_data.file_path)) {
		throw IOException("File not found: " + bind_data.file_path);
	}

	// Open file for reading
	state->file_handle = fs.OpenFile(bind_data.file_path, FileFlags::FILE_FLAGS_READ);
	state->parser = make_uniq<OTLPJSONParser>();
	state->current_line = 0;
	state->finished = false;

	return std::move(state);
}

// Helper to read next line from file handle
static bool ReadLine(FileHandle &file_handle, string &buffer, idx_t &buffer_offset, string &line) {
	line.clear();

	while (true) {
		// If buffer is empty or exhausted, read more data
		if (buffer_offset >= buffer.size()) {
			buffer.resize(8192); // 8KB buffer
			idx_t bytes_read = file_handle.Read((void*)buffer.data(), 8192);
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

	// Read lines until we fill the output chunk or reach EOF
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

		if (!gstate.parser->ParseLine(line, timestamp, resource_json, data_json)) {
			// Skip malformed lines and track for warning
			gstate.skipped_lines++;
			continue;
		}

		// Add parsed data to output chunk
		auto timestamp_data = FlatVector::GetData<timestamp_t>(output.data[OTLPSchema::TIMESTAMP_COL]);
		timestamp_data[output_idx] = timestamp;

		// Set resource JSON
		auto &resource_vector = output.data[OTLPSchema::RESOURCE_COL];
		FlatVector::GetData<string_t>(resource_vector)[output_idx] = StringVector::AddString(resource_vector, resource_json);

		// Set data JSON
		auto &data_vector = output.data[OTLPSchema::DATA_COL];
		FlatVector::GetData<string_t>(data_vector)[output_idx] = StringVector::AddString(data_vector, data_json);

		output_idx++;
	}

	output.SetCardinality(output_idx);

	// Emit warning about skipped lines once at end of scan
	if (gstate.finished && gstate.skipped_lines > 0 && !gstate.warning_emitted) {
		string warning_msg = StringUtil::Format("Skipped %llu malformed or invalid OTLP JSON line%s",
		                                         gstate.skipped_lines,
		                                         gstate.skipped_lines == 1 ? "" : "s");
		// Note: In DuckDB, warnings are typically handled via ErrorData
		// For now, we silently skip - full warning support can be added later
		gstate.warning_emitted = true;
	}
}

} // namespace duckdb
