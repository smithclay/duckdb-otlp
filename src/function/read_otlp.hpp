#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "parsers/json_parser.hpp"
#include "parsers/protobuf_parser.hpp"
#include "parsers/format_detector.hpp"
#include "schema/otlp_types.hpp"

namespace duckdb {

//! read_otlp_*() table functions for reading OTLP files with strongly-typed schemas
class ReadOTLPTableFunction {
public:
	//! Get table functions for registration
	static TableFunction GetTracesFunction();
	static TableFunction GetLogsFunction();
	static TableFunction GetMetricsFunction();

	//! Initialize scan state
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	//! Scan function to read and parse OTLP data
	static void Scan(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

//! Bind data for read_otlp function
struct ReadOTLPBindData : public TableFunctionData {
	string file_path;
	OTLPTableType table_type; // Detected table type for v2 schema

	explicit ReadOTLPBindData(string path, OTLPTableType type) : file_path(std::move(path)), table_type(type) {
	}
};

//! Global state for reading OTLP files
struct ReadOTLPGlobalState : public GlobalTableFunctionState {
	unique_ptr<FileHandle> file_handle;
	unique_ptr<OTLPJSONParser> json_parser;
	unique_ptr<OTLPProtobufParser> protobuf_parser;
	OTLPFormat format;
	idx_t current_line;
	bool finished;
	string buffer;
	idx_t buffer_offset;
	idx_t skipped_lines;
	bool warning_emitted;

	// V2 schema: strongly-typed rows
	vector<vector<Value>> rows;
	idx_t current_row;

	ReadOTLPGlobalState()
	    : format(OTLPFormat::UNKNOWN), current_line(0), finished(false), buffer_offset(0), skipped_lines(0),
	      warning_emitted(false), current_row(0) {
	}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for Phase 2
	}
};

} // namespace duckdb
