#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "json_parser.hpp"
#include "protobuf_parser.hpp"
#include "format_detector.hpp"

namespace duckdb {

//! read_otlp() table function for reading OTLP JSON Lines files
class ReadOTLPTableFunction {
public:
	//! Register the read_otlp table function
	static void RegisterFunction(DatabaseInstance &db);

	//! Get the table function (for registration via ExtensionLoader)
	static TableFunction GetFunction();

	//! Bind function data
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                      vector<LogicalType> &return_types, vector<string> &names);

	//! Initialize scan state
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	//! Scan function to read and parse OTLP data
	static void Scan(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

//! Bind data for read_otlp function
struct ReadOTLPBindData : public TableFunctionData {
	string file_path;

	ReadOTLPBindData(string path) : file_path(std::move(path)) {
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

	// For protobuf parsing
	vector<timestamp_t> timestamps;
	vector<string> resources;
	vector<string> datas;
	idx_t current_row;

	ReadOTLPGlobalState()
		: format(OTLPFormat::UNKNOWN), current_line(0), finished(false),
		  buffer_offset(0), skipped_lines(0), warning_emitted(false), current_row(0) {
	}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for Phase 2
	}
};

} // namespace duckdb
