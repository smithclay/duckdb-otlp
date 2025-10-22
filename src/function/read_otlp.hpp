#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "parsers/json_parser.hpp"
#include "parsers/protobuf_parser.hpp"
#include "parsers/format_detector.hpp"
#include "schema/otlp_types.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <deque>
#include <vector>
#include <unordered_map>

namespace duckdb {

enum class ReadOTLPOnError : uint8_t { FAIL = 0, SKIP = 1, NULLIFY = 2 };

//! read_otlp_*() table functions for reading OTLP files with strongly-typed schemas
class ReadOTLPTableFunction {
public:
	//! Get table functions for registration
	static TableFunction GetTracesFunction();
	static TableFunction GetLogsFunction();
	static TableFunction GetMetricsFunction();
	static TableFunction GetStatsFunction();

	//! Initialize scan state
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	//! Scan function to read and parse OTLP data
	static void Scan(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

//! Bind data for read_otlp function
struct ReadOTLPBindData : public TableFunctionData {
	string pattern;           // Glob pattern or single file path
	OTLPTableType table_type; // Detected table type for v2 schema
	ReadOTLPOnError on_error;

	explicit ReadOTLPBindData(string pattern_p, OTLPTableType type)
	    : pattern(std::move(pattern_p)), table_type(type), on_error(ReadOTLPOnError::FAIL) {
	}
};

//! Global state for reading OTLP files
struct ReadOTLPGlobalState : public GlobalTableFunctionState {
	struct OutputColumnInfo {
		column_t requested_id;
		idx_t chunk_index;
		bool is_row_id;
	};

	vector<string> files;
	idx_t next_file;
	OTLPTableType table_type;
	vector<LogicalType> all_types;

	unique_ptr<FileHandle> current_handle;
	unique_ptr<OTLPJSONParser> json_parser;
	unique_ptr<OTLPProtobufParser> protobuf_parser;

	OTLPFormat current_format;
	bool is_json_lines;
	bool doc_consumed;
	string current_path;

	// JSONL reader state
	string line_buffer;
	idx_t buffer_offset;
	idx_t current_line;
	idx_t approx_line;

	// Materialized chunks ready for scanning
	std::deque<unique_ptr<DataChunk>> chunk_queue;
	unique_ptr<DataChunk> active_chunk;

	// Column projection metadata
	vector<OutputColumnInfo> output_columns;
	vector<column_t> chunk_column_ids;
	vector<LogicalType> chunk_types;
	unique_ptr<TableFilterSet> filters;

	// Row-id tracking
	int64_t row_id_base;
	idx_t error_records;
	idx_t error_documents;
	ReadOTLPOnError on_error;
	bool stats_reported;

	bool finished;

	ReadOTLPGlobalState()
	    : next_file(0), table_type(OTLPTableType::TRACES), current_format(OTLPFormat::UNKNOWN), is_json_lines(false),
	      doc_consumed(false), buffer_offset(0), current_line(0), approx_line(0), row_id_base(0), error_records(0),
	      error_documents(0), on_error(ReadOTLPOnError::FAIL), stats_reported(false), finished(false) {
	}

	idx_t MaxThreads() const override {
		return files.empty() ? 1 : MinValue<idx_t>(files.size(), 8);
	}
};

} // namespace duckdb
