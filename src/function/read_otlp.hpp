#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "parsers/json_parser.hpp"
#include "parsers/protobuf_parser.hpp"
#include "parsers/format_detector.hpp"
#include "schema/otlp_types.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <atomic>
#include <deque>
#include <vector>
#include <unordered_map>

namespace duckdb {

enum class ReadOTLPOnError : uint8_t { FAIL = 0, SKIP = 1, NULLIFY = 2 };

//! Simple optional-like wrapper for compatibility with WASM builds (no C++17)
template <typename T>
struct Optional {
	T value;
	bool has_value;

	Optional() : value(T()), has_value(false) {
	}
	explicit Optional(const T &val) : value(val), has_value(true) {
	}

	// Assignment operator
	Optional &operator=(const T &val) {
		value = val;
		has_value = true;
		return *this;
	}

	// Bool conversion operator (implicit for use in if statements)
	operator bool() const {
		return has_value;
	}

	// Dereference operator
	const T &operator*() const {
		return value;
	}
	T &operator*() {
		return value;
	}

	bool HasValue() const {
		return has_value;
	}
	const T &Value() const {
		return value;
	}
	T &Value() {
		return value;
	}
};

static constexpr int64_t READ_OTLP_DEFAULT_MAX_DOCUMENT_BYTES = static_cast<int64_t>(100) * 1024 * 1024;

//! read_otlp_*() table functions for reading OTLP files with strongly-typed schemas
class ReadOTLPTableFunction {
public:
	//! Get table functions for registration
	static TableFunction GetTracesFunction();
	static TableFunction GetLogsFunction();
	static TableFunction GetMetricsFunction();
	static TableFunction GetMetricsGaugeFunction();
	static TableFunction GetMetricsSumFunction();
	static TableFunction GetMetricsHistogramFunction();
	static TableFunction GetMetricsExpHistogramFunction();
	static TableFunction GetMetricsSummaryFunction();
	static TableFunction GetStatsFunction();
	static TableFunction GetOptionsFunction();

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
	Optional<OTLPMetricType> metric_filter;
	int64_t max_document_bytes;

	explicit ReadOTLPBindData(string pattern_p, OTLPTableType type)
	    : pattern(std::move(pattern_p)), table_type(type), on_error(ReadOTLPOnError::FAIL), metric_filter(),
	      max_document_bytes(READ_OTLP_DEFAULT_MAX_DOCUMENT_BYTES) {
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
	std::atomic<idx_t> next_file {0};
	OTLPTableType table_type;
	vector<LogicalType> all_types;
	vector<OutputColumnInfo> output_columns;
	vector<column_t> chunk_column_ids;
	vector<LogicalType> chunk_types;
	shared_ptr<TableFilterSet> filters;
	ReadOTLPOnError on_error;
	std::atomic<int64_t> next_row_id {0};
	std::atomic<idx_t> error_records {0};
	std::atomic<idx_t> error_documents {0};
	std::atomic<idx_t> active_workers {0};
	std::atomic<bool> stats_reported {false};
	Optional<OTLPMetricType> metric_filter;
	int64_t max_document_bytes;

	ReadOTLPGlobalState()
	    : table_type(OTLPTableType::TRACES), on_error(ReadOTLPOnError::FAIL),
	      max_document_bytes(READ_OTLP_DEFAULT_MAX_DOCUMENT_BYTES) {
	}

	idx_t MaxThreads() const override {
		return files.empty() ? 1 : MinValue<idx_t>(files.size(), 8);
	}
};

struct ReadOTLPLocalState : public LocalTableFunctionState {
	unique_ptr<FileHandle> current_handle;
	unique_ptr<OTLPJSONParser> json_parser;
	unique_ptr<OTLPProtobufParser> protobuf_parser;
	OTLPFormat current_format;
	bool is_json_lines;
	bool doc_consumed;
	string current_path;
	string line_buffer;
	idx_t buffer_offset;
	idx_t current_line;
	idx_t approx_line;
	std::deque<unique_ptr<DataChunk>> chunk_queue;
	unique_ptr<DataChunk> active_chunk;
	bool reported_completion;
	ClientContext *context;

	ReadOTLPLocalState()
	    : current_format(OTLPFormat::UNKNOWN), is_json_lines(false), doc_consumed(false), buffer_offset(0),
	      current_line(0), approx_line(0), reported_completion(false), context(nullptr) {
	}
};

} // namespace duckdb
