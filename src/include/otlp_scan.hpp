#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "ring_buffer.hpp"

namespace duckdb {

//! Bind data for OTLP virtual table scans
struct OTLPScanBindData : public TableFunctionData {
	shared_ptr<RingBuffer> buffer;
	vector<string> column_names;
	vector<LogicalType> column_types;

	OTLPScanBindData() = default;
};

//! Global state for OTLP scans
struct OTLPScanState : public GlobalTableFunctionState {
	vector<RingBuffer::Row> rows;
	idx_t current_row;

	OTLPScanState() : current_row(0) {
	}
};

//! Init global state - read all rows from ring buffer once
unique_ptr<GlobalTableFunctionState> OTLPScanInitGlobal(ClientContext &context, TableFunctionInitInput &input);

//! Scan function - outputs rows to data chunk
void OTLPScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
