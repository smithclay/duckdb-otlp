#pragma once

#include "duckdb/function/table_function.hpp"
#include "ring_buffer.hpp"

namespace duckdb {

//! Bind data for metrics union scan - holds all 5 metric buffers
struct OTLPMetricsUnionScanBindData : public TableFunctionData {
	vector<shared_ptr<RingBuffer>> buffers; // All 5 metric buffers
	vector<string> column_names;
	vector<LogicalType> column_types;

	OTLPMetricsUnionScanBindData() = default;
};

//! Scan state for metrics union - holds transformed rows from all buffers
struct OTLPMetricsUnionScanState : public GlobalTableFunctionState {
	vector<vector<Value>> rows; // All rows transformed to union schema
	idx_t current_row;

	OTLPMetricsUnionScanState() : current_row(0) {
	}
};

//! Function declarations
unique_ptr<GlobalTableFunctionState> OTLPMetricsUnionScanInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input);
void OTLPMetricsUnionScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
