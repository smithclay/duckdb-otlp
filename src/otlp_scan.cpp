#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "ring_buffer.hpp"
#include "otlp_storage_info.hpp"
#include "otlp_receiver.hpp"

namespace duckdb {

struct OTLPScanBindData : public TableFunctionData {
	shared_ptr<RingBuffer> buffer;
	vector<string> column_names;
	vector<LogicalType> column_types;

	OTLPScanBindData() = default;
};

struct OTLPScanState : public GlobalTableFunctionState {
	vector<RingBuffer::Row> rows;
	idx_t current_row;

	OTLPScanState() : current_row(0) {
	}
};

// Note: The bind function is not used when scanning virtual tables
// The bind data is created directly by OTLPTableEntry::GetScanFunction()

//! Init global state - read all rows from ring buffer once
unique_ptr<GlobalTableFunctionState> OTLPScanInitGlobal(ClientContext &context,
                                                          TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<OTLPScanBindData>();
	auto state = make_uniq<OTLPScanState>();

	// Take snapshot of ring buffer contents (thread-safe)
	state->rows = bind_data.buffer->ReadAll();

	return std::move(state);
}

//! Scan function - return rows
void OTLPScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<OTLPScanState>();

	idx_t output_idx = 0;
	while (state.current_row < state.rows.size() && output_idx < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.current_row];

		// Set timestamp
		output.SetValue(0, output_idx, Value::TIMESTAMP(row.timestamp));

		// Set resource JSON
		output.SetValue(1, output_idx, Value(row.resource_json));

		// Set data JSON
		output.SetValue(2, output_idx, Value(row.data_json));

		state.current_row++;
		output_idx++;
	}

	output.SetCardinality(output_idx);
}

} // namespace duckdb
