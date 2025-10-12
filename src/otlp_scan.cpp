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
unique_ptr<GlobalTableFunctionState> OTLPScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
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
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.current_row);

	if (count == 0) {
		output.SetCardinality(0);
		return;
	}

	// Get vectors for each column
	auto timestamp_data = FlatVector::GetData<timestamp_t>(output.data[0]);
	auto &resource_vector = output.data[1];
	auto &data_vector = output.data[2];

	// Fill vectors
	for (idx_t i = 0; i < count; i++) {
		auto &row = state.rows[state.current_row + i];

		// Set timestamp
		timestamp_data[i] = row.timestamp;

		// Set resource JSON
		FlatVector::GetData<string_t>(resource_vector)[i] = StringVector::AddString(resource_vector, row.resource_json);

		// Set data JSON
		FlatVector::GetData<string_t>(data_vector)[i] = StringVector::AddString(data_vector, row.data_json);
	}

	state.current_row += count;
	output.SetCardinality(count);
}

} // namespace duckdb
