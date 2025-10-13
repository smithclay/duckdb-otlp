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

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.current_row);

	if (count == 0) {
		output.SetCardinality(0);
		return;
	}

	// Each row is a vector<Value> with one Value per column
	// Copy values from rows into output DataChunk columns
	idx_t column_count = output.ColumnCount();

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		auto &vec = output.data[col_idx];

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto &row = state.rows[state.current_row + row_idx];

			// Safety check: ensure row has enough columns
			if (col_idx < row.size()) {
				// Copy Value to vector at position row_idx
				auto &value = row[col_idx];
				vec.SetValue(row_idx, value);
			} else {
				// Missing column - set to NULL
				FlatVector::SetNull(vec, row_idx, true);
			}
		}
	}

	state.current_row += count;
	output.SetCardinality(count);
}

} // namespace duckdb
