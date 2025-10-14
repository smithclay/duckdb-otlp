#pragma once

#include "duckdb.hpp"
#include "columnar_ring_buffer.hpp"

namespace duckdb {

struct OTLPColumnarScanBindData : public TableFunctionData {
	shared_ptr<ColumnarRingBuffer> buffer;
	vector<string> column_names;
	vector<LogicalType> column_types;
};

struct OTLPColumnarScanState : public GlobalTableFunctionState {
	vector<shared_ptr<const ColumnarStoredChunk>> snapshot;
	vector<idx_t> out_to_base;          // output column -> base column index
	unique_ptr<TableFilterSet> filters; // pushed filters
	// precomputed timestamp bounds from filters (microseconds)
	std::optional<int64_t> ts_min_us;
	std::optional<int64_t> ts_max_us;
	// precomputed equals filters for service/metric
	std::optional<string> service_eq;
	std::optional<string> metric_eq;

	// parallel state
	std::atomic<idx_t> next_chunk {0};

	idx_t MaxThreads() const override {
		return snapshot.empty() ? 1 : snapshot.size();
	}
};

struct OTLPColumnarLocalState : public LocalTableFunctionState {
	idx_t chunk_idx = DConstants::INVALID_INDEX;
	idx_t row_offset = 0;
	// filtered selection for current chunk
	vector<sel_t> sel_matches;
	idx_t sel_count = 0;
	idx_t sel_pos = 0;
};

unique_ptr<GlobalTableFunctionState> OTLPColumnarScanInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> OTLPColumnarScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state);
void OTLPColumnarScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
