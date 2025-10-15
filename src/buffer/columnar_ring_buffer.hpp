#pragma once

#include "duckdb.hpp"
#include <deque>
#include <shared_mutex>

namespace duckdb {

//! Immutable chunk stored in the columnar buffer
struct ColumnarStoredChunk {
	unique_ptr<DataChunk> chunk; // chunk with initialized vectors
	idx_t size;                  // number of valid rows in chunk
	int64_t ts_min_us;           // min timestamp (microseconds) of column 0
	int64_t ts_max_us;           // max timestamp (microseconds) of column 0
	// Optional metadata for skipping
	bool svc_has = false;
	bool svc_mixed = false;
	string svc_value;
	bool met_has = false;
	bool met_mixed = false;
	string met_value;
};

//! Append-only, chunked columnar ring buffer with per-chunk metadata
class ColumnarRingBuffer {
public:
	ColumnarRingBuffer(const vector<LogicalType> &types, idx_t chunk_capacity = STANDARD_VECTOR_SIZE,
	                   idx_t max_chunks = 256, idx_t service_col_idx = DConstants::INVALID_INDEX,
	                   idx_t metric_col_idx = DConstants::INVALID_INDEX);

	//! Append a single row (expects values aligned with types)
	void AppendRow(const vector<Value> &row);

	//! Append multiple rows in one lock (expects values aligned with types)
	void AppendRows(const vector<vector<Value>> &rows);

	//! Append a chunk by copying (splits across chunks if needed)
	void AppendChunk(DataChunk &input);

	//! Appender for direct typed writes (holds the buffer lock for the batch)
	class Appender {
	public:
		explicit Appender(ColumnarRingBuffer &buf);
		~Appender();

		void BeginRow();
		void SetNull(idx_t col_idx);
		void SetTimestampNS(idx_t col_idx, timestamp_t val);
		void SetDouble(idx_t col_idx, double val);
		void SetUBigint(idx_t col_idx, uint64_t val);
		void SetBigint(idx_t col_idx, int64_t val);
		void SetInteger(idx_t col_idx, int32_t val);
		void SetUInteger(idx_t col_idx, uint32_t val);
		void SetBoolean(idx_t col_idx, bool val);
		void SetVarchar(idx_t col_idx, const string &val);
		void SetValue(idx_t col_idx, const Value &val);
		void CommitRow();

	private:
		void EnsureSpace();

	private:
		ColumnarRingBuffer &buf_;
		std::unique_lock<std::shared_mutex> lock_;
		int64_t row_ts_us_ = NumericLimits<int64_t>::Maximum();
	};

	Appender GetAppender() {
		return Appender(*this);
	}

	//! Take a snapshot of current immutable chunks for scanning
	vector<shared_ptr<const ColumnarStoredChunk>> Snapshot() const;

	//! Current total rows (approximate; snapshot for stable view)
	idx_t Size() const;

	const vector<LogicalType> &Types() const {
		return types_;
	}

private:
	void EnsureCurrentChunk();
	void FinalizeCurrentChunk();
	void FastSetValue(Vector &vec, idx_t row_idx, const Value &val);
	void UpdateCurrentServiceMetricFromValue(idx_t col_idx, const Value &val);
	void UpdateCurrentServiceFromString(const string &val);
	void UpdateCurrentMetricFromString(const string &val);

private:
	const vector<LogicalType> types_;
	const idx_t chunk_capacity_;
	const idx_t max_chunks_;
	const idx_t service_col_idx_;
	const idx_t metric_col_idx_;

	// Immutable finished chunks
	deque<shared_ptr<const ColumnarStoredChunk>> chunks_;

	// Mutable building chunk
	unique_ptr<DataChunk> current_chunk_;
	idx_t current_size_ = 0;
	int64_t current_ts_min_us_ = NumericLimits<int64_t>::Maximum();
	int64_t current_ts_max_us_ = NumericLimits<int64_t>::Minimum();
	bool current_svc_has_ = false;
	bool current_svc_mixed_ = false;
	string current_svc_value_;
	bool current_met_has_ = false;
	bool current_met_mixed_ = false;
	string current_met_value_;

	mutable std::shared_mutex mutex_;
};

} // namespace duckdb
