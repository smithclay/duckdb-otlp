#pragma once

#include "duckdb.hpp"
#include <vector>
#include <mutex>
#include <shared_mutex>

namespace duckdb {

//! Thread-safe ring buffer for OTLP telemetry data
//! Stores rows with schema: (timestamp TIMESTAMP, resource JSON, data JSON)
class RingBuffer {
public:
	struct Row {
		timestamp_t timestamp;
		string resource_json;
		string data_json;

		Row(timestamp_t ts, const string &res, const string &data)
		    : timestamp(ts), resource_json(res), data_json(data) {
		}
	};

	explicit RingBuffer(idx_t capacity);
	~RingBuffer() = default;

	//! Insert a new row (thread-safe, FIFO eviction when full)
	void Insert(timestamp_t timestamp, const string &resource_json, const string &data_json);

	//! Read all current rows (thread-safe snapshot)
	vector<Row> ReadAll() const;

	//! Get current number of rows
	idx_t Size() const;

	//! Get ring buffer capacity
	idx_t Capacity() const {
		return capacity_;
	}

	//! Clear all rows
	void Clear();

private:
	const idx_t capacity_;
	vector<Row> buffer_;
	idx_t write_pos_;
	idx_t size_;
	mutable std::shared_mutex mutex_; // Allows multiple readers, single writer
};

} // namespace duckdb
