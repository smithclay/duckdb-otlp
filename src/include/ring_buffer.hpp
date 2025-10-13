#pragma once

#include "duckdb.hpp"
#include <vector>
#include <mutex>
#include <shared_mutex>

namespace duckdb {

//! Thread-safe ring buffer for OTLP telemetry data
//! Stores rows with strongly-typed columns (vector of Values per row)
class RingBuffer {
public:
	//! A row is a vector of Values, one per column
	//! The schema (column count and types) is determined by the table type
	using Row = vector<Value>;

	explicit RingBuffer(idx_t capacity);
	~RingBuffer() = default;

	//! Insert a new row (thread-safe, FIFO eviction when full)
	//! The row must match the schema expected by the ring buffer
	void Insert(const Row &row);

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
