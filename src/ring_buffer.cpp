#include "ring_buffer.hpp"

namespace duckdb {

RingBuffer::RingBuffer(idx_t capacity) : capacity_(capacity), write_pos_(0), size_(0) {
	buffer_.reserve(capacity);
}

void RingBuffer::Insert(timestamp_t timestamp, const string &resource_json, const string &data_json) {
	std::unique_lock<std::shared_mutex> lock(mutex_);

	if (size_ < capacity_) {
		// Buffer not full yet - just append
		buffer_.emplace_back(timestamp, resource_json, data_json);
		size_++;
		// write_pos_ stays at 0 until buffer is full
	} else {
		// Buffer full - overwrite oldest (FIFO eviction)
		buffer_[write_pos_] = Row(timestamp, resource_json, data_json);
		// Move write position (circular)
		write_pos_ = (write_pos_ + 1) % capacity_;
	}
}

vector<RingBuffer::Row> RingBuffer::ReadAll() const {
	std::shared_lock<std::shared_mutex> lock(mutex_);

	// Return copy of all current rows
	// If buffer isn't full, only return valid entries
	if (size_ < capacity_) {
		return vector<Row>(buffer_.begin(), buffer_.begin() + size_);
	}

	// Buffer is full - return in chronological order (oldest first)
	// Oldest is at write_pos_, wrap around to write_pos_-1
	vector<Row> result;
	result.reserve(capacity_);

	for (idx_t i = 0; i < capacity_; i++) {
		idx_t idx = (write_pos_ + i) % capacity_;
		result.push_back(buffer_[idx]);
	}

	return result;
}

idx_t RingBuffer::Size() const {
	std::shared_lock<std::shared_mutex> lock(mutex_);
	return size_;
}

void RingBuffer::Clear() {
	std::unique_lock<std::shared_mutex> lock(mutex_);
	buffer_.clear();
	write_pos_ = 0;
	size_ = 0;
}

} // namespace duckdb
