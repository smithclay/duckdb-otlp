#include "buffer/columnar_ring_buffer.hpp"

namespace duckdb {

ColumnarRingBuffer::ColumnarRingBuffer(const vector<LogicalType> &types, idx_t chunk_capacity, idx_t max_chunks,
                                       idx_t service_col_idx, idx_t metric_col_idx)
    : types_(types), chunk_capacity_(chunk_capacity), max_chunks_(MaxValue<idx_t>(1, max_chunks)),
      service_col_idx_(service_col_idx), metric_col_idx_(metric_col_idx) {
}

void ColumnarRingBuffer::FastSetValue(Vector &vec, idx_t row_idx, const Value &val) {
	if (val.IsNull()) {
		FlatVector::SetNull(vec, row_idx, true);
		return;
	}
	switch (vec.GetType().id()) {
	case LogicalTypeId::TIMESTAMP_NS: {
		auto data = FlatVector::GetData<timestamp_t>(vec);
		data[row_idx] = val.GetValue<timestamp_t>();
		break;
	}
	case LogicalTypeId::DOUBLE: {
		auto data = FlatVector::GetData<double>(vec);
		data[row_idx] = val.GetValue<double>();
		break;
	}
	case LogicalTypeId::UBIGINT: {
		auto data = FlatVector::GetData<uint64_t>(vec);
		data[row_idx] = val.GetValue<uint64_t>();
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto data = FlatVector::GetData<int64_t>(vec);
		data[row_idx] = val.GetValue<int64_t>();
		break;
	}
	case LogicalTypeId::INTEGER: {
		auto data = FlatVector::GetData<int32_t>(vec);
		data[row_idx] = val.GetValue<int32_t>();
		break;
	}
	case LogicalTypeId::UINTEGER: {
		auto data = FlatVector::GetData<uint32_t>(vec);
		data[row_idx] = val.GetValue<uint32_t>();
		break;
	}
	case LogicalTypeId::BOOLEAN: {
		auto data = FlatVector::GetData<bool>(vec);
		data[row_idx] = val.GetValue<bool>();
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto s = val.ToString();
		auto str = StringVector::AddString(vec, s);
		FlatVector::GetData<string_t>(vec)[row_idx] = str;
		break;
	}
	default:
		vec.SetValue(row_idx, val);
		break;
	}
}

void ColumnarRingBuffer::EnsureCurrentChunk() {
	if (current_chunk_ && current_size_ < chunk_capacity_) {
		return;
	}
	if (current_chunk_ && current_size_ >= chunk_capacity_) {
		FinalizeCurrentChunk();
	}
	current_chunk_ = make_uniq<DataChunk>();
	current_chunk_->Initialize(Allocator::DefaultAllocator(), types_, chunk_capacity_);
	current_size_ = 0;
	current_ts_min_us_ = NumericLimits<int64_t>::Maximum();
	current_ts_max_us_ = NumericLimits<int64_t>::Minimum();
	current_svc_has_ = false;
	current_svc_mixed_ = false;
	current_svc_value_.clear();
	current_met_has_ = false;
	current_met_mixed_ = false;
	current_met_value_.clear();
}

void ColumnarRingBuffer::FinalizeCurrentChunk() {
	if (!current_chunk_ || current_size_ == 0) {
		return;
	}
	current_chunk_->SetCardinality(current_size_);
	auto stored = make_shared_ptr<ColumnarStoredChunk>();
	stored->chunk = std::move(current_chunk_);
	stored->size = current_size_;
	stored->ts_min_us = current_ts_min_us_;
	stored->ts_max_us = current_ts_max_us_;
	stored->svc_has = current_svc_has_;
	stored->svc_mixed = current_svc_mixed_;
	stored->svc_value = current_svc_value_;
	stored->met_has = current_met_has_;
	stored->met_mixed = current_met_mixed_;
	stored->met_value = current_met_value_;
	current_chunk_.reset();
	current_size_ = 0;
	// push into ring, evict oldest if exceed max_chunks_
	chunks_.push_back(stored);
	if (chunks_.size() > max_chunks_) {
		chunks_.pop_front();
	}
}

void ColumnarRingBuffer::UpdateCurrentServiceFromString(const string &val) {
	if (service_col_idx_ == DConstants::INVALID_INDEX)
		return;
	if (!current_svc_has_) {
		current_svc_has_ = true;
		current_svc_value_ = val;
	} else if (!current_svc_mixed_ && current_svc_value_ != val) {
		current_svc_mixed_ = true;
	}
}

void ColumnarRingBuffer::UpdateCurrentMetricFromString(const string &val) {
	if (metric_col_idx_ == DConstants::INVALID_INDEX)
		return;
	if (!current_met_has_) {
		current_met_has_ = true;
		current_met_value_ = val;
	} else if (!current_met_mixed_ && current_met_value_ != val) {
		current_met_mixed_ = true;
	}
}

void ColumnarRingBuffer::UpdateCurrentServiceMetricFromValue(idx_t col_idx, const Value &val) {
	if (val.IsNull())
		return;
	if (col_idx == service_col_idx_) {
		UpdateCurrentServiceFromString(val.ToString());
	} else if (col_idx == metric_col_idx_) {
		UpdateCurrentMetricFromString(val.ToString());
	}
}

void ColumnarRingBuffer::AppendRow(const vector<Value> &row) {
	std::unique_lock<std::shared_mutex> lock(mutex_);
	EnsureCurrentChunk();
	for (idx_t c = 0; c < types_.size(); c++) {
		auto &vec = current_chunk_->data[c];
		if (c < row.size()) {
			FastSetValue(vec, current_size_, row[c]);
			UpdateCurrentServiceMetricFromValue(c, row[c]);
		} else {
			FlatVector::SetNull(vec, current_size_, true);
		}
	}
	// update timestamp min/max (assume col 0 is timestamp)
	if (!row.empty() && !row[0].IsNull()) {
		auto ts = row[0].GetValue<timestamp_t>();
		auto us = Timestamp::GetEpochMicroSeconds(ts);
		if (us < current_ts_min_us_)
			current_ts_min_us_ = us;
		if (us > current_ts_max_us_)
			current_ts_max_us_ = us;
	}
	current_size_++;
	if (current_size_ >= chunk_capacity_) {
		FinalizeCurrentChunk();
	}
}

void ColumnarRingBuffer::AppendRows(const vector<vector<Value>> &rows) {
	std::unique_lock<std::shared_mutex> lock(mutex_);
	for (auto &row : rows) {
		EnsureCurrentChunk();
		for (idx_t c = 0; c < types_.size(); c++) {
			auto &vec = current_chunk_->data[c];
			if (c < row.size()) {
				FastSetValue(vec, current_size_, row[c]);
				UpdateCurrentServiceMetricFromValue(c, row[c]);
			} else {
				FlatVector::SetNull(vec, current_size_, true);
			}
		}
		if (!row.empty() && !row[0].IsNull()) {
			auto ts = row[0].GetValue<timestamp_t>();
			auto us = Timestamp::GetEpochMicroSeconds(ts);
			if (us < current_ts_min_us_)
				current_ts_min_us_ = us;
			if (us > current_ts_max_us_)
				current_ts_max_us_ = us;
		}
		current_size_++;
		if (current_size_ >= chunk_capacity_) {
			FinalizeCurrentChunk();
		}
	}
}

void ColumnarRingBuffer::AppendChunk(DataChunk &input) {
	std::unique_lock<std::shared_mutex> lock(mutex_);
	idx_t offset = 0;
	while (offset < input.size()) {
		EnsureCurrentChunk();
		idx_t space = chunk_capacity_ - current_size_;
		idx_t to_copy = MinValue<idx_t>(space, input.size() - offset);
		// copy per column using vectorized copy
		for (idx_t c = 0; c < types_.size(); c++) {
			VectorOperations::Copy(input.data[c], current_chunk_->data[c], to_copy, offset, current_size_);
		}
		// update ts min/max from copied window
		auto &ts_vec = input.data[0];
		for (idx_t i = 0; i < to_copy; i++) {
			auto val = ts_vec.GetValue(offset + i);
			if (!val.IsNull()) {
				auto us = Timestamp::GetEpochMicroSeconds(val.GetValue<timestamp_t>());
				if (us < current_ts_min_us_)
					current_ts_min_us_ = us;
				if (us > current_ts_max_us_)
					current_ts_max_us_ = us;
			}
		}
		// update service/metric meta from input if indices set
		if (service_col_idx_ != DConstants::INVALID_INDEX) {
			auto &svc_vec = input.data[service_col_idx_];
			for (idx_t i = 0; i < to_copy; i++) {
				auto v = svc_vec.GetValue(offset + i);
				if (!v.IsNull())
					UpdateCurrentServiceMetricFromValue(service_col_idx_, v);
			}
		}
		if (metric_col_idx_ != DConstants::INVALID_INDEX) {
			auto &met_vec = input.data[metric_col_idx_];
			for (idx_t i = 0; i < to_copy; i++) {
				auto v = met_vec.GetValue(offset + i);
				if (!v.IsNull())
					UpdateCurrentServiceMetricFromValue(metric_col_idx_, v);
			}
		}
		current_size_ += to_copy;
		if (current_size_ >= chunk_capacity_) {
			FinalizeCurrentChunk();
		}
		offset += to_copy;
	}
}

vector<shared_ptr<const ColumnarStoredChunk>> ColumnarRingBuffer::Snapshot() const {
	std::shared_lock<std::shared_mutex> lock(mutex_);
	vector<shared_ptr<const ColumnarStoredChunk>> result;
	result.reserve(chunks_.size() + 1);
	for (auto &ptr : chunks_) {
		result.push_back(ptr);
	}
	// include current in-flight chunk as a sealed shallow copy
	if (current_chunk_ && current_size_ > 0) {
		auto temp = make_shared_ptr<ColumnarStoredChunk>();
		temp->chunk = make_uniq<DataChunk>();
		temp->chunk->Initialize(Allocator::DefaultAllocator(), types_, current_size_);
		// Copy current rows into a temporary immutable chunk
		for (idx_t c = 0; c < types_.size(); c++) {
			auto &src = current_chunk_->data[c];
			auto &dst = temp->chunk->data[c];
			for (idx_t r = 0; r < current_size_; r++) {
				// copy via Value to handle complex types safely
				dst.SetValue(r, src.GetValue(r));
			}
		}
		temp->chunk->SetCardinality(current_size_);
		temp->size = current_size_;
		temp->ts_min_us = current_ts_min_us_;
		temp->ts_max_us = current_ts_max_us_;
		result.push_back(temp);
	}
	return result;
}

idx_t ColumnarRingBuffer::Size() const {
	std::shared_lock<std::shared_mutex> lock(mutex_);
	idx_t total = 0;
	for (auto &c : chunks_)
		total += c->size;
	total += current_size_;
	return total;
}

// ---- Appender implementation ----

ColumnarRingBuffer::Appender::Appender(ColumnarRingBuffer &buf) : buf_(buf), lock_(buf_.mutex_) {
	// ensure a current chunk exists
	buf_.EnsureCurrentChunk();
}

ColumnarRingBuffer::Appender::~Appender() {
	// no-op: lock_ unlocks automatically
}

void ColumnarRingBuffer::Appender::EnsureSpace() {
	if (buf_.current_size_ >= buf_.chunk_capacity_) {
		buf_.FinalizeCurrentChunk();
		buf_.EnsureCurrentChunk();
	}
}

void ColumnarRingBuffer::Appender::BeginRow() {
	EnsureSpace();
	row_ts_us_ = NumericLimits<int64_t>::Maximum();
}

void ColumnarRingBuffer::Appender::SetNull(idx_t col_idx) {
	EnsureSpace();
	FlatVector::SetNull(buf_.current_chunk_->data[col_idx], buf_.current_size_, true);
}

void ColumnarRingBuffer::Appender::SetTimestampNS(idx_t col_idx, timestamp_t val) {
	EnsureSpace();
	auto &vec = buf_.current_chunk_->data[col_idx];
	auto data = FlatVector::GetData<timestamp_t>(vec);
	data[buf_.current_size_] = val;
	if (col_idx == 0) {
		row_ts_us_ = Timestamp::GetEpochMicroSeconds(val);
	}
}

void ColumnarRingBuffer::Appender::SetDouble(idx_t col_idx, double val) {
	EnsureSpace();
	FlatVector::GetData<double>(buf_.current_chunk_->data[col_idx])[buf_.current_size_] = val;
}

void ColumnarRingBuffer::Appender::SetUBigint(idx_t col_idx, uint64_t val) {
	EnsureSpace();
	FlatVector::GetData<uint64_t>(buf_.current_chunk_->data[col_idx])[buf_.current_size_] = val;
}

void ColumnarRingBuffer::Appender::SetBigint(idx_t col_idx, int64_t val) {
	EnsureSpace();
	FlatVector::GetData<int64_t>(buf_.current_chunk_->data[col_idx])[buf_.current_size_] = val;
}

void ColumnarRingBuffer::Appender::SetInteger(idx_t col_idx, int32_t val) {
	EnsureSpace();
	FlatVector::GetData<int32_t>(buf_.current_chunk_->data[col_idx])[buf_.current_size_] = val;
}

void ColumnarRingBuffer::Appender::SetUInteger(idx_t col_idx, uint32_t val) {
	EnsureSpace();
	FlatVector::GetData<uint32_t>(buf_.current_chunk_->data[col_idx])[buf_.current_size_] = val;
}

void ColumnarRingBuffer::Appender::SetBoolean(idx_t col_idx, bool val) {
	EnsureSpace();
	FlatVector::GetData<bool>(buf_.current_chunk_->data[col_idx])[buf_.current_size_] = val;
}

void ColumnarRingBuffer::Appender::SetVarchar(idx_t col_idx, const string &val) {
	EnsureSpace();
	auto &vec = buf_.current_chunk_->data[col_idx];
	auto str = StringVector::AddString(vec, val);
	FlatVector::GetData<string_t>(vec)[buf_.current_size_] = str;
}

void ColumnarRingBuffer::Appender::SetValue(idx_t col_idx, const Value &val) {
	EnsureSpace();
	auto &vec = buf_.current_chunk_->data[col_idx];
	vec.SetValue(buf_.current_size_, val);
}

void ColumnarRingBuffer::Appender::CommitRow() {
	// update ts min/max if provided
	if (row_ts_us_ != NumericLimits<int64_t>::Maximum()) {
		if (row_ts_us_ < buf_.current_ts_min_us_)
			buf_.current_ts_min_us_ = row_ts_us_;
		if (row_ts_us_ > buf_.current_ts_max_us_)
			buf_.current_ts_max_us_ = row_ts_us_;
	}
	buf_.current_size_++;
	if (buf_.current_size_ >= buf_.chunk_capacity_) {
		buf_.FinalizeCurrentChunk();
		buf_.EnsureCurrentChunk();
	}
}

} // namespace duckdb
