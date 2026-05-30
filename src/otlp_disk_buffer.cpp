#include "otlp_disk_buffer.hpp"

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"

#include <condition_variable>
#include <deque>
#include <future>
#include <map>
#include <set>
#include <sstream>

namespace duckdb {
namespace {

struct DiskSegmentState {
	idx_t bytes = 0;
	idx_t record_count = 0;
	uint64_t seq_lo = 0;
	uint64_t seq_hi = 0;
	// Number of not-yet-checkpointed records still living in this segment. Maintained alongside `pending` so that
	// SegmentHasPending() is O(1) instead of scanning all pending entries.
	idx_t pending_count = 0;
};

struct AppendResult {
	bool ok = false;
	DiskRecordId id;
	OtlpDiskBufferErrorType error_type = OtlpDiskBufferErrorType::UNAVAILABLE;
	string error;
};

struct Command {
	enum class Type : uint8_t { APPEND, CHECKPOINT_RECORD, CHECKPOINT_SEALED, RECOVER_PENDING, STOP };

	Type type;
	DiskRecord record;
	DiskRecordId id;
	vector<uint64_t> sequences;
	std::promise<AppendResult> append_reply;
	std::promise<vector<RecoveredDiskRecord>> recover_reply;
	std::promise<void> void_reply;
	bool has_reply = false;
};

static string JsonEscape(const string &input) {
	string result;
	result.reserve(input.size() + 8);
	for (auto c : input) {
		switch (c) {
		case '\\':
			result += "\\\\";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			result += c;
			break;
		}
	}
	return result;
}

// Minimal hand-rolled manifest JSON reader. This naive `"field":` substring search is only safe because every
// value we write into the manifest is JsonEscape'd, so a literal `"field":` token can never appear inside a string
// value and be mistaken for a real field. It must therefore ONLY be used on trusted, self-written manifest content
// (the manifest.json this extension produces) -- never on arbitrary/untrusted JSON.
static idx_t FindFieldValue(const string &json, const string &field) {
	auto needle = "\"" + field + "\":";
	auto pos = json.find(needle);
	if (pos == string::npos) {
		throw IOException("OTLP disk buffer manifest missing field %s", field);
	}
	return pos + needle.size();
}

static string ParseJsonStringField(const string &json, const string &field) {
	auto pos = FindFieldValue(json, field);
	if (pos >= json.size() || json[pos] != '"') {
		throw IOException("OTLP disk buffer manifest field %s is not a string", field);
	}
	pos++;
	string result;
	while (pos < json.size()) {
		auto c = json[pos++];
		if (c == '"') {
			return result;
		}
		if (c == '\\') {
			if (pos >= json.size()) {
				throw IOException("OTLP disk buffer manifest has invalid escape");
			}
			auto esc = json[pos++];
			switch (esc) {
			case '"':
			case '\\':
				result.push_back(esc);
				break;
			case 'n':
				result.push_back('\n');
				break;
			case 'r':
				result.push_back('\r');
				break;
			case 't':
				result.push_back('\t');
				break;
			default:
				throw IOException("OTLP disk buffer manifest has unsupported escape");
			}
		} else {
			result.push_back(c);
		}
	}
	throw IOException("OTLP disk buffer manifest has unterminated string");
}

static void WriteAll(FileHandle &handle, const data_t *data, idx_t len) {
	idx_t written = 0;
	while (written < len) {
		auto nwrite = handle.Write(const_cast<data_t *>(data + written), len - written);
		if (nwrite <= 0) {
			throw IOException("Could not append OTLP disk buffer record");
		}
		written += static_cast<idx_t>(nwrite);
	}
}

static void WriteAll(FileHandle &handle, const string &data) {
	if (data.empty()) {
		return;
	}
	WriteAll(handle, reinterpret_cast<const data_t *>(data.data()), data.size());
}

static OtlpDiskBufferStats MergeStats(const OtlpDiskBufferStats &left, const OtlpDiskBufferStats &right) {
	auto result = left;
	result.buffered_bytes += right.buffered_bytes;
	result.segment_bytes += right.segment_bytes;
	result.segments += right.segments;
	result.pending_records += right.pending_records;
	result.journal_writes_total += right.journal_writes_total;
	result.journal_fsync_total += right.journal_fsync_total;
	result.journal_fsync_failures_total += right.journal_fsync_failures_total;
	if (result.oldest_unsealed_seq < 0) {
		result.oldest_unsealed_seq = right.oldest_unsealed_seq;
	} else if (right.oldest_unsealed_seq >= 0) {
		result.oldest_unsealed_seq = MinValue<int64_t>(result.oldest_unsealed_seq, right.oldest_unsealed_seq);
	}
	result.healthy = result.healthy && right.healthy;
	if (!right.last_error.empty()) {
		if (!result.last_error.empty()) {
			result.last_error += "; ";
		}
		result.last_error += right.last_error;
	}
	return result;
}

} // namespace

OtlpDiskBufferException::OtlpDiskBufferException(OtlpDiskBufferErrorType type_p, string message_p)
    : type(type_p), message(std::move(message_p)) {
}

class DiskSegmentWriter::Impl {
public:
	Impl(OtlpRequestKind kind_p, string dir_p, idx_t max_segment_bytes_p, idx_t max_record_bytes_p,
	     idx_t max_total_bytes_p)
	    : kind(kind_p), dir(std::move(dir_p)),
	      max_segment_bytes(MaxValue<idx_t>(max_segment_bytes_p, OTLP_DISK_RECORD_HEADER_BYTES + 1)),
	      max_record_bytes(MaxValue<idx_t>(max_record_bytes_p, 1)),
	      max_total_bytes(MaxValue<idx_t>(max_total_bytes_p, 1)),
	      checkpoint_fsync_records(OTLP_DISK_CHECKPOINT_FSYNC_RECORDS),
	      checkpoint_fsync_delay(std::chrono::milliseconds(OTLP_DISK_CHECKPOINT_FSYNC_DELAY_MS)) {
		fs = FileSystem::CreateLocal();
		Open();
		thread = std::thread([this] { Run(); });
	}

	~Impl() {
		Shutdown();
	}

	DiskRecordId Append(const DiskRecord &record) {
		auto command = make_uniq<Command>();
		command->type = Command::Type::APPEND;
		command->record = record;
		auto future = command->append_reply.get_future();
		EnqueueAppend(std::move(command));
		auto result = future.get();
		if (!result.ok) {
			throw OtlpDiskBufferException(result.error_type, result.error);
		}
		return result.id;
	}

	vector<RecoveredDiskRecord> RecoverPending() {
		auto command = make_uniq<Command>();
		command->type = Command::Type::RECOVER_PENDING;
		auto future = command->recover_reply.get_future();
		EnqueueBlocking(std::move(command));
		return future.get();
	}

	void CheckpointRecord(DiskRecordId id) {
		auto command = make_uniq<Command>();
		command->type = Command::Type::CHECKPOINT_RECORD;
		command->id = id;
		command->has_reply = true;
		auto future = command->void_reply.get_future();
		EnqueueBlocking(std::move(command));
		future.get();
	}

	void CheckpointSealed(const vector<uint64_t> &sequences) {
		auto command = make_uniq<Command>();
		command->type = Command::Type::CHECKPOINT_SEALED;
		command->sequences = sequences;
		EnqueueBlocking(std::move(command));
	}

	// Lock-free metrics read. Stats() is called from the DuckDB query thread (otlp_server_list) while the writer
	// thread runs, so it must never enqueue a command (which would block behind queued appends/fsyncs). All values
	// are read from atomics the writer thread publishes, plus a brief lock on stats_mutex for the error strings.
	OtlpDiskBufferStats Stats() {
		OtlpDiskBufferStats stats;
		stats.buffered_bytes = total_pending_bytes.load(std::memory_order_acquire);
		stats.segment_bytes = total_segment_bytes.load(std::memory_order_acquire);
		stats.segments = segment_count.load(std::memory_order_acquire);
		stats.pending_records = pending_count.load(std::memory_order_acquire);
		stats.journal_writes_total = journal_writes_total.load(std::memory_order_acquire);
		stats.journal_fsync_total = journal_fsync_total.load(std::memory_order_acquire);
		stats.journal_fsync_failures_total = journal_fsync_failures_total.load(std::memory_order_acquire);
		stats.oldest_unsealed_seq = oldest_unsealed_seq.load(std::memory_order_acquire);
		stats.healthy = healthy.load(std::memory_order_acquire);
		{
			std::lock_guard<std::mutex> lock(stats_mutex);
			stats.last_error = fatal_error.empty() ? last_error : fatal_error;
		}
		return stats;
	}

	void Shutdown() {
		if (shutdown.exchange(true)) {
			return;
		}
		auto command = make_uniq<Command>();
		command->type = Command::Type::STOP;
		command->has_reply = true;
		auto future = command->void_reply.get_future();
		{
			std::lock_guard<std::mutex> lock(queue_mutex);
			queue.push_back(std::move(command));
		}
		queue_cv.notify_one();
		try {
			future.get();
		} catch (...) {
		}
		if (thread.joinable()) {
			thread.join();
		}
	}

private:
	void EnqueueAppend(unique_ptr<Command> command) {
		std::lock_guard<std::mutex> lock(queue_mutex);
		if (shutdown.load()) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, "OTLP disk buffer writer is stopped");
		}
		if (!healthy.load(std::memory_order_acquire)) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, FatalError());
		}
		if (queue.size() >= OTLP_DISK_WRITER_QUEUE_CAPACITY) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::QUEUE_FULL, "OTLP disk buffer writer queue is full");
		}
		queue.push_back(std::move(command));
		queue_cv.notify_one();
	}

	void EnqueueBlocking(unique_ptr<Command> command) {
		std::unique_lock<std::mutex> lock(queue_mutex);
		if (shutdown.load()) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, "OTLP disk buffer writer is stopped");
		}
		queue_cv.wait(lock, [&] { return queue.size() < OTLP_DISK_WRITER_QUEUE_CAPACITY || shutdown.load(); });
		if (shutdown.load()) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, "OTLP disk buffer writer is stopped");
		}
		queue.push_back(std::move(command));
		lock.unlock();
		queue_cv.notify_one();
	}

	void Run() {
		while (true) {
			unique_ptr<Command> command;
			{
				std::unique_lock<std::mutex> lock(queue_mutex);
				auto timeout = CheckpointSyncDueIn();
				if (timeout.has_value()) {
					queue_cv.wait_for(lock, timeout.value(), [&] { return !queue.empty(); });
				} else {
					queue_cv.wait(lock, [&] { return !queue.empty(); });
				}
				if (queue.empty()) {
					try {
						SyncCheckpointIfDue(false);
					} catch (std::exception &ex) {
						LatchFatal(ex.what());
					}
					continue;
				}
				command = std::move(queue.front());
				queue.pop_front();
				queue_cv.notify_all();
			}

			if (command->type == Command::Type::STOP) {
				try {
					SyncAppendIfDirty();
					SyncCheckpointIfDue(true);
					if (command->has_reply) {
						command->void_reply.set_value();
					}
				} catch (...) {
					if (command->has_reply) {
						command->void_reply.set_exception(std::current_exception());
					}
				}
				break;
			}

			try {
				switch (command->type) {
				case Command::Type::APPEND:
					HandleAppend(std::move(command));
					break;
				case Command::Type::CHECKPOINT_RECORD:
					CheckpointRecordInternal(command->id);
					if (command->has_reply) {
						command->void_reply.set_value();
					}
					break;
				case Command::Type::CHECKPOINT_SEALED:
					CheckpointSequences(command->sequences);
					break;
				case Command::Type::RECOVER_PENDING:
					command->recover_reply.set_value(RecoverPendingInternal());
					break;
				case Command::Type::STOP:
					break;
				}
			} catch (...) {
				if (command->type == Command::Type::APPEND) {
					AppendResult result;
					result.error_type = OtlpDiskBufferErrorType::UNAVAILABLE;
					try {
						throw;
					} catch (OtlpDiskBufferException &ex) {
						result.error_type = ex.type;
						result.error = ex.what();
					} catch (std::exception &ex) {
						result.error = ex.what();
					}
					command->append_reply.set_value(result);
				} else if (command->type == Command::Type::CHECKPOINT_RECORD ||
				           command->type == Command::Type::CHECKPOINT_SEALED) {
					string message;
					try {
						throw;
					} catch (std::exception &ex) {
						message = ex.what();
					} catch (...) {
						message = "unknown OTLP disk buffer checkpoint failure";
					}
					LatchFatal(message);
					if (command->has_reply) {
						command->void_reply.set_exception(std::make_exception_ptr(
						    OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, message)));
					}
				} else if (command->type == Command::Type::RECOVER_PENDING) {
					command->recover_reply.set_exception(std::current_exception());
				} else if (command->has_reply) {
					command->void_reply.set_exception(std::current_exception());
				}
			}
		}
	}

	void HandleAppend(unique_ptr<Command> first) {
		vector<unique_ptr<Command>> batch;
		batch.push_back(std::move(first));
		while (batch.size() < OTLP_DISK_GROUP_COMMIT_RECORDS) {
			std::lock_guard<std::mutex> lock(queue_mutex);
			if (queue.empty() || queue.front()->type != Command::Type::APPEND) {
				break;
			}
			batch.push_back(std::move(queue.front()));
			queue.pop_front();
			queue_cv.notify_all();
		}

		// Encode and write one record at a time so we hold at most a single encoded body in memory (instead of the
		// whole batch, which could be up to OTLP_DISK_GROUP_COMMIT_RECORDS * max_record_bytes). Only the resulting
		// per-record AppendResult (an id or an error, no body) is retained across the batch; replies are set after the
		// single group-commit fsync below, so a record is only reported ok once durable.
		//
		// A FULL/QUEUE_FULL/UNAVAILABLE OtlpDiskBufferException is a clean, expected per-record rejection (the record
		// was not written), so it becomes that record's error result and the batch continues. Any other failure (e.g.
		// an IO error from WriteAll/Rotate) is fatal: it propagates out so the Run() loop latches it; we first fail
		// every still-unanswered command's reply so no promise is left broken.
		auto base_sequence = next_sequence;
		next_sequence = base_sequence + batch.size();
		vector<AppendResult> results(batch.size());
		try {
			for (idx_t i = 0; i < batch.size(); i++) {
				try {
					auto id = AppendOneEncoded(batch[i]->record, base_sequence + i);
					results[i].ok = true;
					results[i].id = id;
				} catch (OtlpDiskBufferException &ex) {
					results[i].ok = false;
					results[i].error_type = ex.type;
					results[i].error = ex.what();
				}
			}
			SyncAppendIfDirty();
		} catch (std::exception &ex) {
			// A non-per-record failure (IO error from WriteAll/Rotate or the group-commit fsync) means nothing in this
			// batch is durable. Latch the writer fatal and fail every command's reply here so none is left broken; we
			// deliberately do NOT re-throw because the batch's command was moved into `batch` (the Run() catch could
			// not answer it anyway).
			LatchFatal(ex.what());
			AppendResult failure;
			failure.error_type = OtlpDiskBufferErrorType::UNAVAILABLE;
			failure.error = FatalError();
			for (idx_t i = 0; i < batch.size(); i++) {
				batch[i]->append_reply.set_value(failure);
			}
			return;
		}
		for (idx_t i = 0; i < batch.size(); i++) {
			batch[i]->append_reply.set_value(results[i]);
		}
	}

	void Open() {
		fs->CreateDirectoriesRecursive(dir);
		OtlpDiskSyncDirectory(dir);
		auto checkpoint_path = OtlpDiskCheckpointPath(*fs, dir);
		completed = OtlpDiskReadCompleted(*fs, checkpoint_path);
		auto segment_ids = OtlpDiskSegmentIds(*fs, dir);
		uint64_t max_sequence = 0;

		for (auto segment : segment_ids) {
			auto path = OtlpDiskSegmentPath(*fs, dir, segment);
			vector<RecoveredDiskRecord> recovered;
			auto scan = OtlpDiskScanSegment(*fs, path, segment, max_record_bytes, completed, &recovered);
			if (scan.truncated_tail) {
				last_error = StringUtil::Format("truncated corrupt OTLP disk buffer segment %s at %llu bytes: %s", path,
				                                static_cast<uint64_t>(scan.valid_len), scan.truncation_error);
			}
			max_sequence = MaxValue<uint64_t>(max_sequence, scan.seq_hi);
			segments[segment] = DiskSegmentState {scan.valid_len, scan.record_count, scan.seq_lo, scan.seq_hi, 0};
			total_segment_bytes_value += scan.valid_len;
			for (auto &record : recovered) {
				auto body_len = record.record.body.size();
				pending[record.id] = body_len;
				seq_to_segment[record.id.sequence] = record.id.segment;
				segments[segment].pending_count++;
				total_pending_bytes_value += body_len;
			}
		}

		if (segment_ids.empty()) {
			segment_ids.push_back(1);
			auto path = OtlpDiskSegmentPath(*fs, dir, 1);
			auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
			                                     FileFlags::FILE_FLAGS_APPEND | FileFlags::FILE_FLAGS_NULL_IF_EXISTS);
			(void)handle;
			OtlpDiskSyncDirectory(dir);
			segments[1] = DiskSegmentState {};
		}

		active_segment = segment_ids.back();
		auto active_len = segments.find(active_segment) == segments.end() ? 0 : segments[active_segment].bytes;
		if (active_len >= max_segment_bytes) {
			Rotate();
		} else {
			active = OpenSegmentAppend(active_segment);
		}
		checkpoint =
		    fs->OpenFile(checkpoint_path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
		                                      FileFlags::FILE_FLAGS_FILE_CREATE | FileFlags::FILE_FLAGS_APPEND);
		next_sequence = MaxValue<uint64_t>(max_sequence + 1, 1);
		checkpoint_last_sync = std::chrono::steady_clock::now();

		// Publish the recovered state into the lock-free stat mirrors read by Stats().
		PublishPendingBytes(total_pending_bytes_value);
		PublishSegmentBytes(total_segment_bytes_value);
		PublishSegmentCount(segments.size());
		PublishPendingCount(pending.size());
		RecomputeOldestUnsealed();
	}

	unique_ptr<FileHandle> OpenSegmentAppend(uint64_t segment) {
		return fs->OpenFile(OtlpDiskSegmentPath(*fs, dir, segment),
		                    FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
		                        FileFlags::FILE_FLAGS_FILE_CREATE | FileFlags::FILE_FLAGS_APPEND);
	}

	// Encodes a single record at the given sequence and writes it to the active segment (rotating mid-batch if
	// needed). Performs the per-record oversize/FULL and total-bytes/reclaim checks before writing; throws
	// OtlpDiskBufferException(FULL) without writing if the record cannot be admitted. Does not fsync (the caller
	// group-commits via SyncAppendIfDirty()).
	DiskRecordId AppendOneEncoded(const DiskRecord &record, uint64_t sequence) {
		if (record.body.size() > max_record_bytes) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::FULL,
			                              StringUtil::Format("OTLP disk buffer record has %llu bytes; max is %llu",
			                                                 static_cast<uint64_t>(record.body.size()),
			                                                 static_cast<uint64_t>(max_record_bytes)));
		}
		auto encoded = OtlpDiskEncodeRecord(record, sequence);

		auto required_bytes = total_segment_bytes_value + encoded.bytes.size();
		if (required_bytes > max_total_bytes) {
			ReclaimCommittedSegments();
			required_bytes = total_segment_bytes_value + encoded.bytes.size();
		}
		if (required_bytes > max_total_bytes) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::FULL,
			                              StringUtil::Format("OTLP disk buffer needs %llu bytes but max is %llu",
			                                                 static_cast<uint64_t>(required_bytes),
			                                                 static_cast<uint64_t>(max_total_bytes)));
		}

		auto active_bytes = segments[active_segment].bytes;
		if (active_bytes > 0 && active_bytes + encoded.bytes.size() > max_segment_bytes) {
			SyncAppendIfDirty();
			Rotate();
		}
		auto id = DiskRecordId {active_segment, encoded.sequence};
		WriteAll(*active, encoded.bytes.data(), encoded.bytes.size());
		RecordAppended(id, encoded.body_len, encoded.bytes.size());
		return id;
	}

	void RecordAppended(DiskRecordId id, idx_t body_len, idx_t written) {
		auto &state = segments[id.segment];
		if (state.record_count == 0) {
			state.seq_lo = id.sequence;
		}
		state.seq_hi = id.sequence;
		state.record_count++;
		state.bytes += written;
		state.pending_count++;
		PublishSegmentBytes(total_segment_bytes_value + written);
		pending[id] = body_len;
		seq_to_segment[id.sequence] = id.segment;
		PublishPendingBytes(total_pending_bytes_value + body_len);
		PublishPendingCount(pending.size());
		UpdateOldestUnsealed(id.sequence);
		append_dirty_records++;
		append_dirty_segments.insert(id.segment);
		PublishJournalWrites(journal_writes_total_value + 1);
	}

	void SyncAppendIfDirty() {
		if (append_dirty_records == 0) {
			return;
		}
		try {
			for (auto segment : append_dirty_segments) {
				if (segment == active_segment) {
					active->Sync();
				} else {
					auto handle = fs->OpenFile(OtlpDiskSegmentPath(*fs, dir, segment),
					                           FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE);
					handle->Sync();
				}
				PublishJournalFsync(journal_fsync_total_value + 1);
			}
			append_dirty_records = 0;
			append_dirty_segments.clear();
		} catch (std::exception &ex) {
			PublishJournalFsyncFailures(journal_fsync_failures_total_value + 1);
			LatchFatal(ex.what());
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, FatalError());
		}
	}

	void Rotate() {
		active_segment++;
		auto path = OtlpDiskSegmentPath(*fs, dir, active_segment);
		active = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                FileFlags::FILE_FLAGS_APPEND | FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
		OtlpDiskSyncDirectory(dir);
		segments[active_segment] = DiskSegmentState {};
		PublishSegmentCount(segments.size());
	}

	void CheckpointRecordInternal(DiskRecordId id) {
		CheckpointSequences({id.sequence});
	}

	void CheckpointSequences(const vector<uint64_t> &sequences) {
		for (auto sequence : sequences) {
			// completed.insert dedups: a sequence already checkpointed is a no-op.
			if (!completed.insert(sequence).second) {
				continue;
			}
			ErasePending(sequence);
			auto line = std::to_string(sequence) + "\n";
			WriteAll(*checkpoint, line);
			checkpoint_dirty_records++;
		}
		PublishPendingCount(pending.size());
		RecomputeOldestUnsealed();
		SyncCheckpointIfDue(false);
		ReclaimCommittedSegments();
	}

	// Removes a single not-yet-checkpointed sequence from `pending`, locating its segment in O(log n) via
	// seq_to_segment, and keeps the per-segment pending_count and total_pending_bytes mirror in sync.
	void ErasePending(uint64_t sequence) {
		auto seg_it = seq_to_segment.find(sequence);
		if (seg_it == seq_to_segment.end()) {
			return;
		}
		auto segment = seg_it->second;
		seq_to_segment.erase(seg_it);
		auto it = pending.find(DiskRecordId {segment, sequence});
		if (it == pending.end()) {
			return;
		}
		PublishPendingBytes(total_pending_bytes_value >= it->second ? total_pending_bytes_value - it->second : 0);
		auto seg = segments.find(segment);
		if (seg != segments.end() && seg->second.pending_count > 0) {
			seg->second.pending_count--;
		}
		pending.erase(it);
	}

	void SyncCheckpointIfDue(bool force) {
		if (checkpoint_dirty_records == 0) {
			return;
		}
		auto now = std::chrono::steady_clock::now();
		if (force || checkpoint_dirty_records >= checkpoint_fsync_records ||
		    now - checkpoint_last_sync >= checkpoint_fsync_delay) {
			checkpoint->Sync();
			checkpoint_dirty_records = 0;
			checkpoint_last_sync = now;
		}
	}

	std::optional<std::chrono::milliseconds> CheckpointSyncDueIn() {
		if (checkpoint_dirty_records == 0) {
			return std::nullopt;
		}
		auto elapsed = std::chrono::steady_clock::now() - checkpoint_last_sync;
		if (elapsed >= checkpoint_fsync_delay) {
			return std::chrono::milliseconds(0);
		}
		return std::chrono::duration_cast<std::chrono::milliseconds>(checkpoint_fsync_delay - elapsed);
	}

	vector<RecoveredDiskRecord> RecoverPendingInternal() {
		vector<RecoveredDiskRecord> result;
		for (auto &entry : segments) {
			auto path = OtlpDiskSegmentPath(*fs, dir, entry.first);
			OtlpDiskScanSegment(*fs, path, entry.first, max_record_bytes, completed, &result);
		}
		std::sort(result.begin(), result.end(), [](const RecoveredDiskRecord &a, const RecoveredDiskRecord &b) {
			return a.id.sequence < b.id.sequence;
		});
		return result;
	}

	void ReclaimCommittedSegments() {
		vector<uint64_t> candidates;
		for (auto &entry : segments) {
			if (entry.first != active_segment && !SegmentHasPending(entry.first)) {
				candidates.push_back(entry.first);
			}
		}
		bool pruned_completed = false;
		for (auto segment : candidates) {
			auto path = OtlpDiskSegmentPath(*fs, dir, segment);
			fs->RemoveFile(path);
			auto state = segments[segment];
			PublishSegmentBytes(total_segment_bytes_value >= state.bytes ? total_segment_bytes_value - state.bytes : 0);
			if (state.record_count > 0) {
				for (auto seq = state.seq_lo; seq <= state.seq_hi; seq++) {
					auto erased = completed.erase(seq);
					pruned_completed = pruned_completed || erased > 0;
					if (seq == NumericLimits<uint64_t>::Maximum()) {
						break;
					}
				}
			}
			segments.erase(segment);
		}
		if (!candidates.empty()) {
			PublishSegmentCount(segments.size());
			OtlpDiskSyncDirectory(dir);
		}
		if (pruned_completed) {
			RewriteCheckpoint();
		}
	}

	bool SegmentHasPending(uint64_t segment) const {
		auto it = segments.find(segment);
		return it != segments.end() && it->second.pending_count > 0;
	}

	void RewriteCheckpoint() {
		auto path = OtlpDiskCheckpointPath(*fs, dir);
		auto tmp = fs->JoinPath(dir, "checkpoint.log.tmp");
		{
			auto handle = fs->OpenFile(tmp, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
			for (auto sequence : completed) {
				WriteAll(*handle, std::to_string(sequence) + "\n");
			}
			handle->Sync();
		}
		checkpoint.reset();
		fs->MoveFile(tmp, path);
		OtlpDiskSyncDirectory(dir);
		checkpoint = fs->OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
		                                    FileFlags::FILE_FLAGS_FILE_CREATE | FileFlags::FILE_FLAGS_APPEND);
		checkpoint_dirty_records = 0;
		checkpoint_last_sync = std::chrono::steady_clock::now();
	}

	void LatchFatal(const string &message) {
		std::lock_guard<std::mutex> lock(stats_mutex);
		if (fatal_error.empty()) {
			fatal_error = StringUtil::Format("OTLP disk buffer writer for %s entered fatal state: %s",
			                                 OtlpRequestKindName(kind), message);
			healthy.store(false, std::memory_order_release);
		}
	}

	// Returns the latched fatal error string (empty if none). Only ever read/written by the writer thread except via
	// Stats(); both paths take stats_mutex.
	string FatalError() {
		std::lock_guard<std::mutex> lock(stats_mutex);
		return fatal_error;
	}

	// --- Lock-free stat mirrors -------------------------------------------------------------------------------
	// The writer thread owns the plain `*_value` counters; it publishes each change into the matching atomic via
	// these helpers (release stores). Stats() reads the atomics with acquire loads from another thread.
	void PublishPendingBytes(idx_t value) {
		total_pending_bytes_value = value;
		total_pending_bytes.store(value, std::memory_order_release);
	}
	void PublishSegmentBytes(idx_t value) {
		total_segment_bytes_value = value;
		total_segment_bytes.store(value, std::memory_order_release);
	}
	void PublishSegmentCount(idx_t value) {
		segment_count.store(value, std::memory_order_release);
	}
	void PublishPendingCount(idx_t value) {
		pending_count.store(value, std::memory_order_release);
	}
	void PublishJournalWrites(idx_t value) {
		journal_writes_total_value = value;
		journal_writes_total.store(value, std::memory_order_release);
	}
	void PublishJournalFsync(idx_t value) {
		journal_fsync_total_value = value;
		journal_fsync_total.store(value, std::memory_order_release);
	}
	void PublishJournalFsyncFailures(idx_t value) {
		journal_fsync_failures_total_value = value;
		journal_fsync_failures_total.store(value, std::memory_order_release);
	}

	// Cheap update on append: the just-appended sequence is monotonically increasing, so it can only become the
	// oldest unsealed sequence when there is currently none tracked.
	void UpdateOldestUnsealed(uint64_t sequence) {
		if (oldest_unsealed_seq.load(std::memory_order_acquire) < 0) {
			auto clamped = sequence > static_cast<uint64_t>(NumericLimits<int64_t>::Maximum())
			                   ? NumericLimits<int64_t>::Maximum()
			                   : static_cast<int64_t>(sequence);
			oldest_unsealed_seq.store(clamped, std::memory_order_release);
		}
	}

	// Recompute the oldest unsealed sequence after pending entries are removed. seq_to_segment is ordered, so the
	// minimum pending sequence is its first key. A slightly stale value is acceptable for a metrics endpoint.
	void RecomputeOldestUnsealed() {
		if (seq_to_segment.empty()) {
			oldest_unsealed_seq.store(-1, std::memory_order_release);
			return;
		}
		auto oldest = seq_to_segment.begin()->first;
		auto clamped = oldest > static_cast<uint64_t>(NumericLimits<int64_t>::Maximum())
		                   ? NumericLimits<int64_t>::Maximum()
		                   : static_cast<int64_t>(oldest);
		oldest_unsealed_seq.store(clamped, std::memory_order_release);
	}

private:
	OtlpRequestKind kind;
	string dir;
	idx_t max_segment_bytes;
	idx_t max_record_bytes;
	idx_t max_total_bytes;
	idx_t checkpoint_fsync_records;
	std::chrono::milliseconds checkpoint_fsync_delay;
	unique_ptr<FileSystem> fs;
	unique_ptr<FileHandle> active;
	unique_ptr<FileHandle> checkpoint;
	uint64_t active_segment = 1;
	uint64_t next_sequence = 1;
	std::set<uint64_t> completed;
	std::map<uint64_t, DiskSegmentState> segments;
	std::map<DiskRecordId, idx_t> pending;
	// sequence -> segment id for every pending record. Ordered, so its first key is the oldest unsealed sequence and
	// it gives O(log n) lookup of a sequence's segment without scanning `pending`.
	std::map<uint64_t, uint64_t> seq_to_segment;
	// Writer-thread-only authoritative counters. Each is mirrored into the matching atomic below via a Publish*
	// helper so Stats() can read them lock-free from another thread.
	idx_t total_segment_bytes_value = 0;
	idx_t total_pending_bytes_value = 0;
	idx_t journal_writes_total_value = 0;
	idx_t journal_fsync_total_value = 0;
	idx_t journal_fsync_failures_total_value = 0;
	idx_t append_dirty_records = 0;
	std::set<uint64_t> append_dirty_segments;
	idx_t checkpoint_dirty_records = 0;
	std::chrono::steady_clock::time_point checkpoint_last_sync;

	// Lock-free mirrors of the counters above, published (release) by the writer thread and read (acquire) by
	// Stats() on the query thread. The writer thread is the sole writer, so release/acquire is sufficient.
	std::atomic<idx_t> total_segment_bytes {0};
	std::atomic<idx_t> total_pending_bytes {0};
	std::atomic<idx_t> segment_count {0};
	std::atomic<idx_t> pending_count {0};
	std::atomic<idx_t> journal_writes_total {0};
	std::atomic<idx_t> journal_fsync_total {0};
	std::atomic<idx_t> journal_fsync_failures_total {0};
	std::atomic<int64_t> oldest_unsealed_seq {-1};
	std::atomic<bool> healthy {true};

	// Guards the error strings only; Stats() briefly locks this (never the queue) to read them.
	std::mutex stats_mutex;
	string fatal_error;
	string last_error;

	std::mutex queue_mutex;
	std::condition_variable queue_cv;
	std::deque<unique_ptr<Command>> queue;
	std::thread thread;
	std::atomic<bool> shutdown {false};
};

DiskSegmentWriter::DiskSegmentWriter(OtlpRequestKind kind, string dir, idx_t max_segment_bytes, idx_t max_record_bytes,
                                     idx_t max_total_bytes)
    : impl(make_uniq<Impl>(kind, std::move(dir), max_segment_bytes, max_record_bytes, max_total_bytes)) {
}

DiskSegmentWriter::~DiskSegmentWriter() {
}

DiskRecordId DiskSegmentWriter::Append(const DiskRecord &record) {
	return impl->Append(record);
}

vector<RecoveredDiskRecord> DiskSegmentWriter::RecoverPending() {
	return impl->RecoverPending();
}

void DiskSegmentWriter::CheckpointRecord(DiskRecordId id) {
	impl->CheckpointRecord(id);
}

void DiskSegmentWriter::CheckpointSealed(const vector<uint64_t> &sequences) {
	impl->CheckpointSealed(sequences);
}

OtlpDiskBufferStats DiskSegmentWriter::Stats() {
	return impl->Stats();
}

void DiskSegmentWriter::Shutdown() {
	impl->Shutdown();
}

OtlpDiskBuffer::OtlpDiskBuffer(OtlpDiskBufferConfig config_p, string catalog_name, string schema_name,
                               idx_t max_record_bytes_p)
    : config(std::move(config_p)), max_record_bytes(max_record_bytes_p) {
	if (config.dir.empty()) {
		throw InvalidInputException("disk_buffer_dir is required when buffer='disk'");
	}
	config.segment_bytes = MaxValue<idx_t>(config.segment_bytes, OTLP_DISK_RECORD_HEADER_BYTES + 1);
	config.max_disk_bytes = MaxValue<idx_t>(config.max_disk_bytes, config.segment_bytes);
	EnsureManifest(catalog_name, schema_name);
	AcquireOwnerLock();

	auto fs = FileSystem::CreateLocal();
	logs = make_uniq<DiskSegmentWriter>(OtlpRequestKind::LOGS, fs->JoinPath(config.dir, "logs"), config.segment_bytes,
	                                    max_record_bytes, config.max_disk_bytes);
	traces = make_uniq<DiskSegmentWriter>(OtlpRequestKind::TRACES, fs->JoinPath(config.dir, "traces"),
	                                      config.segment_bytes, max_record_bytes, config.max_disk_bytes);
	metrics = make_uniq<DiskSegmentWriter>(OtlpRequestKind::METRICS, fs->JoinPath(config.dir, "metrics"),
	                                       config.segment_bytes, max_record_bytes, config.max_disk_bytes);
}

OtlpDiskBuffer::~OtlpDiskBuffer() {
	Shutdown();
}

DiskRecordId OtlpDiskBuffer::Append(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
                                    const string &body) {
	DiskRecord record;
	record.request_kind = kind;
	record.content_type = content_type;
	record.content_encoding = content_encoding;
	record.body = body;
	// accepted_at_micros is left 0 here: OtlpDiskEncodeRecord fills a now() fallback when it is 0, making the encode
	// step the single source of truth for the timestamp.
	return WriterFor(kind).Append(record);
}

vector<RecoveredDiskRecord> OtlpDiskBuffer::RecoverPending() {
	vector<RecoveredDiskRecord> result;
	for (auto kind : {OtlpRequestKind::LOGS, OtlpRequestKind::TRACES, OtlpRequestKind::METRICS}) {
		auto recovered = WriterFor(kind).RecoverPending();
		result.insert(result.end(), std::make_move_iterator(recovered.begin()),
		              std::make_move_iterator(recovered.end()));
	}
	std::sort(result.begin(), result.end(), [](const RecoveredDiskRecord &a, const RecoveredDiskRecord &b) {
		if (a.record.request_kind != b.record.request_kind) {
			return static_cast<uint8_t>(a.record.request_kind) < static_cast<uint8_t>(b.record.request_kind);
		}
		return a.id.sequence < b.id.sequence;
	});
	return result;
}

void OtlpDiskBuffer::CheckpointTerminal(OtlpRequestKind kind, DiskRecordId id) {
	WriterFor(kind).CheckpointRecord(id);
}

void OtlpDiskBuffer::CheckpointSealed(OtlpRequestKind kind, const vector<uint64_t> &sequences) {
	WriterFor(kind).CheckpointSealed(sequences);
}

OtlpDiskBufferStats OtlpDiskBuffer::Stats() const {
	auto result = logs->Stats();
	result = MergeStats(result, traces->Stats());
	result = MergeStats(result, metrics->Stats());
	result.replay_records_total = replay_records_total.load();
	return result;
}

void OtlpDiskBuffer::RecordReplayRecords(idx_t records) {
	replay_records_total.fetch_add(records);
}

void OtlpDiskBuffer::Shutdown() {
	if (logs) {
		logs->Shutdown();
	}
	if (traces) {
		traces->Shutdown();
	}
	if (metrics) {
		metrics->Shutdown();
	}
	// Release the OS advisory lock last, after all writers have stopped touching the directory.
	owner_lock.reset();
}

DiskSegmentWriter &OtlpDiskBuffer::WriterFor(OtlpRequestKind kind) const {
	switch (kind) {
	case OtlpRequestKind::LOGS:
		return *logs;
	case OtlpRequestKind::TRACES:
		return *traces;
	case OtlpRequestKind::METRICS:
		return *metrics;
	default:
		throw InternalException("Unknown OTLP request kind");
	}
}

void OtlpDiskBuffer::AcquireOwnerLock() {
	auto fs = FileSystem::CreateLocal();
	auto lock_path = fs->JoinPath(config.dir, "OWNER.lock");
	// DuckDB's LocalFileSystem applies an OS advisory lock (fcntl on POSIX, LockFileEx on Windows) when a
	// FileLockType is passed to OpenFile, so this is portable across macOS/Linux/Windows. A conflicting lock held
	// by another otlp_serve instance/process makes OpenFile throw; we surface that as an IOException with a clear
	// message. The handle (and thus the lock) is held for the lifetime of this OtlpDiskBuffer.
	try {
		owner_lock = fs->OpenFile(lock_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                         FileLockType::WRITE_LOCK);
	} catch (std::exception &ex) {
		throw IOException("OTLP disk buffer directory \"%s\" is already in use by another otlp_serve instance: %s",
		                  config.dir, ex.what());
	}
	if (!owner_lock) {
		throw IOException("OTLP disk buffer directory \"%s\" is already in use by another otlp_serve instance",
		                  config.dir);
	}
}

void OtlpDiskBuffer::EnsureManifest(const string &catalog_name, const string &schema_name) {
	auto fs = FileSystem::CreateLocal();
	fs->CreateDirectoriesRecursive(config.dir);
	OtlpDiskSyncDirectory(config.dir);
	auto manifest_path = fs->JoinPath(config.dir, "manifest.json");
	auto existing = fs->OpenFile(manifest_path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	if (existing) {
		auto file_size = existing->GetFileSize();
		string contents;
		contents.resize(file_size);
		if (file_size > 0) {
			existing->Read(reinterpret_cast<data_t *>(&contents[0]), file_size, 0);
		}
		auto existing_catalog = ParseJsonStringField(contents, "catalog_name");
		auto existing_schema = ParseJsonStringField(contents, "schema_name");
		if (existing_catalog != catalog_name || existing_schema != schema_name) {
			throw IOException(
			    "OTLP disk buffer manifest mismatch for %s: expected catalog=%s schema=%s, found catalog=%s schema=%s",
			    config.dir, catalog_name, schema_name, existing_catalog, existing_schema);
		}
		return;
	}

	auto tmp = fs->JoinPath(config.dir, "manifest.json.tmp");
	auto manifest = "{\"version\":1,\"catalog_name\":\"" + JsonEscape(catalog_name) + "\",\"schema_name\":\"" +
	                JsonEscape(schema_name) + "\"}\n";
	{
		auto handle = fs->OpenFile(tmp, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		WriteAll(*handle, manifest);
		handle->Sync();
	}
	fs->MoveFile(tmp, manifest_path);
	OtlpDiskSyncDirectory(config.dir);
}

} // namespace duckdb
