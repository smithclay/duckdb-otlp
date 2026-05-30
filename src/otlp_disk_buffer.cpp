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
};

struct AppendResult {
	bool ok = false;
	DiskRecordId id;
	OtlpDiskBufferErrorType error_type = OtlpDiskBufferErrorType::UNAVAILABLE;
	string error;
};

struct Command {
	enum class Type : uint8_t { APPEND, CHECKPOINT_RECORD, CHECKPOINT_UP_TO, RECOVER_PENDING, STATS, STOP };

	Type type;
	DiskRecord record;
	DiskRecordId id;
	uint64_t sequence = 0;
	std::promise<AppendResult> append_reply;
	std::promise<vector<RecoveredDiskRecord>> recover_reply;
	std::promise<OtlpDiskBufferStats> stats_reply;
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

	void CheckpointUpTo(uint64_t sequence) {
		auto command = make_uniq<Command>();
		command->type = Command::Type::CHECKPOINT_UP_TO;
		command->sequence = sequence;
		EnqueueBlocking(std::move(command));
	}

	OtlpDiskBufferStats Stats() {
		auto command = make_uniq<Command>();
		command->type = Command::Type::STATS;
		auto future = command->stats_reply.get_future();
		EnqueueBlocking(std::move(command));
		return future.get();
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
		if (!fatal_error.empty()) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, fatal_error);
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
				case Command::Type::CHECKPOINT_UP_TO:
					CheckpointUpToInternal(command->sequence);
					break;
				case Command::Type::RECOVER_PENDING:
					command->recover_reply.set_value(RecoverPendingInternal());
					break;
				case Command::Type::STATS:
					command->stats_reply.set_value(StatsInternal());
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
				           command->type == Command::Type::CHECKPOINT_UP_TO) {
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
				} else if (command->type == Command::Type::STATS) {
					command->stats_reply.set_exception(std::current_exception());
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

		vector<EncodedDiskRecord> encoded;
		encoded.reserve(batch.size());
		auto base_sequence = next_sequence;
		for (idx_t i = 0; i < batch.size(); i++) {
			if (batch[i]->record.body.size() > max_record_bytes) {
				throw OtlpDiskBufferException(OtlpDiskBufferErrorType::FULL,
				                              StringUtil::Format("OTLP disk buffer record has %llu bytes; max is %llu",
				                                                 static_cast<uint64_t>(batch[i]->record.body.size()),
				                                                 static_cast<uint64_t>(max_record_bytes)));
			}
			encoded.push_back(OtlpDiskEncodeRecord(batch[i]->record, base_sequence + i));
		}

		vector<DiskRecordId> ids;
		AppendEncoded(encoded, ids);
		SyncAppendIfDirty();
		next_sequence = base_sequence + ids.size();
		for (idx_t i = 0; i < batch.size(); i++) {
			AppendResult result;
			result.ok = true;
			result.id = ids[i];
			batch[i]->append_reply.set_value(result);
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
			segments[segment] = DiskSegmentState {scan.valid_len, scan.record_count, scan.seq_lo, scan.seq_hi};
			total_segment_bytes += scan.valid_len;
			for (auto &record : recovered) {
				auto body_len = record.record.body.size();
				pending[record.id] = body_len;
				total_pending_bytes += body_len;
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
	}

	unique_ptr<FileHandle> OpenSegmentAppend(uint64_t segment) {
		return fs->OpenFile(OtlpDiskSegmentPath(*fs, dir, segment),
		                    FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
		                        FileFlags::FILE_FLAGS_FILE_CREATE | FileFlags::FILE_FLAGS_APPEND);
	}

	void AppendEncoded(vector<EncodedDiskRecord> &encoded, vector<DiskRecordId> &ids) {
		idx_t encoded_bytes = 0;
		for (auto &record : encoded) {
			encoded_bytes += record.bytes.size();
		}
		auto required_bytes = total_segment_bytes + encoded_bytes;
		if (required_bytes > max_total_bytes) {
			ReclaimCommittedSegments();
			required_bytes = total_segment_bytes + encoded_bytes;
		}
		if (required_bytes > max_total_bytes) {
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::FULL,
			                              StringUtil::Format("OTLP disk buffer needs %llu bytes but max is %llu",
			                                                 static_cast<uint64_t>(required_bytes),
			                                                 static_cast<uint64_t>(max_total_bytes)));
		}

		for (auto &record : encoded) {
			auto active_bytes = segments[active_segment].bytes;
			if (active_bytes > 0 && active_bytes + record.bytes.size() > max_segment_bytes) {
				SyncAppendIfDirty();
				Rotate();
			}
			auto id = DiskRecordId {active_segment, record.sequence};
			WriteAll(*active, record.bytes.data(), record.bytes.size());
			RecordAppended(id, record.body_len, record.bytes.size());
			ids.push_back(id);
		}
	}

	void RecordAppended(DiskRecordId id, idx_t body_len, idx_t written) {
		auto &state = segments[id.segment];
		if (state.record_count == 0) {
			state.seq_lo = id.sequence;
		}
		state.seq_hi = id.sequence;
		state.record_count++;
		state.bytes += written;
		total_segment_bytes += written;
		pending[id] = body_len;
		total_pending_bytes += body_len;
		append_dirty_records++;
		append_dirty_segments.insert(id.segment);
		journal_writes_total++;
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
				journal_fsync_total++;
			}
			append_dirty_records = 0;
			append_dirty_segments.clear();
		} catch (std::exception &ex) {
			journal_fsync_failures_total++;
			LatchFatal(ex.what());
			throw OtlpDiskBufferException(OtlpDiskBufferErrorType::UNAVAILABLE, fatal_error);
		}
	}

	void Rotate() {
		active_segment++;
		auto path = OtlpDiskSegmentPath(*fs, dir, active_segment);
		active = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                FileFlags::FILE_FLAGS_APPEND | FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
		OtlpDiskSyncDirectory(dir);
		segments[active_segment] = DiskSegmentState {};
	}

	void CheckpointRecordInternal(DiskRecordId id) {
		CheckpointSequences({id.sequence});
	}

	void CheckpointUpToInternal(uint64_t sequence) {
		vector<uint64_t> sequences;
		for (auto &entry : pending) {
			if (entry.first.sequence <= sequence) {
				sequences.push_back(entry.first.sequence);
			}
		}
		CheckpointSequences(sequences);
	}

	void CheckpointSequences(const vector<uint64_t> &sequences) {
		for (auto sequence : sequences) {
			if (!completed.insert(sequence).second) {
				continue;
			}
			for (auto it = pending.begin(); it != pending.end(); ++it) {
				if (it->first.sequence == sequence) {
					total_pending_bytes = total_pending_bytes >= it->second ? total_pending_bytes - it->second : 0;
					pending.erase(it);
					break;
				}
			}
			auto line = std::to_string(sequence) + "\n";
			WriteAll(*checkpoint, line);
			checkpoint_dirty_records++;
		}
		SyncCheckpointIfDue(false);
		ReclaimCommittedSegments();
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

	OtlpDiskBufferStats StatsInternal() {
		OtlpDiskBufferStats stats;
		stats.buffered_bytes = total_pending_bytes;
		stats.segment_bytes = total_segment_bytes;
		stats.segments = segments.size();
		stats.pending_records = pending.size();
		stats.journal_writes_total = journal_writes_total;
		stats.journal_fsync_total = journal_fsync_total;
		stats.journal_fsync_failures_total = journal_fsync_failures_total;
		stats.healthy = fatal_error.empty();
		stats.last_error = fatal_error.empty() ? last_error : fatal_error;
		if (!pending.empty()) {
			uint64_t oldest = NumericLimits<uint64_t>::Maximum();
			for (auto &entry : pending) {
				oldest = MinValue<uint64_t>(oldest, entry.first.sequence);
			}
			stats.oldest_unsealed_seq = oldest > NumericLimits<int64_t>::Maximum() ? NumericLimits<int64_t>::Maximum()
			                                                                       : static_cast<int64_t>(oldest);
		}
		return stats;
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
			total_segment_bytes = total_segment_bytes >= state.bytes ? total_segment_bytes - state.bytes : 0;
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
			OtlpDiskSyncDirectory(dir);
		}
		if (pruned_completed) {
			RewriteCheckpoint();
		}
	}

	bool SegmentHasPending(uint64_t segment) const {
		for (auto &entry : pending) {
			if (entry.first.segment == segment) {
				return true;
			}
		}
		return false;
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
		std::lock_guard<std::mutex> lock(queue_mutex);
		if (fatal_error.empty()) {
			fatal_error = StringUtil::Format("OTLP disk buffer writer for %s entered fatal state: %s",
			                                 OtlpRequestKindName(kind), message);
		}
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
	idx_t total_segment_bytes = 0;
	idx_t total_pending_bytes = 0;
	idx_t append_dirty_records = 0;
	std::set<uint64_t> append_dirty_segments;
	idx_t journal_writes_total = 0;
	idx_t journal_fsync_total = 0;
	idx_t journal_fsync_failures_total = 0;
	idx_t checkpoint_dirty_records = 0;
	std::chrono::steady_clock::time_point checkpoint_last_sync;
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

void DiskSegmentWriter::CheckpointUpTo(uint64_t sequence) {
	impl->CheckpointUpTo(sequence);
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
	record.accepted_at_micros =
	    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
	        .count();
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

void OtlpDiskBuffer::CheckpointUpTo(OtlpRequestKind kind, uint64_t sequence) {
	WriterFor(kind).CheckpointUpTo(sequence);
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
