#pragma once

#include "duckdb.hpp"

#include "otlp_disk_codec.hpp"

#include <atomic>
#include <optional>

namespace duckdb {

enum class OtlpBufferMode : uint8_t { MEMORY, DISK };

struct OtlpDiskBufferConfig {
	string dir;
	idx_t segment_bytes = OTLP_DISK_DEFAULT_SEGMENT_BYTES;
	idx_t max_disk_bytes = OTLP_DISK_DEFAULT_MAX_BYTES;
};

enum class OtlpDiskBufferErrorType : uint8_t { FULL, QUEUE_FULL, UNAVAILABLE };

class OtlpDiskBufferException : public std::exception {
public:
	OtlpDiskBufferException(OtlpDiskBufferErrorType type_p, string message_p);

	const char *what() const noexcept override {
		return message.c_str();
	}

	OtlpDiskBufferErrorType type;
	string message;
};

struct OtlpDiskBufferStats {
	idx_t buffered_bytes = 0;
	idx_t segment_bytes = 0;
	idx_t segments = 0;
	idx_t pending_records = 0;
	idx_t journal_writes_total = 0;
	idx_t journal_fsync_total = 0;
	idx_t journal_fsync_failures_total = 0;
	idx_t replay_records_total = 0;
	int64_t oldest_unsealed_seq = -1;
	bool healthy = true;
	string last_error;
};

class DiskSegmentWriter {
public:
	DiskSegmentWriter(OtlpRequestKind kind, string dir, idx_t max_segment_bytes, idx_t max_record_bytes,
	                  idx_t max_total_bytes);
	~DiskSegmentWriter();

	DiskRecordId Append(const DiskRecord &record);
	vector<RecoveredDiskRecord> RecoverPending();
	void CheckpointRecord(DiskRecordId id);
	void CheckpointSealed(const vector<uint64_t> &sequences);
	OtlpDiskBufferStats Stats();
	void Shutdown();

private:
	class Impl;
	unique_ptr<Impl> impl;
};

class OtlpDiskBuffer {
public:
	OtlpDiskBuffer(OtlpDiskBufferConfig config, string catalog_name, string schema_name, idx_t max_record_bytes);
	~OtlpDiskBuffer();

	DiskRecordId Append(OtlpRequestKind kind, const string &content_type, const string &content_encoding,
	                    const string &body);
	vector<RecoveredDiskRecord> RecoverPending();
	void CheckpointTerminal(OtlpRequestKind kind, DiskRecordId id);
	void CheckpointSealed(OtlpRequestKind kind, const vector<uint64_t> &sequences);
	OtlpDiskBufferStats Stats() const;
	void RecordReplayRecords(idx_t records);
	void Shutdown();

private:
	DiskSegmentWriter &WriterFor(OtlpRequestKind kind) const;
	void EnsureManifest(const string &catalog_name, const string &schema_name);
	void AcquireOwnerLock();

private:
	OtlpDiskBufferConfig config;
	idx_t max_record_bytes;
	// Exclusive OS advisory lock on <dir>/OWNER.lock; held for the lifetime of the buffer so that only one
	// otlp_serve instance/process may write the segment journals. Released on destruction/Shutdown.
	unique_ptr<FileHandle> owner_lock;
	unique_ptr<DiskSegmentWriter> logs;
	unique_ptr<DiskSegmentWriter> traces;
	unique_ptr<DiskSegmentWriter> metrics;
	std::atomic<idx_t> replay_records_total {0};
};

} // namespace duckdb
