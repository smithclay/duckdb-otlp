#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"

#include "otlp_request.hpp"

#include <set>

namespace duckdb {

static constexpr idx_t OTLP_DISK_RECORD_HEADER_BYTES = 8 + 4 + 8;
static constexpr idx_t OTLP_DISK_DEFAULT_SEGMENT_BYTES = 64ULL * 1024ULL * 1024ULL;
static constexpr idx_t OTLP_DISK_DEFAULT_MAX_BYTES = 1024ULL * 1024ULL * 1024ULL;
static constexpr idx_t OTLP_DISK_WRITER_QUEUE_CAPACITY = 1024;
static constexpr idx_t OTLP_DISK_GROUP_COMMIT_RECORDS = 64;
static constexpr idx_t OTLP_DISK_CHECKPOINT_FSYNC_RECORDS = 1024;
static constexpr int64_t OTLP_DISK_CHECKPOINT_FSYNC_DELAY_MS = 1000;

struct DiskRecordId {
	uint64_t segment = 0;
	uint64_t sequence = 0;

	bool operator<(const DiskRecordId &other) const {
		if (segment != other.segment) {
			return segment < other.segment;
		}
		return sequence < other.sequence;
	}

	bool operator==(const DiskRecordId &other) const {
		return segment == other.segment && sequence == other.sequence;
	}
};

struct DiskRecord {
	OtlpRequestKind request_kind = OtlpRequestKind::LOGS;
	string content_type;
	string content_encoding;
	int64_t accepted_at_micros = 0;
	string body;
};

struct RecoveredDiskRecord {
	DiskRecordId id;
	DiskRecord record;
};

struct EncodedDiskRecord {
	uint64_t sequence = 0;
	uint64_t body_len = 0;
	vector<data_t> bytes;
};

struct OtlpDiskSegmentScan {
	idx_t valid_len = 0;
	idx_t record_count = 0;
	uint64_t seq_lo = 0;
	uint64_t seq_hi = 0;
	bool truncated_tail = false;
	string truncation_error;
};

uint64_t OtlpDiskChecksum(const string &body);
string OtlpDiskSegmentPath(FileSystem &fs, const string &dir, uint64_t segment);
string OtlpDiskCheckpointPath(FileSystem &fs, const string &dir);
vector<uint64_t> OtlpDiskSegmentIds(FileSystem &fs, const string &dir);
std::set<uint64_t> OtlpDiskReadCompleted(FileSystem &fs, const string &path);
EncodedDiskRecord OtlpDiskEncodeRecord(const DiskRecord &record, uint64_t sequence);
OtlpDiskSegmentScan OtlpDiskScanSegment(FileSystem &fs, const string &path, uint64_t segment, idx_t max_record_bytes,
                                        const std::set<uint64_t> &completed,
                                        vector<RecoveredDiskRecord> *pending = nullptr);
void OtlpDiskSyncDirectory(const string &path);

} // namespace duckdb
