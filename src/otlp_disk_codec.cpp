#include "otlp_disk_codec.hpp"

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/local_file_system.hpp"

#include <cstring>
#include <sstream>

#ifdef _WIN32
#include "duckdb/common/windows.hpp"
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace {

static constexpr data_t OTLP_DISK_MAGIC[8] = {'O', 'T', 'L', 'P', 'B', 'U', 'F', '1'};

static void StoreU32LE(vector<data_t> &out, uint32_t value) {
	out.push_back(static_cast<data_t>(value & 0xFF));
	out.push_back(static_cast<data_t>((value >> 8) & 0xFF));
	out.push_back(static_cast<data_t>((value >> 16) & 0xFF));
	out.push_back(static_cast<data_t>((value >> 24) & 0xFF));
}

static void StoreU64LE(vector<data_t> &out, uint64_t value) {
	for (idx_t i = 0; i < 8; i++) {
		out.push_back(static_cast<data_t>((value >> (i * 8)) & 0xFF));
	}
}

static uint32_t LoadU32LE(const data_t *data) {
	return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
	       (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
}

static uint64_t LoadU64LE(const data_t *data) {
	uint64_t result = 0;
	for (idx_t i = 0; i < 8; i++) {
		result |= static_cast<uint64_t>(data[i]) << (i * 8);
	}
	return result;
}

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

static bool ReadExact(FileHandle &handle, data_t *buffer, idx_t len, bool &clean_eof) {
	clean_eof = false;
	idx_t read = 0;
	while (read < len) {
		auto nread = handle.Read(buffer + read, len - read);
		if (nread < 0) {
			throw IOException("Could not read OTLP disk buffer record");
		}
		if (nread == 0) {
			clean_eof = read == 0;
			return false;
		}
		read += static_cast<idx_t>(nread);
	}
	return true;
}

static bool ReadExactString(FileHandle &handle, string &buffer, idx_t len, bool &clean_eof) {
	buffer.resize(len);
	if (len == 0) {
		clean_eof = false;
		return true;
	}
	return ReadExact(handle, reinterpret_cast<data_t *>(&buffer[0]), len, clean_eof);
}

enum class ReadRecordStatus : uint8_t { OK, EOF_REACHED, TORN, CORRUPT };

// Minimal JSON field parser for TRUSTED, self-written record headers only — NOT a general JSON parser.
// These helpers (FindFieldValue / ParseJson*Field) locate a field via a raw `"<field>":` substring search,
// which is only safe because every string value emitted by OtlpDiskEncodeRecord is JsonEscape'd at write
// time. Escaping guarantees a bare `"` (and thus a spurious `"<field>":` token) can never appear inside a
// value, so the substring lookup cannot be fooled by attacker- or value-controlled content. The `json`
// argument is always just the header string (never the body), so the search is naturally scoped to the
// header. Do NOT feed untrusted JSON here, and keep this in lockstep with OtlpDiskEncodeRecord's escaping.
static idx_t FindFieldValue(const string &json, const string &field) {
	auto needle = "\"" + field + "\":";
	auto pos = json.find(needle);
	if (pos == string::npos) {
		throw InvalidInputException("OTLP disk buffer header missing field %s", field);
	}
	return pos + needle.size();
}

static string ParseJsonStringField(const string &json, const string &field) {
	auto pos = FindFieldValue(json, field);
	if (pos >= json.size() || json[pos] != '"') {
		throw InvalidInputException("OTLP disk buffer header field %s is not a string", field);
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
				throw InvalidInputException("OTLP disk buffer header has invalid escape");
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
				throw InvalidInputException("OTLP disk buffer header has unsupported escape");
			}
		} else {
			result.push_back(c);
		}
	}
	throw InvalidInputException("OTLP disk buffer header has unterminated string");
}

static uint64_t ParseJsonUInt64Field(const string &json, const string &field) {
	auto pos = FindFieldValue(json, field);
	if (pos >= json.size() || json[pos] < '0' || json[pos] > '9') {
		throw InvalidInputException("OTLP disk buffer header field %s is not an unsigned integer", field);
	}
	uint64_t value = 0;
	while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
		value = value * 10 + static_cast<uint64_t>(json[pos] - '0');
		pos++;
	}
	return value;
}

static int64_t ParseJsonInt64Field(const string &json, const string &field) {
	auto pos = FindFieldValue(json, field);
	bool neg = false;
	if (pos < json.size() && json[pos] == '-') {
		neg = true;
		pos++;
	}
	if (pos >= json.size() || json[pos] < '0' || json[pos] > '9') {
		throw InvalidInputException("OTLP disk buffer header field %s is not an integer", field);
	}
	int64_t value = 0;
	while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
		value = value * 10 + static_cast<int64_t>(json[pos] - '0');
		pos++;
	}
	return neg ? -value : value;
}

static ReadRecordStatus ReadRecordAt(FileHandle &handle, idx_t max_record_bytes, uint64_t &sequence, DiskRecord &record,
                                     idx_t &bytes_read, string &error) {
	idx_t started = handle.SeekPosition();
	data_t magic[8];
	bool clean_eof = false;
	if (!ReadExact(handle, magic, sizeof(magic), clean_eof)) {
		return clean_eof ? ReadRecordStatus::EOF_REACHED : ReadRecordStatus::TORN;
	}
	if (std::memcmp(magic, OTLP_DISK_MAGIC, sizeof(magic)) != 0) {
		error = "invalid OTLP disk buffer record magic";
		return ReadRecordStatus::CORRUPT;
	}

	data_t len_buf[8];
	if (!ReadExact(handle, len_buf, 4, clean_eof)) {
		return ReadRecordStatus::TORN;
	}
	auto header_len = LoadU32LE(len_buf);
	if (!ReadExact(handle, len_buf, 8, clean_eof)) {
		return ReadRecordStatus::TORN;
	}
	auto body_len = LoadU64LE(len_buf);
	if (header_len == 0 || body_len > max_record_bytes || body_len > NumericLimits<idx_t>::Maximum()) {
		error = "invalid OTLP disk buffer record length";
		return ReadRecordStatus::CORRUPT;
	}

	string header;
	if (!ReadExactString(handle, header, header_len, clean_eof)) {
		return ReadRecordStatus::TORN;
	}
	string body;
	if (!ReadExactString(handle, body, static_cast<idx_t>(body_len), clean_eof)) {
		return ReadRecordStatus::TORN;
	}

	try {
		sequence = ParseJsonUInt64Field(header, "sequence");
		auto kind_name = ParseJsonStringField(header, "request_kind");
		if (!OtlpRequestKindFromName(kind_name, record.request_kind)) {
			error = "invalid OTLP disk buffer request kind";
			return ReadRecordStatus::CORRUPT;
		}
		record.content_type = ParseJsonStringField(header, "content_type");
		record.content_encoding = ParseJsonStringField(header, "content_encoding");
		record.accepted_at_micros = ParseJsonInt64Field(header, "accepted_at_micros");
		auto body_checksum = ParseJsonUInt64Field(header, "body_checksum");
		if (body_checksum != OtlpDiskChecksum(body)) {
			error = "OTLP disk buffer record checksum mismatch";
			return ReadRecordStatus::CORRUPT;
		}
		record.body = std::move(body);
		bytes_read = handle.SeekPosition() - started;
		return ReadRecordStatus::OK;
	} catch (std::exception &ex) {
		error = ex.what();
		return ReadRecordStatus::CORRUPT;
	}
}

static bool ParseSegmentId(const string &name, uint64_t &segment) {
	static const string PREFIX = "segment-";
	static const string SUFFIX = ".otlpbuf";
	if (name.size() <= PREFIX.size() + SUFFIX.size()) {
		return false;
	}
	if (name.substr(0, PREFIX.size()) != PREFIX) {
		return false;
	}
	if (name.substr(name.size() - SUFFIX.size()) != SUFFIX) {
		return false;
	}
	auto raw = name.substr(PREFIX.size(), name.size() - PREFIX.size() - SUFFIX.size());
	if (raw.empty()) {
		return false;
	}
	uint64_t value = 0;
	for (auto c : raw) {
		if (c < '0' || c > '9') {
			return false;
		}
		value = value * 10 + static_cast<uint64_t>(c - '0');
	}
	segment = value;
	return true;
}

static string FormatSegmentName(uint64_t segment) {
	char buffer[64];
	snprintf(buffer, sizeof(buffer), "segment-%020llu.otlpbuf", static_cast<unsigned long long>(segment));
	return string(buffer);
}

} // namespace

uint64_t OtlpDiskChecksum(const string &body) {
	uint64_t hash = 0xcbf29ce484222325ULL;
	for (auto c : body) {
		hash ^= static_cast<uint8_t>(c);
		hash *= 0x100000001b3ULL;
	}
	return hash;
}

string OtlpDiskSegmentPath(FileSystem &fs, const string &dir, uint64_t segment) {
	return fs.JoinPath(dir, FormatSegmentName(segment));
}

string OtlpDiskCheckpointPath(FileSystem &fs, const string &dir) {
	return fs.JoinPath(dir, "checkpoint.log");
}

vector<uint64_t> OtlpDiskSegmentIds(FileSystem &fs, const string &dir) {
	vector<uint64_t> result;
	if (!fs.DirectoryExists(dir)) {
		return result;
	}
	fs.ListFiles(dir, [&](const string &name, bool is_dir) {
		if (is_dir) {
			return;
		}
		uint64_t segment = 0;
		if (ParseSegmentId(name, segment)) {
			result.push_back(segment);
		}
	});
	std::sort(result.begin(), result.end());
	return result;
}

std::set<uint64_t> OtlpDiskReadCompleted(FileSystem &fs, const string &path) {
	std::set<uint64_t> completed;
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	if (!handle) {
		return completed;
	}
	auto file_size = handle->GetFileSize();
	if (file_size == 0) {
		return completed;
	}
	string contents;
	contents.resize(file_size);
	handle->Read(reinterpret_cast<data_t *>(&contents[0]), file_size, 0);
	std::stringstream ss(contents);
	string line;
	while (std::getline(ss, line)) {
		StringUtil::Trim(line);
		if (line.empty()) {
			continue;
		}
		uint64_t value = 0;
		for (auto c : line) {
			if (c < '0' || c > '9') {
				throw IOException("Invalid OTLP disk buffer checkpoint sequence: %s", line);
			}
			value = value * 10 + static_cast<uint64_t>(c - '0');
		}
		completed.insert(value);
	}
	return completed;
}

EncodedDiskRecord OtlpDiskEncodeRecord(const DiskRecord &record, uint64_t sequence) {
	auto accepted_at = record.accepted_at_micros;
	if (accepted_at == 0) {
		accepted_at =
		    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
		        .count();
	}
	auto checksum = OtlpDiskChecksum(record.body);
	string header = "{\"sequence\":" + std::to_string(sequence) + ",\"request_kind\":\"" +
	                JsonEscape(OtlpRequestKindName(record.request_kind)) + "\",\"content_type\":\"" +
	                JsonEscape(record.content_type) + "\",\"content_encoding\":\"" +
	                JsonEscape(record.content_encoding) + "\",\"accepted_at_micros\":" + std::to_string(accepted_at) +
	                ",\"body_checksum\":" + std::to_string(checksum) + "}";
	if (header.size() > NumericLimits<uint32_t>::Maximum()) {
		throw IOException("OTLP disk buffer header is too large");
	}

	EncodedDiskRecord result;
	result.sequence = sequence;
	result.body_len = record.body.size();
	result.bytes.reserve(OTLP_DISK_RECORD_HEADER_BYTES + header.size() + record.body.size());
	result.bytes.insert(result.bytes.end(), OTLP_DISK_MAGIC, OTLP_DISK_MAGIC + sizeof(OTLP_DISK_MAGIC));
	StoreU32LE(result.bytes, static_cast<uint32_t>(header.size()));
	StoreU64LE(result.bytes, result.body_len);
	result.bytes.insert(result.bytes.end(), header.begin(), header.end());
	result.bytes.insert(result.bytes.end(), record.body.begin(), record.body.end());
	return result;
}

OtlpDiskSegmentScan OtlpDiskScanSegment(FileSystem &fs, const string &path, uint64_t segment, idx_t max_record_bytes,
                                        const std::set<uint64_t> &completed, vector<RecoveredDiskRecord> *pending) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE);
	OtlpDiskSegmentScan scan;
	while (true) {
		auto offset = handle->SeekPosition();
		uint64_t sequence = 0;
		idx_t bytes_read = 0;
		DiskRecord record;
		string error;
		auto status = ReadRecordAt(*handle, max_record_bytes, sequence, record, bytes_read, error);
		if (status == ReadRecordStatus::EOF_REACHED) {
			break;
		}
		if (status == ReadRecordStatus::TORN || status == ReadRecordStatus::CORRUPT) {
			scan.truncated_tail = true;
			scan.truncation_error = status == ReadRecordStatus::CORRUPT ? error : "torn OTLP disk buffer record";
			handle->Truncate(static_cast<int64_t>(offset));
			handle->Seek(offset);
			break;
		}
		scan.valid_len += bytes_read;
		if (scan.record_count == 0) {
			scan.seq_lo = sequence;
		}
		scan.seq_hi = MaxValue<uint64_t>(scan.seq_hi, sequence);
		scan.record_count++;
		if (pending && completed.find(sequence) == completed.end()) {
			pending->push_back(RecoveredDiskRecord {DiskRecordId {segment, sequence}, std::move(record)});
		}
	}
	return scan;
}

void OtlpDiskSyncDirectory(const string &path) {
#ifdef _WIN32
	auto handle = CreateFileA(path.c_str(), FILE_LIST_DIRECTORY, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
	                          nullptr, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr);
	if (handle == INVALID_HANDLE_VALUE) {
		throw IOException("Could not open directory \"%s\" for fsync", path);
	}
	if (!FlushFileBuffers(handle)) {
		auto err = GetLastError();
		CloseHandle(handle);
		throw IOException("Could not fsync directory \"%s\" (error %llu)", path, static_cast<unsigned long long>(err));
	}
	CloseHandle(handle);
#else
	auto fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		throw IOException("Could not open directory \"%s\" for fsync: %s", path, strerror(errno));
	}
	if (fsync(fd) != 0) {
		auto err = errno;
		close(fd);
		throw IOException("Could not fsync directory \"%s\": %s", path, strerror(err));
	}
	close(fd);
#endif
}

} // namespace duckdb
