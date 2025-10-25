#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include <cctype>
#include <limits>
#include <string>

namespace duckdb {

//! Helper: Escape a string for JSON contexts (quotes, backslashes, control chars)
inline std::string EscapeJsonString(const std::string &input) {
	std::string result;
	result.reserve(input.size());
	static const char hex_chars[] = "0123456789abcdef";
	for (unsigned char ch : input) {
		switch (ch) {
		case '\"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
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
			if (ch < 0x20) {
				result += "\\u00";
				result.push_back(hex_chars[(ch >> 4) & 0x0F]);
				result.push_back(hex_chars[ch & 0x0F]);
			} else {
				result.push_back(static_cast<char>(ch));
			}
			break;
		}
	}
	return result;
}

//! Helper: Convert bytes to lowercase hex. Accepts both binary and already-hex strings.
inline std::string BytesToHex(const std::string &bytes) {
	if (bytes.size() % 2 == 0 && !bytes.empty()) {
		bool is_hex = true;
		for (char c : bytes) {
			if (!std::isxdigit(static_cast<unsigned char>(c))) {
				is_hex = false;
				break;
			}
		}
		if (is_hex) {
			std::string normalized = bytes;
			for (auto &c : normalized) {
				c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
			}
			return normalized;
		}
	}

	static const char hex_chars[] = "0123456789abcdef";
	std::string result;
	result.reserve(bytes.size() * 2);
	for (unsigned char c : bytes) {
		result.push_back(hex_chars[c >> 4]);
		result.push_back(hex_chars[c & 0x0F]);
	}
	return result;
}

//! Helper: Convert Unix nanoseconds to DuckDB TIMESTAMP_NS (clamped to valid range)
inline timestamp_ns_t NanosToTimestamp(uint64_t nanos) {
	constexpr uint64_t MAX_TIMESTAMP_NS = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
	if (nanos > MAX_TIMESTAMP_NS) {
		return timestamp_ns_t(std::numeric_limits<int64_t>::max());
	}
	return timestamp_ns_t(static_cast<int64_t>(nanos));
}

//! Helper: Clamp duration (end - start) to signed 64-bit range, guarding underflow.
inline int64_t ClampDuration(uint64_t start_nanos, uint64_t end_nanos) {
	if (end_nanos <= start_nanos) {
		return 0;
	}
	uint64_t diff = end_nanos - start_nanos;
	const uint64_t max_duration = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
	if (diff > max_duration) {
		return std::numeric_limits<int64_t>::max();
	}
	return static_cast<int64_t>(diff);
}

} // namespace duckdb
