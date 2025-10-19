#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include <cctype>
#include <limits>
#include <string>

namespace duckdb {

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

} // namespace duckdb
