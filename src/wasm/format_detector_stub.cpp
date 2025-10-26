// WASM stub implementation - only JSON format detection supported
#include "parsers/format_detector.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstring>

namespace duckdb {

OTLPFormat FormatDetector::DetectFormat(const char *data, size_t len) {
	if (len == 0 || data == nullptr) {
		return OTLPFormat::UNKNOWN;
	}

	const unsigned char *buffer = reinterpret_cast<const unsigned char *>(data);
	size_t remaining = len;

	// Strip UTF-8 BOM if present
	const unsigned char utf8_bom[] = {0xEF, 0xBB, 0xBF};
	if (remaining >= 3 && memcmp(buffer, utf8_bom, 3) == 0) {
		buffer += 3;
		remaining -= 3;
	}

	// Skip ASCII whitespace
	size_t i = 0;
	while (i < remaining && StringUtil::CharacterIsSpace(static_cast<char>(buffer[i]))) {
		i++;
	}

	if (i < remaining) {
		char first_char = static_cast<char>(buffer[i]);
		if (first_char == '{' || first_char == '[') {
			return OTLPFormat::JSON;
		}
	}

	// In WASM builds, we don't support protobuf
	// If it doesn't look like JSON, return UNKNOWN (not PROTOBUF)
	return OTLPFormat::UNKNOWN;
}

FormatDetector::SignalType FormatDetector::DetectProtobufSignalType(const char *data, size_t len) {
	// Protobuf signal type detection not supported in WASM builds
	return SignalType::UNKNOWN;
}

} // namespace duckdb
