#include "format_detector.hpp"

namespace duckdb {

#ifdef DUCKSPAN_DISABLE_PROTOBUF

// WASM stub: Always return JSON format (protobuf not supported)
OTLPFormat FormatDetector::DetectFormat(const char *data, size_t len) {
	// In WASM builds, we only support JSON format
	return OTLPFormat::JSON;
}

FormatDetector::SignalType FormatDetector::DetectProtobufSignalType(const char *data, size_t len) {
	// Protobuf not supported in WASM
	return SignalType::UNKNOWN;
}

#endif // DUCKSPAN_DISABLE_PROTOBUF

} // namespace duckdb
