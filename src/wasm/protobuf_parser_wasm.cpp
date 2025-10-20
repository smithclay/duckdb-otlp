#include "parsers/protobuf_parser.hpp"

namespace duckdb {

#ifdef DUCKSPAN_DISABLE_PROTOBUF

// WASM stub: Protobuf parsing is not supported in WASM builds
// Only JSON parsing is available

OTLPProtobufParser::OTLPProtobufParser() {
}

OTLPProtobufParser::~OTLPProtobufParser() {
}

idx_t OTLPProtobufParser::ParseTracesToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	throw NotImplementedException("Protobuf parsing is not supported in WASM builds. Use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseLogsToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	throw NotImplementedException("Protobuf parsing is not supported in WASM builds. Use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseMetricsToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	throw NotImplementedException("Protobuf parsing is not supported in WASM builds. Use JSON format instead.");
}

string OTLPProtobufParser::GetLastError() const {
	return "Protobuf parsing not available in WASM builds";
}

#endif // DUCKSPAN_DISABLE_PROTOBUF

} // namespace duckdb
