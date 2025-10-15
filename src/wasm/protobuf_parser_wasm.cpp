#include "parsers/protobuf_parser.hpp"

namespace duckdb {

#ifdef DUCKSPAN_DISABLE_PROTOBUF

// WASM stub: Protobuf parsing is not supported in WASM builds
// Only JSON parsing is available

OTLPProtobufParser::OTLPProtobufParser() {
}

OTLPProtobufParser::~OTLPProtobufParser() {
}

idx_t OTLPProtobufParser::ParseTracesData(const char *data, size_t length, vector<timestamp_t> &timestamps,
                                          vector<string> &resources, vector<string> &datas) {
	throw NotImplementedException("Protobuf parsing is not supported in WASM builds. Use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseMetricsData(const char *data, size_t length, vector<timestamp_t> &timestamps,
                                           vector<string> &resources, vector<string> &datas) {
	throw NotImplementedException("Protobuf parsing is not supported in WASM builds. Use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseLogsData(const char *data, size_t length, vector<timestamp_t> &timestamps,
                                        vector<string> &resources, vector<string> &datas) {
	throw NotImplementedException("Protobuf parsing is not supported in WASM builds. Use JSON format instead.");
}

string OTLPProtobufParser::GetLastError() const {
	return "Protobuf parsing not available in WASM builds";
}

#endif // DUCKSPAN_DISABLE_PROTOBUF

} // namespace duckdb
