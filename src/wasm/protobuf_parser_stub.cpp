// WASM stub implementation - protobuf not supported in WebAssembly builds
#include "parsers/protobuf_parser.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

OTLPProtobufParser::OTLPProtobufParser() {
}

OTLPProtobufParser::~OTLPProtobufParser() {
}

idx_t OTLPProtobufParser::ParseTracesToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	throw NotImplementedException(
	    "Protobuf parsing is not supported in WebAssembly builds. Please use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseTracesToTypedRows(google::protobuf::io::ZeroCopyInputStream &stream,
                                                 vector<vector<Value>> &rows) {
	throw NotImplementedException(
	    "Protobuf parsing is not supported in WebAssembly builds. Please use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseLogsToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	throw NotImplementedException(
	    "Protobuf parsing is not supported in WebAssembly builds. Please use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseLogsToTypedRows(google::protobuf::io::ZeroCopyInputStream &stream,
                                               vector<vector<Value>> &rows) {
	throw NotImplementedException(
	    "Protobuf parsing is not supported in WebAssembly builds. Please use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseMetricsToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	throw NotImplementedException(
	    "Protobuf parsing is not supported in WebAssembly builds. Please use JSON format instead.");
}

idx_t OTLPProtobufParser::ParseMetricsToTypedRows(google::protobuf::io::ZeroCopyInputStream &stream,
                                                  vector<vector<Value>> &rows) {
	throw NotImplementedException(
	    "Protobuf parsing is not supported in WebAssembly builds. Please use JSON format instead.");
}

string OTLPProtobufParser::GetLastError() const {
	return "Protobuf parsing is not supported in WebAssembly builds";
}

} // namespace duckdb
