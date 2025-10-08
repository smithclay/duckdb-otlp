#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/timestamp.hpp"

// Forward declare google::protobuf::Message
namespace google {
namespace protobuf {
class Message;
}
} // namespace google

namespace duckdb {

//! OTLPProtobufParser handles parsing OTLP protobuf binary format
//! Supports traces, metrics, and logs signals
class OTLPProtobufParser {
public:
	OTLPProtobufParser();
	~OTLPProtobufParser();

	//! Parse OTLP protobuf data and extract rows
	//! Returns number of rows extracted
	idx_t ParseTracesData(const char *data, size_t length, vector<timestamp_t> &timestamps, vector<string> &resources,
	                      vector<string> &datas);

	idx_t ParseMetricsData(const char *data, size_t length, vector<timestamp_t> &timestamps, vector<string> &resources,
	                       vector<string> &datas);

	idx_t ParseLogsData(const char *data, size_t length, vector<timestamp_t> &timestamps, vector<string> &resources,
	                    vector<string> &datas);

	//! Get the last parsing error message
	string GetLastError() const;

private:
	//! Convert protobuf Message to JSON string
	string MessageToJSON(const google::protobuf::Message &msg);

	string last_error;
};

} // namespace duckdb
