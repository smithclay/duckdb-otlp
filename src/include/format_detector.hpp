#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class OTLPFormat { JSON, PROTOBUF, UNKNOWN };

//! FormatDetector detects whether data is OTLP JSON or Protobuf format
class FormatDetector {
public:
	//! Detect format from file data
	//! Reads first few bytes to determine format
	static OTLPFormat DetectFormat(const char *data, size_t len);

	//! Detect signal type from protobuf data (TRACES, METRICS, or LOGS)
	enum class SignalType { TRACES, METRICS, LOGS, UNKNOWN };
	static SignalType DetectProtobufSignalType(const char *data, size_t len);
};

} // namespace duckdb
