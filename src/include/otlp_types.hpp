#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! OTLP signal types - the three types of telemetry data
enum class OTLPSignalType : uint8_t { TRACES = 0, METRICS = 1, LOGS = 2 };

//! Convert signal type enum to table name string
inline string SignalTypeToString(OTLPSignalType type) {
	switch (type) {
	case OTLPSignalType::TRACES:
		return "traces";
	case OTLPSignalType::METRICS:
		return "metrics";
	case OTLPSignalType::LOGS:
		return "logs";
	default:
		throw InternalException("Invalid OTLP signal type");
	}
}

//! Convert table name string to signal type enum
inline optional_ptr<OTLPSignalType> StringToSignalType(const string &name) {
	static OTLPSignalType traces_type = OTLPSignalType::TRACES;
	static OTLPSignalType metrics_type = OTLPSignalType::METRICS;
	static OTLPSignalType logs_type = OTLPSignalType::LOGS;

	if (name == "traces")
		return &traces_type;
	if (name == "metrics")
		return &metrics_type;
	if (name == "logs")
		return &logs_type;
	return nullptr;
}

//! Convert OTLP nanosecond timestamps to DuckDB microsecond timestamps
//! Uses rounding (not truncation) to avoid systematic negative bias
inline timestamp_t NanosToTimestamp(int64_t nanos, bool round = true) {
	timestamp_t ts;
	if (round) {
		// Round to nearest microsecond: add 500ns before dividing
		ts.value = (nanos + 500) / 1000;
	} else {
		// Truncate (faster, but introduces up to 999ns negative bias)
		ts.value = nanos / 1000;
	}
	return ts;
}

} // namespace duckdb
