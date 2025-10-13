#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! OTLP signal types - the three types of telemetry data
enum class OTLPSignalType : uint8_t { TRACES = 0, METRICS = 1, LOGS = 2 };

//! OTLP table types - 3 tables with union schema for metrics
enum class OTLPTableType : uint8_t {
	TRACES = 0,
	LOGS = 1,
	METRICS = 2 // Single table with union schema (27 columns, MetricType discriminator)
};

//! Metric data types from OTLP spec
enum class OTLPMetricType : uint8_t {
	GAUGE = 0,
	SUM = 1,
	HISTOGRAM = 2,
	EXPONENTIAL_HISTOGRAM = 3,
	SUMMARY = 4,
	UNKNOWN = 255
};

//! Convert signal type enum to table name string (old 3-table schema)
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

//! Convert table type enum to table name string (3-table union schema)
inline string TableTypeToString(OTLPTableType type) {
	switch (type) {
	case OTLPTableType::TRACES:
		return "otel_traces";
	case OTLPTableType::LOGS:
		return "otel_logs";
	case OTLPTableType::METRICS:
		return "otel_metrics";
	default:
		throw InternalException("Invalid OTLP table type");
	}
}

//! Convert table name string to table type enum
inline optional_ptr<OTLPTableType> StringToTableType(const string &name) {
	static OTLPTableType traces_type = OTLPTableType::TRACES;
	static OTLPTableType logs_type = OTLPTableType::LOGS;
	static OTLPTableType metrics_type = OTLPTableType::METRICS;

	if (name == "otel_traces") {
		return &traces_type;
	}
	if (name == "otel_logs") {
		return &logs_type;
	}
	if (name == "otel_metrics") {
		return &metrics_type;
	}
	return nullptr;
}

//! Convert table name string to signal type enum (old API for compatibility)
inline optional_ptr<OTLPSignalType> StringToSignalType(const string &name) {
	static OTLPSignalType traces_type = OTLPSignalType::TRACES;
	static OTLPSignalType metrics_type = OTLPSignalType::METRICS;
	static OTLPSignalType logs_type = OTLPSignalType::LOGS;

	if (name == "traces" || name == "otel_traces") {
		return &traces_type;
	}
	if (name == "logs" || name == "otel_logs") {
		return &logs_type;
	}
	if (name == "metrics" || name == "otel_metrics") {
		return &metrics_type;
	}
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
