#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! OTLP signal types - the three gRPC service types (OTLP protocol level)
enum class OTLPSignalType : uint8_t { TRACES = 0, METRICS = 1, LOGS = 2 };

//! OTLP table types - 7 tables (1 traces, 1 logs, 5 metric types)
enum class OTLPTableType : uint8_t {
	TRACES = 0,
	LOGS = 1,
	METRICS_GAUGE = 2,
	METRICS_SUM = 3,
	METRICS_HISTOGRAM = 4,
	METRICS_EXP_HISTOGRAM = 5,
	METRICS_SUMMARY = 6
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

//! Convert table type enum to table name string
inline string TableTypeToString(OTLPTableType type) {
	switch (type) {
	case OTLPTableType::TRACES:
		return "otel_traces";
	case OTLPTableType::LOGS:
		return "otel_logs";
	case OTLPTableType::METRICS_GAUGE:
		return "otel_metrics_gauge";
	case OTLPTableType::METRICS_SUM:
		return "otel_metrics_sum";
	case OTLPTableType::METRICS_HISTOGRAM:
		return "otel_metrics_histogram";
	case OTLPTableType::METRICS_EXP_HISTOGRAM:
		return "otel_metrics_exp_histogram";
	case OTLPTableType::METRICS_SUMMARY:
		return "otel_metrics_summary";
	default:
		throw InternalException("Invalid OTLP table type");
	}
}

//! Convert table name string to table type enum
inline optional_ptr<OTLPTableType> StringToTableType(const string &name) {
	static OTLPTableType traces_type = OTLPTableType::TRACES;
	static OTLPTableType logs_type = OTLPTableType::LOGS;
	static OTLPTableType metrics_gauge_type = OTLPTableType::METRICS_GAUGE;
	static OTLPTableType metrics_sum_type = OTLPTableType::METRICS_SUM;
	static OTLPTableType metrics_histogram_type = OTLPTableType::METRICS_HISTOGRAM;
	static OTLPTableType metrics_exp_histogram_type = OTLPTableType::METRICS_EXP_HISTOGRAM;
	static OTLPTableType metrics_summary_type = OTLPTableType::METRICS_SUMMARY;

	if (name == "otel_traces") {
		return &traces_type;
	}
	if (name == "otel_logs") {
		return &logs_type;
	}
	if (name == "otel_metrics_gauge") {
		return &metrics_gauge_type;
	}
	if (name == "otel_metrics_sum") {
		return &metrics_sum_type;
	}
	if (name == "otel_metrics_histogram") {
		return &metrics_histogram_type;
	}
	if (name == "otel_metrics_exp_histogram") {
		return &metrics_exp_histogram_type;
	}
	if (name == "otel_metrics_summary") {
		return &metrics_summary_type;
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
