#pragma once

#include "duckdb/logging/log_type.hpp"

namespace duckdb {

//! Named log type for the OTLP HTTP ingest server. Registering it lets operators
//! see and filter server-side ingest diagnostics in duckdb_logs (log_type =
//! 'OTLP') — every failure path on the request hot path writes here instead of
//! only returning an error to a fire-and-forget exporter.
class OtlpLogType : public LogType {
public:
	static constexpr const char *NAME = "OTLP";
	static constexpr LogLevel LEVEL = LogLevel::LOG_WARNING;

	OtlpLogType() : LogType(NAME, LEVEL) {
	}
};

} // namespace duckdb
