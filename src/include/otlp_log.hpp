#pragma once

#include "duckdb/logging/log_type.hpp"

namespace duckdb {

//! Named log type for the OTLP HTTP ingest server. Registering it lets operators
//! see and filter server-side ingest diagnostics in duckdb_logs (log_type =
//! 'OTLP'). Routine lifecycle events (seals, catalog maintenance) write at INFO;
//! failure paths — which would otherwise only surface in a fire-and-forget
//! exporter's response — write at WARNING via LogServerEvent's level argument.
//! LEVEL is the type's default/informational level and the threshold applied
//! when the 'OTLP' log type is explicitly enabled.
class OtlpLogType : public LogType {
public:
	static constexpr const char *NAME = "OTLP";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	OtlpLogType() : LogType(NAME, LEVEL) {
	}
};

} // namespace duckdb
