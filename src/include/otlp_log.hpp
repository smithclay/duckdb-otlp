#pragma once

#include "duckdb/logging/log_type.hpp"

namespace duckdb {

//! Ingest server diagnostics in duckdb_logs (log_type = 'OTLP'): lifecycle events at INFO,
//! failures at WARNING, since an exporter's response is otherwise the only place they surface.
class OtlpLogType : public LogType {
public:
	static constexpr const char *NAME = "OTLP";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	OtlpLogType() : LogType(NAME, LEVEL) {
	}
};

} // namespace duckdb
