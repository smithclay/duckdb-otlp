#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Schema definition for otel_logs table
class OTLPLogsSchema {
public:
	// Column indices
	static constexpr idx_t COL_TIMESTAMP = 0;
	static constexpr idx_t COL_TRACE_ID = 1;
	static constexpr idx_t COL_SPAN_ID = 2;
	static constexpr idx_t COL_TRACE_FLAGS = 3;
	static constexpr idx_t COL_SEVERITY_TEXT = 4;
	static constexpr idx_t COL_SEVERITY_NUMBER = 5;
	static constexpr idx_t COL_SERVICE_NAME = 6;
	static constexpr idx_t COL_BODY = 7;
	static constexpr idx_t COL_RESOURCE_SCHEMA_URL = 8;
	static constexpr idx_t COL_RESOURCE_ATTRIBUTES = 9;
	static constexpr idx_t COL_SCOPE_SCHEMA_URL = 10;
	static constexpr idx_t COL_SCOPE_NAME = 11;
	static constexpr idx_t COL_SCOPE_VERSION = 12;
	static constexpr idx_t COL_SCOPE_ATTRIBUTES = 13;
	static constexpr idx_t COL_LOG_ATTRIBUTES = 14;
	static constexpr idx_t COLUMN_COUNT = 15;

	//! Get column names
	static vector<string> GetColumnNames() {
		return {"Timestamp",      "TraceId",     "SpanId",       "TraceFlags",        "SeverityText",
		        "SeverityNumber", "ServiceName", "Body",         "ResourceSchemaUrl", "ResourceAttributes",
		        "ScopeSchemaUrl", "ScopeName",   "ScopeVersion", "ScopeAttributes",   "LogAttributes"};
	}

	//! Get column types
	static vector<LogicalType> GetColumnTypes() {
		auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);

		return {
		    LogicalType::TIMESTAMP_NS, // Timestamp
		    LogicalType::VARCHAR,      // TraceId
		    LogicalType::VARCHAR,      // SpanId
		    LogicalType::UINTEGER,     // TraceFlags
		    LogicalType::VARCHAR,      // SeverityText
		    LogicalType::INTEGER,      // SeverityNumber
		    LogicalType::VARCHAR,      // ServiceName
		    LogicalType::VARCHAR,      // Body
		    LogicalType::VARCHAR,      // ResourceSchemaUrl
		    map_type,                  // ResourceAttributes
		    LogicalType::VARCHAR,      // ScopeSchemaUrl
		    LogicalType::VARCHAR,      // ScopeName
		    LogicalType::VARCHAR,      // ScopeVersion
		    map_type,                  // ScopeAttributes
		    map_type                   // LogAttributes
		};
	}
};

} // namespace duckdb
