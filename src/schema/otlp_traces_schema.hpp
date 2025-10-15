#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Schema definition for otel_traces table
class OTLPTracesSchema {
public:
	// Column indices
	static constexpr idx_t COL_TIMESTAMP = 0;
	static constexpr idx_t COL_TRACE_ID = 1;
	static constexpr idx_t COL_SPAN_ID = 2;
	static constexpr idx_t COL_PARENT_SPAN_ID = 3;
	static constexpr idx_t COL_TRACE_STATE = 4;
	static constexpr idx_t COL_SPAN_NAME = 5;
	static constexpr idx_t COL_SPAN_KIND = 6;
	static constexpr idx_t COL_SERVICE_NAME = 7;
	static constexpr idx_t COL_RESOURCE_ATTRIBUTES = 8;
	static constexpr idx_t COL_SCOPE_NAME = 9;
	static constexpr idx_t COL_SCOPE_VERSION = 10;
	static constexpr idx_t COL_SPAN_ATTRIBUTES = 11;
	static constexpr idx_t COL_DURATION = 12;
	static constexpr idx_t COL_STATUS_CODE = 13;
	static constexpr idx_t COL_STATUS_MESSAGE = 14;
	static constexpr idx_t COL_EVENTS_TIMESTAMP = 15;
	static constexpr idx_t COL_EVENTS_NAME = 16;
	static constexpr idx_t COL_EVENTS_ATTRIBUTES = 17;
	static constexpr idx_t COL_LINKS_TRACE_ID = 18;
	static constexpr idx_t COL_LINKS_SPAN_ID = 19;
	static constexpr idx_t COL_LINKS_TRACE_STATE = 20;
	static constexpr idx_t COL_LINKS_ATTRIBUTES = 21;
	static constexpr idx_t COLUMN_COUNT = 22;

	//! Get column names
	static vector<string> GetColumnNames() {
		return {"Timestamp",          "TraceId",           "SpanId",        "ParentSpanId",
		        "TraceState",         "SpanName",          "SpanKind",      "ServiceName",
		        "ResourceAttributes", "ScopeName",         "ScopeVersion",  "SpanAttributes",
		        "Duration",           "StatusCode",        "StatusMessage", "Events.Timestamp",
		        "Events.Name",        "Events.Attributes", "Links.TraceId", "Links.SpanId",
		        "Links.TraceState",   "Links.Attributes"};
	}

	//! Get column types
	static vector<LogicalType> GetColumnTypes() {
		auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
		auto timestamp_list = LogicalType::LIST(LogicalType::TIMESTAMP_NS);
		auto varchar_list = LogicalType::LIST(LogicalType::VARCHAR);
		auto map_list = LogicalType::LIST(map_type);

		return {
		    LogicalType::TIMESTAMP_NS, // Timestamp
		    LogicalType::VARCHAR,      // TraceId
		    LogicalType::VARCHAR,      // SpanId
		    LogicalType::VARCHAR,      // ParentSpanId
		    LogicalType::VARCHAR,      // TraceState
		    LogicalType::VARCHAR,      // SpanName
		    LogicalType::VARCHAR,      // SpanKind
		    LogicalType::VARCHAR,      // ServiceName
		    map_type,                  // ResourceAttributes
		    LogicalType::VARCHAR,      // ScopeName
		    LogicalType::VARCHAR,      // ScopeVersion
		    map_type,                  // SpanAttributes
		    LogicalType::BIGINT,       // Duration (nanoseconds)
		    LogicalType::VARCHAR,      // StatusCode
		    LogicalType::VARCHAR,      // StatusMessage
		    timestamp_list,            // Events.Timestamp
		    varchar_list,              // Events.Name
		    map_list,                  // Events.Attributes
		    varchar_list,              // Links.TraceId
		    varchar_list,              // Links.SpanId
		    varchar_list,              // Links.TraceState
		    map_list                   // Links.Attributes
		};
	}
};

} // namespace duckdb
