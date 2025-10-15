#pragma once

#include "duckdb.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "schema/otlp_logs_schema.hpp"

namespace duckdb {

struct TracesRowData {
	timestamp_ns_t timestamp;
	string trace_id;
	string span_id;
	string parent_span_id;
	string trace_state;
	string span_name;
	string span_kind;
	string service_name;
	Value resource_attributes; // MAP<VARCHAR,VARCHAR>
	string scope_name;
	string scope_version;
	Value span_attributes; // MAP<VARCHAR,VARCHAR>
	int64_t duration_ns;
	string status_code;
	string status_message;
	vector<Value> events_timestamps;  // LIST<TIMESTAMP_NS> (elements)
	vector<Value> events_names;       // LIST<VARCHAR>
	vector<Value> events_attributes;  // LIST<MAP<VARCHAR,VARCHAR>>
	vector<Value> links_trace_ids;    // LIST<VARCHAR>
	vector<Value> links_span_ids;     // LIST<VARCHAR>
	vector<Value> links_trace_states; // LIST<VARCHAR>
	vector<Value> links_attributes;   // LIST<MAP<VARCHAR,VARCHAR>>
};

vector<Value> BuildTracesRow(const TracesRowData &d);

struct LogsRowData {
	timestamp_ns_t timestamp;
	string trace_id;
	string span_id;
	uint32_t trace_flags;
	string severity_text;
	int32_t severity_number;
	string service_name;
	string body;
	string resource_schema_url;
	Value resource_attributes; // MAP<VARCHAR,VARCHAR>
	string scope_schema_url;
	string scope_name;
	string scope_version;
	Value scope_attributes; // MAP<VARCHAR,VARCHAR>
	Value log_attributes;   // MAP<VARCHAR,VARCHAR>
};

vector<Value> BuildLogsRow(const LogsRowData &d);

} // namespace duckdb
