#include "receiver/row_builders_traces_logs.hpp"

namespace duckdb {

vector<Value> BuildTracesRow(const TracesRowData &d) {
	vector<Value> row(OTLPTracesSchema::COLUMN_COUNT);
	row[OTLPTracesSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(d.timestamp);
	row[OTLPTracesSchema::COL_TRACE_ID] = Value(d.trace_id);
	row[OTLPTracesSchema::COL_SPAN_ID] = Value(d.span_id);
	row[OTLPTracesSchema::COL_PARENT_SPAN_ID] = Value(d.parent_span_id);
	row[OTLPTracesSchema::COL_TRACE_STATE] = Value(d.trace_state);
	row[OTLPTracesSchema::COL_SPAN_NAME] = Value(d.span_name);
	row[OTLPTracesSchema::COL_SPAN_KIND] = Value(d.span_kind);
	row[OTLPTracesSchema::COL_SERVICE_NAME] = Value(d.service_name);
	row[OTLPTracesSchema::COL_RESOURCE_ATTRIBUTES] = d.resource_attributes;
	row[OTLPTracesSchema::COL_SCOPE_NAME] = Value(d.scope_name);
	row[OTLPTracesSchema::COL_SCOPE_VERSION] = Value(d.scope_version);
	row[OTLPTracesSchema::COL_SPAN_ATTRIBUTES] = d.span_attributes;
	row[OTLPTracesSchema::COL_DURATION] = Value::BIGINT(d.duration_ns);
	row[OTLPTracesSchema::COL_STATUS_CODE] = Value(d.status_code);
	row[OTLPTracesSchema::COL_STATUS_MESSAGE] = Value(d.status_message);
	row[OTLPTracesSchema::COL_EVENTS_TIMESTAMP] = Value::LIST(LogicalType::TIMESTAMP_NS, d.events_timestamps);
	row[OTLPTracesSchema::COL_EVENTS_NAME] = Value::LIST(LogicalType::VARCHAR, d.events_names);
	row[OTLPTracesSchema::COL_EVENTS_ATTRIBUTES] =
	    Value::LIST(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), d.events_attributes);
	row[OTLPTracesSchema::COL_LINKS_TRACE_ID] = Value::LIST(LogicalType::VARCHAR, d.links_trace_ids);
	row[OTLPTracesSchema::COL_LINKS_SPAN_ID] = Value::LIST(LogicalType::VARCHAR, d.links_span_ids);
	row[OTLPTracesSchema::COL_LINKS_TRACE_STATE] = Value::LIST(LogicalType::VARCHAR, d.links_trace_states);
	row[OTLPTracesSchema::COL_LINKS_ATTRIBUTES] =
	    Value::LIST(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), d.links_attributes);
	return row;
}

vector<Value> BuildLogsRow(const LogsRowData &d) {
	vector<Value> row(OTLPLogsSchema::COLUMN_COUNT);
	row[OTLPLogsSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(d.timestamp);
	row[OTLPLogsSchema::COL_TRACE_ID] = Value(d.trace_id);
	row[OTLPLogsSchema::COL_SPAN_ID] = Value(d.span_id);
	row[OTLPLogsSchema::COL_TRACE_FLAGS] = Value::UINTEGER(d.trace_flags);
	row[OTLPLogsSchema::COL_SEVERITY_TEXT] = Value(d.severity_text);
	row[OTLPLogsSchema::COL_SEVERITY_NUMBER] = Value::INTEGER(d.severity_number);
	row[OTLPLogsSchema::COL_SERVICE_NAME] = Value(d.service_name);
	row[OTLPLogsSchema::COL_BODY] = Value(d.body);
	row[OTLPLogsSchema::COL_RESOURCE_SCHEMA_URL] = Value(d.resource_schema_url);
	row[OTLPLogsSchema::COL_RESOURCE_ATTRIBUTES] = d.resource_attributes;
	row[OTLPLogsSchema::COL_SCOPE_SCHEMA_URL] = Value(d.scope_schema_url);
	row[OTLPLogsSchema::COL_SCOPE_NAME] = Value(d.scope_name);
	row[OTLPLogsSchema::COL_SCOPE_VERSION] = Value(d.scope_version);
	row[OTLPLogsSchema::COL_SCOPE_ATTRIBUTES] = d.scope_attributes;
	row[OTLPLogsSchema::COL_LOG_ATTRIBUTES] = d.log_attributes;
	return row;
}

} // namespace duckdb
