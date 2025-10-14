#include "json_parser.hpp"
#include "otlp_helpers.hpp"
#include "otlp_traces_schema.hpp"
#include "otlp_logs_schema.hpp"
#include "otlp_metrics_union_schema.hpp"
#include "row_builders_traces_logs.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"

namespace duckdb {

OTLPJSONParser::OTLPJSONParser() : last_error("") {
}

OTLPJSONParser::~OTLPJSONParser() {
}

bool OTLPJSONParser::IsValidOTLPJSON(const string &line) {
	if (line.empty()) {
		return false;
	}

	// Quick check: should start with '{'
	string trimmed = line;
	StringUtil::Trim(trimmed);
	if (trimmed.empty() || trimmed[0] != '{') {
		return false;
	}

	// Parse as JSON to validate
	auto doc = duckdb_yyjson::yyjson_read(trimmed.c_str(), trimmed.size(), 0);
	if (!doc) {
		return false;
	}

	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	bool valid = duckdb_yyjson::yyjson_is_obj(root);
	duckdb_yyjson::yyjson_doc_free(doc);

	return valid;
}

OTLPJSONParser::SignalType OTLPJSONParser::DetectSignalType(const string &json) {
	// Detect based on top-level keys
	if (json.find("resourceSpans") != string::npos) {
		return SignalType::TRACES;
	} else if (json.find("resourceMetrics") != string::npos) {
		return SignalType::METRICS;
	} else if (json.find("resourceLogs") != string::npos) {
		return SignalType::LOGS;
	}
	return SignalType::UNKNOWN;
}

bool OTLPJSONParser::ExtractTimestamp(const string &json, timestamp_t &ts) {
	auto doc = duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		return false;
	}

	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	bool found = false;

	// Try to find timestamp in various OTLP locations
	// For traces: resourceSpans[0].scopeSpans[0].spans[0].startTimeUnixNano
	// For metrics: resourceMetrics[0].scopeMetrics[0].metrics[0].gauge.dataPoints[0].timeUnixNano
	// For logs: resourceLogs[0].scopeLogs[0].logRecords[0].timeUnixNano

	// For now, use current time if we can't find a timestamp
	// This will be improved in later iterations
	ts = Timestamp::GetCurrentTimestamp();
	found = true;

	duckdb_yyjson::yyjson_doc_free(doc);
	return found;
}

// Helper: Get resource field name for signal type
static const char *GetResourceFieldName(OTLPJSONParser::SignalType type) {
	switch (type) {
	case OTLPJSONParser::SignalType::TRACES:
		return "resourceSpans";
	case OTLPJSONParser::SignalType::METRICS:
		return "resourceMetrics";
	case OTLPJSONParser::SignalType::LOGS:
		return "resourceLogs";
	default:
		return nullptr;
	}
}

bool OTLPJSONParser::ExtractResource(const string &json, string &resource) {
	auto doc = duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		return false;
	}

	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	duckdb_yyjson::yyjson_val *resource_val = nullptr;

	// Try to extract resource based on signal type (unified logic)
	auto signal_type = DetectSignalType(json);
	const char *field_name = GetResourceFieldName(signal_type);

	if (field_name) {
		auto resource_array = duckdb_yyjson::yyjson_obj_get(root, field_name);
		if (resource_array && duckdb_yyjson::yyjson_is_arr(resource_array)) {
			auto first_elem = duckdb_yyjson::yyjson_arr_get_first(resource_array);
			if (first_elem) {
				resource_val = duckdb_yyjson::yyjson_obj_get(first_elem, "resource");
			}
		}
	}

	bool found = false;
	if (resource_val) {
		// Convert resource object to JSON string
		auto json_str = duckdb_yyjson::yyjson_val_write(resource_val, 0, nullptr);
		if (json_str) {
			resource = string(json_str);
			free(json_str);
			found = true;
		}
	}

	if (!found) {
		// Default to empty object
		resource = "{}";
		found = true;
	}

	duckdb_yyjson::yyjson_doc_free(doc);
	return found;
}

bool OTLPJSONParser::ExtractData(const string &json, string &data) {
	// For Phase 1, store the entire JSON as data
	// Later phases will extract specific signal data
	data = json;
	return true;
}

bool OTLPJSONParser::ParseLine(const string &line, timestamp_t &timestamp, string &resource, string &data) {
	last_error = "";

	// Quick check: should start with '{'
	string trimmed = line;
	StringUtil::Trim(trimmed);
	if (trimmed.empty() || trimmed[0] != '{') {
		last_error = "Invalid JSON format";
		return false;
	}

	// Parse JSON once and reuse the document for all extractions
	auto doc = duckdb_yyjson::yyjson_read(trimmed.c_str(), trimmed.size(), 0);
	if (!doc) {
		last_error = "Invalid JSON format";
		return false;
	}

	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	if (!duckdb_yyjson::yyjson_is_obj(root)) {
		duckdb_yyjson::yyjson_doc_free(doc);
		last_error = "Invalid JSON format";
		return false;
	}

	// Detect signal type from root object (single scan, no string search)
	SignalType signal_type = SignalType::UNKNOWN;
	const char *field_name = nullptr;

	if (duckdb_yyjson::yyjson_obj_get(root, "resourceSpans")) {
		signal_type = SignalType::TRACES;
		field_name = "resourceSpans";
	} else if (duckdb_yyjson::yyjson_obj_get(root, "resourceMetrics")) {
		signal_type = SignalType::METRICS;
		field_name = "resourceMetrics";
	} else if (duckdb_yyjson::yyjson_obj_get(root, "resourceLogs")) {
		signal_type = SignalType::LOGS;
		field_name = "resourceLogs";
	}

	if (signal_type == SignalType::UNKNOWN) {
		duckdb_yyjson::yyjson_doc_free(doc);
		last_error = "Not OTLP format - missing resourceSpans/resourceMetrics/resourceLogs";
		return false;
	}

	// Extract resource from the same document
	duckdb_yyjson::yyjson_val *resource_val = nullptr;
	auto resource_array = duckdb_yyjson::yyjson_obj_get(root, field_name);
	if (resource_array && duckdb_yyjson::yyjson_is_arr(resource_array)) {
		auto first_elem = duckdb_yyjson::yyjson_arr_get_first(resource_array);
		if (first_elem) {
			resource_val = duckdb_yyjson::yyjson_obj_get(first_elem, "resource");
		}
	}

	if (resource_val) {
		auto json_str = duckdb_yyjson::yyjson_val_write(resource_val, 0, nullptr);
		if (json_str) {
			resource = string(json_str);
			free(json_str);
		} else {
			resource = "{}";
		}
	} else {
		resource = "{}";
	}

	// Extract timestamp (use current time for now, will improve later)
	timestamp = Timestamp::GetCurrentTimestamp();

	// Store entire JSON as data
	data = trimmed;

	// Free document
	duckdb_yyjson::yyjson_doc_free(doc);

	return true;
}

string OTLPJSONParser::GetLastError() const {
	return last_error;
}

//===--------------------------------------------------------------------===//
// V2 Schema: Typed Row Parsing Methods for JSON
//===--------------------------------------------------------------------===//

// Helper: Get string value from JSON, empty string if missing
static string GetStringValue(duckdb_yyjson::yyjson_val *obj, const char *key) {
	if (!obj)
		return "";
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val)
		return "";
	auto str = duckdb_yyjson::yyjson_get_str(val);
	return str ? string(str) : "";
}

// Helper: Get uint64 value from JSON string (for nanosecond timestamps)
static uint64_t GetUint64Value(duckdb_yyjson::yyjson_val *obj, const char *key) {
	if (!obj)
		return 0;
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val)
		return 0;

	// OTLP often encodes uint64 as strings to avoid precision loss
	if (duckdb_yyjson::yyjson_is_str(val)) {
		auto str = duckdb_yyjson::yyjson_get_str(val);
		if (str) {
			try {
				return std::stoull(string(str));
			} catch (const std::exception &) {
				// Malformed timestamp (non-numeric or overflow) - return 0
				return 0;
			}
		}
	} else if (duckdb_yyjson::yyjson_is_uint(val)) {
		return duckdb_yyjson::yyjson_get_uint(val);
	}
	return 0;
}

// Helper: Get int value from JSON
static int32_t GetIntValue(duckdb_yyjson::yyjson_val *obj, const char *key, int32_t default_val = 0) {
	if (!obj)
		return default_val;
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val)
		return default_val;
	if (duckdb_yyjson::yyjson_is_int(val)) {
		return (int32_t)duckdb_yyjson::yyjson_get_int(val);
	}
	return default_val;
}

// Helper: Get uint value from JSON
static uint32_t GetUintValue(duckdb_yyjson::yyjson_val *obj, const char *key, uint32_t default_val = 0) {
	if (!obj)
		return default_val;
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val)
		return default_val;
	if (duckdb_yyjson::yyjson_is_uint(val)) {
		return (uint32_t)duckdb_yyjson::yyjson_get_uint(val);
	}
	return default_val;
}

// Helper: Get double value from JSON
static double GetDoubleValue(duckdb_yyjson::yyjson_val *obj, const char *key, double default_val = 0.0) {
	if (!obj)
		return default_val;
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val)
		return default_val;
	if (duckdb_yyjson::yyjson_is_real(val)) {
		return duckdb_yyjson::yyjson_get_real(val);
	} else if (duckdb_yyjson::yyjson_is_int(val)) {
		return (double)duckdb_yyjson::yyjson_get_int(val);
	}
	return default_val;
}

// Helper: Convert OTLP JSON AnyValue object to string
static string JSONAnyToString(duckdb_yyjson::yyjson_val *any_obj) {
	if (!any_obj || !duckdb_yyjson::yyjson_is_obj(any_obj)) {
		return "";
	}
	auto s = duckdb_yyjson::yyjson_obj_get(any_obj, "stringValue");
	if (s && duckdb_yyjson::yyjson_is_str(s)) {
		auto str = duckdb_yyjson::yyjson_get_str(s);
		return str ? string(str) : "";
	}
	auto i = duckdb_yyjson::yyjson_obj_get(any_obj, "intValue");
	if (i && duckdb_yyjson::yyjson_is_int(i)) {
		return std::to_string(duckdb_yyjson::yyjson_get_int(i));
	}
	auto d = duckdb_yyjson::yyjson_obj_get(any_obj, "doubleValue");
	if (d && duckdb_yyjson::yyjson_is_real(d)) {
		return std::to_string(duckdb_yyjson::yyjson_get_real(d));
	}
	auto b = duckdb_yyjson::yyjson_obj_get(any_obj, "boolValue");
	if (b && (duckdb_yyjson::yyjson_is_true(b) || duckdb_yyjson::yyjson_is_false(b))) {
		return duckdb_yyjson::yyjson_is_true(b) ? "true" : "false";
	}
	auto kvl = duckdb_yyjson::yyjson_obj_get(any_obj, "kvlistValue");
	if (kvl) {
		auto values = duckdb_yyjson::yyjson_obj_get(kvl, "values");
		string out = "{";
		bool first = true;
		if (values && duckdb_yyjson::yyjson_is_arr(values)) {
			size_t iidx, imax;
			duckdb_yyjson::yyjson_val *item;
			yyjson_arr_foreach(values, iidx, imax, item) {
				if (!duckdb_yyjson::yyjson_is_obj(item))
					continue;
				auto key = GetStringValue(item, "key");
				auto v = duckdb_yyjson::yyjson_obj_get(item, "value");
				if (!first)
					out += ",";
				first = false;
				out += '"' + key + '"';
				out += ":";
				out += '"' + JSONAnyToString(v) + '"';
			}
		}
		out += "}";
		return out;
	}
	auto arr = duckdb_yyjson::yyjson_obj_get(any_obj, "arrayValue");
	if (arr) {
		auto values = duckdb_yyjson::yyjson_obj_get(arr, "values");
		string out = "[";
		bool first = true;
		if (values && duckdb_yyjson::yyjson_is_arr(values)) {
			size_t iidx, imax;
			duckdb_yyjson::yyjson_val *item;
			yyjson_arr_foreach(values, iidx, imax, item) {
				if (!first)
					out += ",";
				first = false;
				out += '"' + JSONAnyToString(item) + '"';
			}
		}
		out += "]";
		return out;
	}
	return "";
}

// Build DuckDB MAP<VARCHAR,VARCHAR> from OTLP JSON attributes array
static Value JSONAttributesToMap(duckdb_yyjson::yyjson_val *attributes_arr) {
	vector<Value> keys;
	vector<Value> vals;
	if (attributes_arr && duckdb_yyjson::yyjson_is_arr(attributes_arr)) {
		size_t idx, max;
		duckdb_yyjson::yyjson_val *attr;
		yyjson_arr_foreach(attributes_arr, idx, max, attr) {
			if (!duckdb_yyjson::yyjson_is_obj(attr))
				continue;
			auto key = GetStringValue(attr, "key");
			auto val = duckdb_yyjson::yyjson_obj_get(attr, "value");
			string sval = JSONAnyToString(val);
			keys.emplace_back(Value(key));
			vals.emplace_back(Value(sval));
		}
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, vals);
}

bool OTLPJSONParser::ParseTracesToTypedRows(const string &json, vector<vector<Value>> &rows) {
	last_error = "";

	auto doc = duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		last_error = "Failed to parse JSON";
		return false;
	}

	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	auto resource_spans_array = duckdb_yyjson::yyjson_obj_get(root, "resourceSpans");

	if (!resource_spans_array || !duckdb_yyjson::yyjson_is_arr(resource_spans_array)) {
		duckdb_yyjson::yyjson_doc_free(doc);
		last_error = "Missing resourceSpans array";
		return false;
	}

	// Iterate through resource spans
	size_t idx1, max1;
	duckdb_yyjson::yyjson_val *resource_span;
	yyjson_arr_foreach(resource_spans_array, idx1, max1, resource_span) {
		auto resource = duckdb_yyjson::yyjson_obj_get(resource_span, "resource");

		// Extract service name and resource attributes
		string service_name = "unknown_service";
		Value resource_attributes_map = Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, {}, {});
		if (resource) {
			auto attrs = duckdb_yyjson::yyjson_obj_get(resource, "attributes");
			resource_attributes_map = JSONAttributesToMap(attrs);
			if (attrs && duckdb_yyjson::yyjson_is_arr(attrs)) {
				size_t attr_idx, attr_max;
				duckdb_yyjson::yyjson_val *attr;
				yyjson_arr_foreach(attrs, attr_idx, attr_max, attr) {
					auto key = GetStringValue(attr, "key");
					if (key == "service.name") {
						auto value = duckdb_yyjson::yyjson_obj_get(attr, "value");
						if (value) {
							service_name = JSONAnyToString(value);
						}
						break;
					}
				}
			}
		}

		// Iterate through scope spans
		auto scope_spans_array = duckdb_yyjson::yyjson_obj_get(resource_span, "scopeSpans");
		if (!scope_spans_array)
			continue;

		size_t idx2, max2;
		duckdb_yyjson::yyjson_val *scope_span;
		yyjson_arr_foreach(scope_spans_array, idx2, max2, scope_span) {
			auto scope = duckdb_yyjson::yyjson_obj_get(scope_span, "scope");
			string scope_name = GetStringValue(scope, "name");
			string scope_version = GetStringValue(scope, "version");

			// Iterate through spans
			auto spans_array = duckdb_yyjson::yyjson_obj_get(scope_span, "spans");
			if (!spans_array)
				continue;

			size_t idx3, max3;
			duckdb_yyjson::yyjson_val *span;
			yyjson_arr_foreach(spans_array, idx3, max3, span) {
				auto start_time = GetUint64Value(span, "startTimeUnixNano");
				auto end_time = GetUint64Value(span, "endTimeUnixNano");
				auto ts = NanosToTimestamp(start_time);
				int64_t duration = (int64_t)(end_time - start_time);

				auto status = duckdb_yyjson::yyjson_obj_get(span, "status");
				string status_code = GetStringValue(status, "code");
				if (status_code.empty())
					status_code = "UNSET";

				vector<Value> ev_ts;
				vector<Value> ev_names;
				vector<Value> ev_attrs;
				auto events_arr = duckdb_yyjson::yyjson_obj_get(span, "events");
				if (events_arr && duckdb_yyjson::yyjson_is_arr(events_arr)) {
					size_t eidx, emax;
					duckdb_yyjson::yyjson_val *event;
					yyjson_arr_foreach(events_arr, eidx, emax, event) {
						auto et = GetUint64Value(event, "timeUnixNano");
						ev_ts.emplace_back(Value::TIMESTAMPNS(NanosToTimestamp(et)));
						ev_names.emplace_back(Value(GetStringValue(event, "name")));
						ev_attrs.emplace_back(JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(event, "attributes")));
					}
				}

				// Links
				vector<Value> l_trace_ids, l_span_ids, l_trace_states, l_link_attrs;
				auto links_arr = duckdb_yyjson::yyjson_obj_get(span, "links");
				if (links_arr && duckdb_yyjson::yyjson_is_arr(links_arr)) {
					size_t lidx, lmax;
					duckdb_yyjson::yyjson_val *link;
					yyjson_arr_foreach(links_arr, lidx, lmax, link) {
						l_trace_ids.emplace_back(Value(BytesToHex(GetStringValue(link, "traceId"))));
						l_span_ids.emplace_back(Value(BytesToHex(GetStringValue(link, "spanId"))));
						l_trace_states.emplace_back(Value(GetStringValue(link, "traceState")));
						l_link_attrs.emplace_back(
						    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(link, "attributes")));
					}
				}

				auto kind_str = GetStringValue(span, "kind");
				TracesRowData d {ts,
				                 BytesToHex(GetStringValue(span, "traceId")),
				                 BytesToHex(GetStringValue(span, "spanId")),
				                 BytesToHex(GetStringValue(span, "parentSpanId")),
				                 GetStringValue(span, "traceState"),
				                 GetStringValue(span, "name"),
				                 (kind_str.empty() ? string("UNSPECIFIED") : kind_str),
				                 service_name,
				                 resource_attributes_map,
				                 scope_name,
				                 scope_version,
				                 JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(span, "attributes")),
				                 duration,
				                 status_code,
				                 GetStringValue(status, "message"),
				                 std::move(ev_ts),
				                 std::move(ev_names),
				                 std::move(ev_attrs),
				                 std::move(l_trace_ids),
				                 std::move(l_span_ids),
				                 std::move(l_trace_states),
				                 std::move(l_link_attrs)};
				rows.push_back(BuildTracesRow(d));
			}
		}
	}

	duckdb_yyjson::yyjson_doc_free(doc);
	return rows.size() > 0;
}

bool OTLPJSONParser::ParseLogsToTypedRows(const string &json, vector<vector<Value>> &rows) {
	last_error = "";

	auto doc = duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		last_error = "Failed to parse JSON";
		return false;
	}

	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	auto resource_logs_array = duckdb_yyjson::yyjson_obj_get(root, "resourceLogs");

	if (!resource_logs_array || !duckdb_yyjson::yyjson_is_arr(resource_logs_array)) {
		duckdb_yyjson::yyjson_doc_free(doc);
		last_error = "Missing resourceLogs array";
		return false;
	}

	// Iterate through resource logs
	size_t idx1, max1;
	duckdb_yyjson::yyjson_val *resource_log;
	yyjson_arr_foreach(resource_logs_array, idx1, max1, resource_log) {
		auto resource = duckdb_yyjson::yyjson_obj_get(resource_log, "resource");
		string resource_schema_url = GetStringValue(resource_log, "schemaUrl");

		// Extract service name
		string service_name = "unknown_service";
		if (resource) {
			auto attrs = duckdb_yyjson::yyjson_obj_get(resource, "attributes");
			if (attrs && duckdb_yyjson::yyjson_is_arr(attrs)) {
				size_t attr_idx, attr_max;
				duckdb_yyjson::yyjson_val *attr;
				yyjson_arr_foreach(attrs, attr_idx, attr_max, attr) {
					auto key = GetStringValue(attr, "key");
					if (key == "service.name") {
						auto value = duckdb_yyjson::yyjson_obj_get(attr, "value");
						if (value) {
							service_name = GetStringValue(value, "stringValue");
						}
						break;
					}
				}
			}
		}

		// Iterate through scope logs
		auto scope_logs_array = duckdb_yyjson::yyjson_obj_get(resource_log, "scopeLogs");
		if (!scope_logs_array)
			continue;

		size_t idx2, max2;
		duckdb_yyjson::yyjson_val *scope_log;
		yyjson_arr_foreach(scope_logs_array, idx2, max2, scope_log) {
			auto scope = duckdb_yyjson::yyjson_obj_get(scope_log, "scope");
			string scope_name = GetStringValue(scope, "name");
			string scope_version = GetStringValue(scope, "version");
			string scope_schema_url = GetStringValue(scope_log, "schemaUrl");

			// Iterate through log records
			auto log_records_array = duckdb_yyjson::yyjson_obj_get(scope_log, "logRecords");
			if (!log_records_array)
				continue;

			size_t idx3, max3;
			duckdb_yyjson::yyjson_val *log_record;
			yyjson_arr_foreach(log_records_array, idx3, max3, log_record) {
				auto ts = NanosToTimestamp(GetUint64Value(log_record, "timeUnixNano"));
				auto body = duckdb_yyjson::yyjson_obj_get(log_record, "body");
				string body_str = body ? GetStringValue(body, "stringValue") : "";
				LogsRowData d {
				    ts,
				    BytesToHex(GetStringValue(log_record, "traceId")),
				    BytesToHex(GetStringValue(log_record, "spanId")),
				    GetUintValue(log_record, "flags"),
				    GetStringValue(log_record, "severityText"),
				    GetIntValue(log_record, "severityNumber"),
				    service_name,
				    body_str,
				    resource_schema_url,
				    JSONAttributesToMap(resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes") : nullptr),
				    scope_schema_url,
				    scope_name,
				    scope_version,
				    JSONAttributesToMap(scope ? duckdb_yyjson::yyjson_obj_get(scope, "attributes") : nullptr),
				    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(log_record, "attributes"))};
				rows.push_back(BuildLogsRow(d));
			}
		}
	}

	duckdb_yyjson::yyjson_doc_free(doc);
	return rows.size() > 0;
}

bool OTLPJSONParser::ParseMetricsToTypedRows(const string &json, vector<vector<Value>> &rows) {
	last_error = "";

	auto doc = duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		last_error = "Failed to parse JSON";
		return false;
	}

	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	auto resource_metrics_array = duckdb_yyjson::yyjson_obj_get(root, "resourceMetrics");

	if (!resource_metrics_array || !duckdb_yyjson::yyjson_is_arr(resource_metrics_array)) {
		duckdb_yyjson::yyjson_doc_free(doc);
		last_error = "Missing resourceMetrics array";
		return false;
	}

	// Iterate through resource metrics
	size_t idx1, max1;
	duckdb_yyjson::yyjson_val *resource_metric;
	yyjson_arr_foreach(resource_metrics_array, idx1, max1, resource_metric) {
		auto resource = duckdb_yyjson::yyjson_obj_get(resource_metric, "resource");

		// Extract service name
		string service_name = "unknown_service";
		if (resource) {
			auto attrs = duckdb_yyjson::yyjson_obj_get(resource, "attributes");
			if (attrs && duckdb_yyjson::yyjson_is_arr(attrs)) {
				size_t attr_idx, attr_max;
				duckdb_yyjson::yyjson_val *attr;
				yyjson_arr_foreach(attrs, attr_idx, attr_max, attr) {
					auto key = GetStringValue(attr, "key");
					if (key == "service.name") {
						auto value = duckdb_yyjson::yyjson_obj_get(attr, "value");
						if (value) {
							service_name = GetStringValue(value, "stringValue");
						}
						break;
					}
				}
			}
		}

		// Iterate through scope metrics
		auto scope_metrics_array = duckdb_yyjson::yyjson_obj_get(resource_metric, "scopeMetrics");
		if (!scope_metrics_array)
			continue;

		size_t idx2, max2;
		duckdb_yyjson::yyjson_val *scope_metric;
		yyjson_arr_foreach(scope_metrics_array, idx2, max2, scope_metric) {
			auto scope = duckdb_yyjson::yyjson_obj_get(scope_metric, "scope");
			string scope_name = GetStringValue(scope, "name");
			string scope_version = GetStringValue(scope, "version");

			// Iterate through metrics
			auto metrics_array = duckdb_yyjson::yyjson_obj_get(scope_metric, "metrics");
			if (!metrics_array)
				continue;

			size_t idx3, max3;
			duckdb_yyjson::yyjson_val *metric;
			yyjson_arr_foreach(metrics_array, idx3, max3, metric) {
				string metric_name = GetStringValue(metric, "name");
				string metric_description = GetStringValue(metric, "description");
				string metric_unit = GetStringValue(metric, "unit");

				// Check for gauge
				auto gauge = duckdb_yyjson::yyjson_obj_get(metric, "gauge");
				if (gauge) {
					auto data_points = duckdb_yyjson::yyjson_obj_get(gauge, "dataPoints");
					if (data_points && duckdb_yyjson::yyjson_is_arr(data_points)) {
						size_t dp_idx, dp_max;
						duckdb_yyjson::yyjson_val *data_point;
						yyjson_arr_foreach(data_points, dp_idx, dp_max, data_point) {
							vector<Value> row(OTLPMetricsUnionSchema::COLUMN_COUNT);

							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							// Base columns (9)
							row[OTLPMetricsBaseSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
							row[OTLPMetricsBaseSchema::COL_SERVICE_NAME] = Value(service_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_NAME] = Value(metric_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
							row[OTLPMetricsBaseSchema::COL_METRIC_UNIT] = Value(metric_unit);
							// Resource and datapoint attributes
							row[OTLPMetricsBaseSchema::COL_RESOURCE_ATTRIBUTES] = JSONAttributesToMap(
							    resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes") : nullptr);
							row[OTLPMetricsBaseSchema::COL_SCOPE_NAME] = Value(scope_name);
							row[OTLPMetricsBaseSchema::COL_SCOPE_VERSION] = Value(scope_version);
							row[OTLPMetricsBaseSchema::COL_ATTRIBUTES] =
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes"));

							// Union discriminator
							row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("gauge");

							// Gauge value - check field existence first (not value == 0.0, as 0.0 is valid)
							double value = 0.0;
							auto as_double_field = duckdb_yyjson::yyjson_obj_get(data_point, "asDouble");
							if (as_double_field) {
								value = GetDoubleValue(data_point, "asDouble");
							} else {
								value = (double)GetUint64Value(data_point, "asInt");
							}
							row[OTLPMetricsUnionSchema::COL_VALUE] = Value::DOUBLE(value);

							// NULL all other union columns
							for (idx_t i = OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY;
							     i < OTLPMetricsUnionSchema::COLUMN_COUNT; i++) {
								if (i == OTLPMetricsUnionSchema::COL_VALUE)
									continue;
								if (i == OTLPMetricsUnionSchema::COL_IS_MONOTONIC) {
									row[i] = Value(LogicalType::BOOLEAN);
								} else if (i == OTLPMetricsUnionSchema::COL_COUNT ||
								           i == OTLPMetricsUnionSchema::COL_ZERO_COUNT) {
									row[i] = Value(LogicalType::UBIGINT);
								} else if (i == OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY ||
								           i == OTLPMetricsUnionSchema::COL_SCALE ||
								           i == OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET ||
								           i == OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET) {
									row[i] = Value(LogicalType::INTEGER);
								} else if (i == OTLPMetricsUnionSchema::COL_SUM ||
								           i == OTLPMetricsUnionSchema::COL_MIN ||
								           i == OTLPMetricsUnionSchema::COL_MAX) {
									row[i] = Value(LogicalType::DOUBLE);
								} else {
									row[i] = Value(LogicalType::LIST(LogicalType::UBIGINT));
								}
							}

							rows.push_back(std::move(row));
						}
					}
				}

				// Check for sum
				auto sum = duckdb_yyjson::yyjson_obj_get(metric, "sum");
				if (sum) {
					auto data_points = duckdb_yyjson::yyjson_obj_get(sum, "dataPoints");
					if (data_points && duckdb_yyjson::yyjson_is_arr(data_points)) {
						size_t dp_idx, dp_max;
						duckdb_yyjson::yyjson_val *data_point;
						yyjson_arr_foreach(data_points, dp_idx, dp_max, data_point) {
							vector<Value> row(OTLPMetricsUnionSchema::COLUMN_COUNT);

							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							// Base columns
							row[OTLPMetricsBaseSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
							row[OTLPMetricsBaseSchema::COL_SERVICE_NAME] = Value(service_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_NAME] = Value(metric_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
							row[OTLPMetricsBaseSchema::COL_METRIC_UNIT] = Value(metric_unit);
							row[OTLPMetricsBaseSchema::COL_RESOURCE_ATTRIBUTES] = JSONAttributesToMap(
							    resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes") : nullptr);
							row[OTLPMetricsBaseSchema::COL_SCOPE_NAME] = Value(scope_name);
							row[OTLPMetricsBaseSchema::COL_SCOPE_VERSION] = Value(scope_version);
							row[OTLPMetricsBaseSchema::COL_ATTRIBUTES] =
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes"));

							// Union discriminator
							row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("sum");

							// Sum value - check field existence first (not value == 0.0, as 0.0 is valid)
							double value = 0.0;
							auto as_double_field = duckdb_yyjson::yyjson_obj_get(data_point, "asDouble");
							if (as_double_field) {
								value = GetDoubleValue(data_point, "asDouble");
							} else {
								value = (double)GetUint64Value(data_point, "asInt");
							}
							row[OTLPMetricsUnionSchema::COL_VALUE] = Value::DOUBLE(value);

							// Sum-specific fields
							row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] =
							    Value::INTEGER(GetIntValue(sum, "aggregationTemporality"));
							row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value::BOOLEAN(
							    duckdb_yyjson::yyjson_obj_get(sum, "isMonotonic") &&
							    duckdb_yyjson::yyjson_get_bool(duckdb_yyjson::yyjson_obj_get(sum, "isMonotonic")));

							// NULL all other union columns
							row[OTLPMetricsUnionSchema::COL_COUNT] = Value(LogicalType::UBIGINT);
							row[OTLPMetricsUnionSchema::COL_SUM] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));
							row[OTLPMetricsUnionSchema::COL_MIN] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_MAX] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_SCALE] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value(LogicalType::UBIGINT);
							row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));
							row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));

							rows.push_back(std::move(row));
						}
					}
				}

				// Check for histogram
				auto histogram = duckdb_yyjson::yyjson_obj_get(metric, "histogram");
				if (histogram) {
					auto data_points = duckdb_yyjson::yyjson_obj_get(histogram, "dataPoints");
					if (data_points && duckdb_yyjson::yyjson_is_arr(data_points)) {
						size_t dp_idx, dp_max;
						duckdb_yyjson::yyjson_val *data_point;
						yyjson_arr_foreach(data_points, dp_idx, dp_max, data_point) {
							vector<Value> row(OTLPMetricsUnionSchema::COLUMN_COUNT);

							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							// Base columns
							row[OTLPMetricsBaseSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
							row[OTLPMetricsBaseSchema::COL_SERVICE_NAME] = Value(service_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_NAME] = Value(metric_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
							row[OTLPMetricsBaseSchema::COL_METRIC_UNIT] = Value(metric_unit);
							row[OTLPMetricsBaseSchema::COL_RESOURCE_ATTRIBUTES] = JSONAttributesToMap(
							    resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes") : nullptr);
							row[OTLPMetricsBaseSchema::COL_SCOPE_NAME] = Value(scope_name);
							row[OTLPMetricsBaseSchema::COL_SCOPE_VERSION] = Value(scope_version);
							row[OTLPMetricsBaseSchema::COL_ATTRIBUTES] =
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes"));

							// Union discriminator
							row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("histogram");

							// Histogram fields
							row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] =
							    Value::INTEGER(GetIntValue(histogram, "aggregationTemporality"));
							row[OTLPMetricsUnionSchema::COL_COUNT] =
							    Value::UBIGINT(GetUint64Value(data_point, "count"));
							row[OTLPMetricsUnionSchema::COL_SUM] = Value::DOUBLE(GetDoubleValue(data_point, "sum"));
							row[OTLPMetricsUnionSchema::COL_MIN] = Value::DOUBLE(GetDoubleValue(data_point, "min"));
							row[OTLPMetricsUnionSchema::COL_MAX] = Value::DOUBLE(GetDoubleValue(data_point, "max"));

							// Bucket counts and explicit bounds - NULL LISTs for now
							row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));

							// NULL other union columns
							row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
							row[OTLPMetricsUnionSchema::COL_SCALE] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value(LogicalType::UBIGINT);
							row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));
							row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));

							rows.push_back(std::move(row));
						}
					}
				}

				// Check for exponential histogram
				auto exp_histogram = duckdb_yyjson::yyjson_obj_get(metric, "exponentialHistogram");
				if (exp_histogram) {
					auto data_points = duckdb_yyjson::yyjson_obj_get(exp_histogram, "dataPoints");
					if (data_points && duckdb_yyjson::yyjson_is_arr(data_points)) {
						size_t dp_idx, dp_max;
						duckdb_yyjson::yyjson_val *data_point;
						yyjson_arr_foreach(data_points, dp_idx, dp_max, data_point) {
							vector<Value> row(OTLPMetricsUnionSchema::COLUMN_COUNT);

							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							// Base columns
							row[OTLPMetricsBaseSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
							row[OTLPMetricsBaseSchema::COL_SERVICE_NAME] = Value(service_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_NAME] = Value(metric_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
							row[OTLPMetricsBaseSchema::COL_METRIC_UNIT] = Value(metric_unit);
							row[OTLPMetricsBaseSchema::COL_RESOURCE_ATTRIBUTES] = JSONAttributesToMap(
							    resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes") : nullptr);
							row[OTLPMetricsBaseSchema::COL_SCOPE_NAME] = Value(scope_name);
							row[OTLPMetricsBaseSchema::COL_SCOPE_VERSION] = Value(scope_version);
							row[OTLPMetricsBaseSchema::COL_ATTRIBUTES] =
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes"));

							// Union discriminator
							row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("exponential_histogram");

							// Exponential histogram fields
							row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] =
							    Value::INTEGER(GetIntValue(exp_histogram, "aggregationTemporality"));
							row[OTLPMetricsUnionSchema::COL_COUNT] =
							    Value::UBIGINT(GetUint64Value(data_point, "count"));
							row[OTLPMetricsUnionSchema::COL_SUM] = Value::DOUBLE(GetDoubleValue(data_point, "sum"));
							row[OTLPMetricsUnionSchema::COL_SCALE] = Value::INTEGER(GetIntValue(data_point, "scale"));
							row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] =
							    Value::UBIGINT(GetUint64Value(data_point, "zeroCount"));
							row[OTLPMetricsUnionSchema::COL_MIN] = Value::DOUBLE(GetDoubleValue(data_point, "min"));
							row[OTLPMetricsUnionSchema::COL_MAX] = Value::DOUBLE(GetDoubleValue(data_point, "max"));

							// Positive/negative bucket info - NULL LISTs for now
							row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value::INTEGER(0);
							row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value::INTEGER(0);
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));

							// NULL other union columns
							row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
							row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));
							row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));
							row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));

							rows.push_back(std::move(row));
						}
					}
				}

				// Check for summary
				auto summary = duckdb_yyjson::yyjson_obj_get(metric, "summary");
				if (summary) {
					auto data_points = duckdb_yyjson::yyjson_obj_get(summary, "dataPoints");
					if (data_points && duckdb_yyjson::yyjson_is_arr(data_points)) {
						size_t dp_idx, dp_max;
						duckdb_yyjson::yyjson_val *data_point;
						yyjson_arr_foreach(data_points, dp_idx, dp_max, data_point) {
							vector<Value> row(OTLPMetricsUnionSchema::COLUMN_COUNT);

							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							// Base columns
							row[OTLPMetricsBaseSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
							row[OTLPMetricsBaseSchema::COL_SERVICE_NAME] = Value(service_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_NAME] = Value(metric_name);
							row[OTLPMetricsBaseSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
							row[OTLPMetricsBaseSchema::COL_METRIC_UNIT] = Value(metric_unit);
							row[OTLPMetricsBaseSchema::COL_RESOURCE_ATTRIBUTES] =
							    Value(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
							row[OTLPMetricsBaseSchema::COL_SCOPE_NAME] = Value(scope_name);
							row[OTLPMetricsBaseSchema::COL_SCOPE_VERSION] = Value(scope_version);
							row[OTLPMetricsBaseSchema::COL_ATTRIBUTES] =
							    Value(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

							// Union discriminator
							row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("summary");

							// Summary fields
							row[OTLPMetricsUnionSchema::COL_COUNT] =
							    Value::UBIGINT(GetUint64Value(data_point, "count"));
							row[OTLPMetricsUnionSchema::COL_SUM] = Value::DOUBLE(GetDoubleValue(data_point, "sum"));

							// Quantile values - NULL LISTs for now
							row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));
							row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));

							// NULL other union columns
							row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
							row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
							    Value(LogicalType::LIST(LogicalType::DOUBLE));
							row[OTLPMetricsUnionSchema::COL_MIN] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_MAX] = Value(LogicalType::DOUBLE);
							row[OTLPMetricsUnionSchema::COL_SCALE] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value(LogicalType::UBIGINT);
							row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value(LogicalType::INTEGER);
							row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] =
							    Value(LogicalType::LIST(LogicalType::UBIGINT));

							rows.push_back(std::move(row));
						}
					}
				}
			}
		}
	}

	duckdb_yyjson::yyjson_doc_free(doc);
	if (rows.size() == 0) {
		last_error = "No metrics data points found in JSON";
	}
	return rows.size() > 0;
}

} // namespace duckdb
