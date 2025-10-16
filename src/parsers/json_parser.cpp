#include "parsers/json_parser.hpp"
#include "receiver/otlp_helpers.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "schema/otlp_logs_schema.hpp"
#include "schema/otlp_metrics_union_schema.hpp"
#include "receiver/row_builders_traces_logs.hpp"
#include "receiver/row_builders_metrics.hpp"
#include "receiver/row_builders.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"
#include <utility>

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
	} else if (duckdb_yyjson::yyjson_is_int(val)) {
		auto signed_val = duckdb_yyjson::yyjson_get_int(val);
		if (signed_val < 0) {
			return 0;
		}
		return static_cast<uint64_t>(signed_val);
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

static bool TryParseInt64Value(duckdb_yyjson::yyjson_val *val, int64_t &out) {
	if (!val) {
		return false;
	}
	if (duckdb_yyjson::yyjson_is_int(val)) {
		out = duckdb_yyjson::yyjson_get_int(val);
		return true;
	}
	if (duckdb_yyjson::yyjson_is_uint(val)) {
		auto raw = duckdb_yyjson::yyjson_get_uint(val);
		if (raw > NumericLimits<int64_t>::Maximum()) {
			return false;
		}
		out = static_cast<int64_t>(raw);
		return true;
	}
	if (duckdb_yyjson::yyjson_is_str(val)) {
		auto str_ptr = duckdb_yyjson::yyjson_get_str(val);
		if (!str_ptr) {
			return false;
		}
		try {
			auto text = string(str_ptr);
			StringUtil::Trim(text);
			if (text.empty()) {
				return false;
			}
			out = std::stoll(text);
			return true;
		} catch (const std::exception &) {
			return false;
		}
	}
	return false;
}

static bool TryParseDoubleValue(duckdb_yyjson::yyjson_val *val, double &out) {
	if (!val) {
		return false;
	}
	if (duckdb_yyjson::yyjson_is_real(val)) {
		out = duckdb_yyjson::yyjson_get_real(val);
		return true;
	}
	if (duckdb_yyjson::yyjson_is_int(val)) {
		out = static_cast<double>(duckdb_yyjson::yyjson_get_int(val));
		return true;
	}
	if (duckdb_yyjson::yyjson_is_uint(val)) {
		out = static_cast<double>(duckdb_yyjson::yyjson_get_uint(val));
		return true;
	}
	if (duckdb_yyjson::yyjson_is_str(val)) {
		auto str_ptr = duckdb_yyjson::yyjson_get_str(val);
		if (!str_ptr) {
			return false;
		}
		try {
			auto text = string(str_ptr);
			StringUtil::Trim(text);
			if (text.empty()) {
				return false;
			}
			out = std::stod(text);
			return true;
		} catch (const std::exception &) {
			return false;
		}
	}
	return false;
}

static bool TryGetInt64Field(duckdb_yyjson::yyjson_val *obj, const char *key, int64_t &out) {
	if (!obj) {
		return false;
	}
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	return TryParseInt64Value(val, out);
}

static bool TryGetDoubleField(duckdb_yyjson::yyjson_val *obj, const char *key, double &out) {
	if (!obj) {
		return false;
	}
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	return TryParseDoubleValue(val, out);
}

static vector<Value> ParseUint64List(duckdb_yyjson::yyjson_val *arr) {
	vector<Value> result;
	if (!arr || !duckdb_yyjson::yyjson_is_arr(arr)) {
		return result;
	}
	size_t idx, max;
	duckdb_yyjson::yyjson_val *item;
	yyjson_arr_foreach(arr, idx, max, item) {
		int64_t parsed = 0;
		if (TryParseInt64Value(item, parsed) && parsed >= 0) {
			result.emplace_back(Value::UBIGINT(static_cast<uint64_t>(parsed)));
		}
	}
	return result;
}

static vector<Value> ParseDoubleList(duckdb_yyjson::yyjson_val *arr) {
	vector<Value> result;
	if (!arr || !duckdb_yyjson::yyjson_is_arr(arr)) {
		return result;
	}
	size_t idx, max;
	duckdb_yyjson::yyjson_val *item;
	yyjson_arr_foreach(arr, idx, max, item) {
		double parsed = 0;
		if (TryParseDoubleValue(item, parsed)) {
			result.emplace_back(Value::DOUBLE(parsed));
		}
	}
	return result;
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
							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							double value = 0.0;
							bool has_value = false;
							if (TryParseDoubleValue(duckdb_yyjson::yyjson_obj_get(data_point, "asDouble"), value)) {
								has_value = true;
							} else {
								int64_t int_value = 0;
								if (TryParseInt64Value(duckdb_yyjson::yyjson_obj_get(data_point, "asInt"), int_value)) {
									value = static_cast<double>(int_value);
									has_value = true;
								}
							}
							if (!has_value) {
								duckdb_yyjson::yyjson_doc_free(doc);
								last_error = "Gauge data point missing numeric value";
								return false;
							}

							MetricsGaugeData d {
							    timestamp,
							    service_name,
							    metric_name,
							    metric_description,
							    metric_unit,
							    JSONAttributesToMap(resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes")
							                                 : nullptr),
							    scope_name,
							    scope_version,
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes")),
							    value};

							rows.push_back(TransformGaugeRow(BuildMetricsGaugeRow(d)));
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
							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							double value = 0.0;
							bool has_value = false;
							if (TryParseDoubleValue(duckdb_yyjson::yyjson_obj_get(data_point, "asDouble"), value)) {
								has_value = true;
							} else {
								int64_t int_value = 0;
								if (TryParseInt64Value(duckdb_yyjson::yyjson_obj_get(data_point, "asInt"), int_value)) {
									value = static_cast<double>(int_value);
									has_value = true;
								}
							}
							if (!has_value) {
								duckdb_yyjson::yyjson_doc_free(doc);
								last_error = "Sum data point missing numeric value";
								return false;
							}

							int64_t agg_temp_raw = 0;
							int32_t agg_temp = 0;
							if (TryGetInt64Field(sum, "aggregationTemporality", agg_temp_raw)) {
								if (agg_temp_raw < NumericLimits<int32_t>::Minimum()) {
									agg_temp = NumericLimits<int32_t>::Minimum();
								} else if (agg_temp_raw > NumericLimits<int32_t>::Maximum()) {
									agg_temp = NumericLimits<int32_t>::Maximum();
								} else {
									agg_temp = static_cast<int32_t>(agg_temp_raw);
								}
							}
							bool is_monotonic =
							    duckdb_yyjson::yyjson_obj_get(sum, "isMonotonic") &&
							    duckdb_yyjson::yyjson_get_bool(duckdb_yyjson::yyjson_obj_get(sum, "isMonotonic"));

							// Build type-specific row using shared builder
							MetricsSumData d {
							    timestamp,
							    service_name,
							    metric_name,
							    metric_description,
							    metric_unit,
							    JSONAttributesToMap(resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes")
							                                 : nullptr),
							    scope_name,
							    scope_version,
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes")),
							    value,
							    agg_temp,
							    is_monotonic};

							// Transform to union schema for file reading
							rows.push_back(TransformSumRow(BuildMetricsSumRow(d)));
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
							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							auto bucket_counts =
							    ParseUint64List(duckdb_yyjson::yyjson_obj_get(data_point, "bucketCounts"));
							auto explicit_bounds =
							    ParseDoubleList(duckdb_yyjson::yyjson_obj_get(data_point, "explicitBounds"));

							MetricsHistogramData d {
							    timestamp,
							    service_name,
							    metric_name,
							    metric_description,
							    metric_unit,
							    JSONAttributesToMap(resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes")
							                                 : nullptr),
							    scope_name,
							    scope_version,
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes")),
							    GetUint64Value(data_point, "count"),
							    GetDoubleValue(data_point, "sum"),
							    std::move(bucket_counts),
							    std::move(explicit_bounds),
							    GetDoubleValue(data_point, "min"),
							    GetDoubleValue(data_point, "max")};

							// Transform to union schema for file reading
							rows.push_back(TransformHistogramRow(BuildMetricsHistogramRow(d)));
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
							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							int64_t scale_raw = 0;
							int32_t scale = 0;
							if (TryGetInt64Field(data_point, "scale", scale_raw)) {
								if (scale_raw < NumericLimits<int32_t>::Minimum()) {
									scale = NumericLimits<int32_t>::Minimum();
								} else if (scale_raw > NumericLimits<int32_t>::Maximum()) {
									scale = NumericLimits<int32_t>::Maximum();
								} else {
									scale = static_cast<int32_t>(scale_raw);
								}
							}

							auto positive = duckdb_yyjson::yyjson_obj_get(data_point, "positive");
							int32_t positive_offset = 0;
							if (positive && duckdb_yyjson::yyjson_is_obj(positive)) {
								int64_t offset64 = 0;
								if (TryGetInt64Field(positive, "offset", offset64)) {
									if (offset64 < NumericLimits<int32_t>::Minimum()) {
										positive_offset = NumericLimits<int32_t>::Minimum();
									} else if (offset64 > NumericLimits<int32_t>::Maximum()) {
										positive_offset = NumericLimits<int32_t>::Maximum();
									} else {
										positive_offset = static_cast<int32_t>(offset64);
									}
								}
							}
							auto positive_bucket_counts = ParseUint64List(
							    positive ? duckdb_yyjson::yyjson_obj_get(positive, "bucketCounts") : nullptr);

							auto negative = duckdb_yyjson::yyjson_obj_get(data_point, "negative");
							int32_t negative_offset = 0;
							if (negative && duckdb_yyjson::yyjson_is_obj(negative)) {
								int64_t offset64 = 0;
								if (TryGetInt64Field(negative, "offset", offset64)) {
									if (offset64 < NumericLimits<int32_t>::Minimum()) {
										negative_offset = NumericLimits<int32_t>::Minimum();
									} else if (offset64 > NumericLimits<int32_t>::Maximum()) {
										negative_offset = NumericLimits<int32_t>::Maximum();
									} else {
										negative_offset = static_cast<int32_t>(offset64);
									}
								}
							}
							auto negative_bucket_counts = ParseUint64List(
							    negative ? duckdb_yyjson::yyjson_obj_get(negative, "bucketCounts") : nullptr);

							MetricsExpHistogramData d {
							    timestamp,
							    service_name,
							    metric_name,
							    metric_description,
							    metric_unit,
							    JSONAttributesToMap(resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes")
							                                 : nullptr),
							    scope_name,
							    scope_version,
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes")),
							    GetUint64Value(data_point, "count"),
							    GetDoubleValue(data_point, "sum"),
							    scale,
							    GetUint64Value(data_point, "zeroCount"),
							    positive_offset,
							    std::move(positive_bucket_counts),
							    negative_offset,
							    std::move(negative_bucket_counts),
							    GetDoubleValue(data_point, "min"),
							    GetDoubleValue(data_point, "max")};

							// Transform to union schema for file reading
							rows.push_back(TransformExpHistogramRow(BuildMetricsExpHistogramRow(d)));
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
							auto timestamp = NanosToTimestamp(GetUint64Value(data_point, "timeUnixNano"));

							vector<Value> quantile_values;
							vector<Value> quantile_quantiles;
							auto quantile_arr = duckdb_yyjson::yyjson_obj_get(data_point, "quantileValues");
							if (quantile_arr && duckdb_yyjson::yyjson_is_arr(quantile_arr)) {
								size_t qidx, qmax;
								duckdb_yyjson::yyjson_val *quantile_entry;
								yyjson_arr_foreach(quantile_arr, qidx, qmax, quantile_entry) {
									double quantile = 0.0;
									double quantile_value = 0.0;
									if (TryGetDoubleField(quantile_entry, "quantile", quantile) &&
									    TryGetDoubleField(quantile_entry, "value", quantile_value)) {
										quantile_quantiles.emplace_back(Value::DOUBLE(quantile));
										quantile_values.emplace_back(Value::DOUBLE(quantile_value));
									}
								}
							}

							MetricsSummaryData d {
							    timestamp,
							    service_name,
							    metric_name,
							    metric_description,
							    metric_unit,
							    JSONAttributesToMap(resource ? duckdb_yyjson::yyjson_obj_get(resource, "attributes")
							                                 : nullptr),
							    scope_name,
							    scope_version,
							    JSONAttributesToMap(duckdb_yyjson::yyjson_obj_get(data_point, "attributes")),
							    GetUint64Value(data_point, "count"),
							    GetDoubleValue(data_point, "sum"),
							    std::move(quantile_values),
							    std::move(quantile_quantiles)};

							// Transform to union schema for file reading
							rows.push_back(TransformSummaryRow(BuildMetricsSummaryRow(d)));
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
