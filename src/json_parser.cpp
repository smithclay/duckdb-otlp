#include "json_parser.hpp"
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

} // namespace duckdb
