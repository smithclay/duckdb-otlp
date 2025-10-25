#pragma once

#include "duckdb.hpp"
#include "include/otlp_utils.hpp"

// Generated protobuf stubs
#include "opentelemetry/proto/common/v1/common.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"

#include <google/protobuf/repeated_ptr_field.h>

namespace duckdb {

//! Context structs to reduce parameter passing through nested processing

//! Resource-level context (service name, attributes)
struct ResourceContext {
	string service_name;
	Value resource_attrs;
};

//! Scope-level context (includes resource context + scope info)
struct ScopeContext {
	ResourceContext resource;
	string scope_name;
	string scope_version;
};

//! Metric-level context (includes scope context + metric metadata)
struct MetricContext {
	ScopeContext scope;
	string metric_name;
	string metric_description;
	string metric_unit;
};

//! Shared helper functions for parsing OTLP protobuf data used by the file readers

// Forward declaration used by ConvertAttributesToMap
inline string AnyValueToString(const opentelemetry::proto::common::v1::AnyValue &any_value);

//! Helper: Convert OTLP KeyValue attributes to DuckDB MAP
inline Value ConvertAttributesToMap(
    const google::protobuf::RepeatedPtrField<opentelemetry::proto::common::v1::KeyValue> &attributes) {
	// Represent attributes as MAP<VARCHAR, VARCHAR> by stringifying AnyValue
	// Note: This preserves information and avoids JSON extraction in SQL.
	// Complex nested/array values are JSON-serialized strings.
	vector<Value> keys;
	vector<Value> vals;
	keys.reserve(static_cast<size_t>(attributes.size()));
	vals.reserve(static_cast<size_t>(attributes.size()));

	for (const auto &kv : attributes) {
		const auto &key = kv.key();
		const auto &val = kv.value();
		string sval;
		if (val.has_string_value()) {
			sval = val.string_value();
		} else if (val.has_int_value()) {
			sval = std::to_string(val.int_value());
		} else if (val.has_double_value()) {
			sval = std::to_string(val.double_value());
		} else if (val.has_bool_value()) {
			sval = val.bool_value() ? "true" : "false";
		} else if (val.has_kvlist_value()) {
			// Serialize KV list to compact JSON string key1=...,key2=... style
			// For simplicity, use JSON-like rendering
			string out = "{";
			bool first = true;
			for (const auto &subkv : val.kvlist_value().values()) {
				if (!first)
					out += ",";
				first = false;
				out.push_back('"');
				out += EscapeJsonString(subkv.key());
				out += "\":\"";
				out += EscapeJsonString(AnyValueToString(subkv.value()));
				out.push_back('"');
			}
			out += "}";
			sval = std::move(out);
		} else if (val.has_array_value()) {
			// Serialize array to JSON-like [v1,v2]
			string out = "[";
			bool first = true;
			for (const auto &elem : val.array_value().values()) {
				if (!first)
					out += ",";
				first = false;
				out.push_back('"');
				out += EscapeJsonString(AnyValueToString(elem));
				out.push_back('"');
			}
			out += "]";
			sval = std::move(out);
		} else {
			sval = "";
		}

		keys.emplace_back(Value(key));
		vals.emplace_back(Value(sval));
	}

	// Build MAP from key and value lists
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, vals);
}

//! Helper: Extract service.name from resource attributes
inline string ExtractServiceName(const opentelemetry::proto::resource::v1::Resource &resource) {
	for (const auto &attr : resource.attributes()) {
		if (attr.key() == "service.name" && attr.value().has_string_value()) {
			return attr.value().string_value();
		}
	}
	return "unknown_service";
}

//! Helper: Convert span kind enum to string
inline string SpanKindToString(opentelemetry::proto::trace::v1::Span::SpanKind kind) {
	switch (kind) {
	case opentelemetry::proto::trace::v1::Span::SPAN_KIND_UNSPECIFIED:
		return "UNSPECIFIED";
	case opentelemetry::proto::trace::v1::Span::SPAN_KIND_INTERNAL:
		return "INTERNAL";
	case opentelemetry::proto::trace::v1::Span::SPAN_KIND_SERVER:
		return "SERVER";
	case opentelemetry::proto::trace::v1::Span::SPAN_KIND_CLIENT:
		return "CLIENT";
	case opentelemetry::proto::trace::v1::Span::SPAN_KIND_PRODUCER:
		return "PRODUCER";
	case opentelemetry::proto::trace::v1::Span::SPAN_KIND_CONSUMER:
		return "CONSUMER";
	default:
		return "UNSPECIFIED";
	}
}

//! Helper: Convert status code enum to string
inline string StatusCodeToString(opentelemetry::proto::trace::v1::Status::StatusCode code) {
	switch (code) {
	case opentelemetry::proto::trace::v1::Status::STATUS_CODE_UNSET:
		return "UNSET";
	case opentelemetry::proto::trace::v1::Status::STATUS_CODE_OK:
		return "OK";
	case opentelemetry::proto::trace::v1::Status::STATUS_CODE_ERROR:
		return "ERROR";
	default:
		return "UNSET";
	}
}

//! Helper: Convert AnyValue to string
inline string AnyValueToString(const opentelemetry::proto::common::v1::AnyValue &any_value) {
	if (any_value.has_string_value()) {
		return any_value.string_value();
	} else if (any_value.has_int_value()) {
		return std::to_string(any_value.int_value());
	} else if (any_value.has_double_value()) {
		return std::to_string(any_value.double_value());
	} else if (any_value.has_bool_value()) {
		return any_value.bool_value() ? "true" : "false";
	}
	return "";
}

//! Helper: Convert AnyValue to a JSON-like string (handles nested kvlist/array)
inline string AnyValueToJSONString(const opentelemetry::proto::common::v1::AnyValue &any_value) {
	if (any_value.has_string_value() || any_value.has_int_value() || any_value.has_double_value() ||
	    any_value.has_bool_value()) {
		return AnyValueToString(any_value);
	}
	if (any_value.has_kvlist_value()) {
		string out = "{";
		bool first = true;
		for (const auto &subkv : any_value.kvlist_value().values()) {
			if (!first)
				out += ",";
			first = false;
			out.push_back('"');
			out += EscapeJsonString(subkv.key());
			out += "\":\"";
			out += EscapeJsonString(AnyValueToString(subkv.value()));
			out.push_back('"');
		}
		out += "}";
		return out;
	}
	if (any_value.has_array_value()) {
		string out = "[";
		bool first = true;
		for (const auto &elem : any_value.array_value().values()) {
			if (!first)
				out += ",";
			first = false;
			out.push_back('"');
			out += EscapeJsonString(AnyValueToString(elem));
			out.push_back('"');
		}
		out += "]";
		return out;
	}
	return "";
}

//! Helper: Populate base metric fields shared by all metric types
//! Populates columns 0-7 (timestamp through scope_version) for all metric schemas
//! Caller must still populate COL_ATTRIBUTES (index 8) and type-specific fields
template <typename AppenderType>
inline void PopulateBaseMetricFields(AppenderType &app, timestamp_t timestamp, const MetricContext &ctx) {
	// All metric types share base columns 0-8
	app.SetTimestampNS(0, timestamp);                   // COL_TIMESTAMP
	app.SetVarchar(1, ctx.scope.resource.service_name); // COL_SERVICE_NAME
	app.SetVarchar(2, ctx.metric_name);                 // COL_METRIC_NAME
	app.SetVarchar(3, ctx.metric_description);          // COL_METRIC_DESCRIPTION
	app.SetVarchar(4, ctx.metric_unit);                 // COL_METRIC_UNIT
	app.SetValue(5, ctx.scope.resource.resource_attrs); // COL_RESOURCE_ATTRIBUTES
	app.SetVarchar(6, ctx.scope.scope_name);            // COL_SCOPE_NAME
	app.SetVarchar(7, ctx.scope.scope_version);         // COL_SCOPE_VERSION
	// Note: COL_ATTRIBUTES (index 8) must be set by caller with data-point-specific attributes
}

} // namespace duckdb
