#pragma once

#include "duckdb.hpp"

// Generated protobuf stubs
#include "opentelemetry/proto/common/v1/common.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"

#include <google/protobuf/repeated_ptr_field.h>

namespace duckdb {

//! Shared helper functions for parsing OTLP protobuf data
//! Used by both otlp_receiver.cpp (gRPC) and protobuf_parser.cpp (file reading)

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
				out += '"' + subkv.key() + '"';
				out += ":";
				out += '"' + AnyValueToString(subkv.value()) + '"';
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
				out += '"' + AnyValueToString(elem) + '"';
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

//! Helper: Convert bytes to hex string
inline string BytesToHex(const string &bytes) {
	// Check if already hex-encoded (all chars are [0-9A-Fa-f] and even length)
	// JSON formats often provide trace IDs as hex strings already
	if (bytes.size() % 2 == 0 && !bytes.empty()) {
		bool is_hex = true;
		for (char c : bytes) {
			if (!std::isxdigit(static_cast<unsigned char>(c))) {
				is_hex = false;
				break;
			}
		}
		if (is_hex) {
			// Normalize to lowercase for consistency
			string normalized = bytes;
			for (auto &c : normalized)
				c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
			return normalized; // Already hex, normalized
		}
	}

	// Convert binary to hex
	static const char hex_chars[] = "0123456789abcdef";
	string result;
	result.reserve(bytes.size() * 2);
	for (unsigned char c : bytes) {
		result.push_back(hex_chars[c >> 4]);
		result.push_back(hex_chars[c & 0x0F]);
	}
	return result;
}

//! Helper: Convert Unix nanoseconds to DuckDB nanoseconds timestamp
inline timestamp_ns_t NanosToTimestamp(uint64_t nanos) {
	// DuckDB TIMESTAMP_NS is in nanoseconds, keep as-is
	// Check for overflow: timestamp_ns_t is int64_t, max value is INT64_MAX
	constexpr uint64_t MAX_TIMESTAMP_NS = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
	if (nanos > MAX_TIMESTAMP_NS) {
		// Clamp to maximum valid timestamp to avoid undefined behavior
		return timestamp_ns_t(std::numeric_limits<int64_t>::max());
	}
	return timestamp_ns_t(static_cast<int64_t>(nanos));
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
			out += '"' + subkv.key() + '"';
			out += ":";
			out += '"' + AnyValueToString(subkv.value()) + '"';
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
			out += '"' + AnyValueToString(elem) + '"';
		}
		out += "]";
		return out;
	}
	return "";
}

} // namespace duckdb
