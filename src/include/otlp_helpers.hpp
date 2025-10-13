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

//! Helper: Convert OTLP KeyValue attributes to DuckDB MAP
inline Value ConvertAttributesToMap(
    const google::protobuf::RepeatedPtrField<opentelemetry::proto::common::v1::KeyValue> &attributes) {
	// Create empty MAP for now - will populate in future iteration
	// TODO: Actually populate the MAP with key-value pairs
	return Value(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
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
			return bytes; // Already hex, don't double-encode
		}
	}

	// Convert binary to hex
	static const char hex_chars[] = "0123456789ABCDEF";
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

} // namespace duckdb
