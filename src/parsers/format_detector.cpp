#include "parsers/format_detector.hpp"
#include "duckdb/common/string_util.hpp"

// Try to parse as protobuf to detect signal type
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "opentelemetry/proto/metrics/v1/metrics.pb.h"
#include "opentelemetry/proto/logs/v1/logs.pb.h"

namespace duckdb {

OTLPFormat FormatDetector::DetectFormat(const char *data, size_t len) {
	if (len == 0) {
		return OTLPFormat::UNKNOWN;
	}

	// JSON detection: Look for opening brace or bracket
	// Skip whitespace
	size_t i = 0;
	while (i < len && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r')) {
		i++;
	}

	if (i < len) {
		char first_char = data[i];
		if (first_char == '{' || first_char == '[') {
			return OTLPFormat::JSON;
		}
	}

	// Protobuf detection: Binary data, typically starts with field tags
	// Protobuf messages start with field numbers and wire types
	// For OTLP, the first field is usually field 1 (resource_spans/resource_metrics/resource_logs)
	// which is a repeated message field (wire type 2)
	// First byte would be: (field_number << 3) | wire_type
	// Field 1, wire type 2: (1 << 3) | 2 = 0x0A

	if (len > 0) {
		// Check if first byte looks like a protobuf field tag
		unsigned char first_byte = static_cast<unsigned char>(data[0]);

		// Common OTLP protobuf patterns:
		// 0x0A = field 1, wire type 2 (length-delimited) - common for OTLP messages
		// 0x12 = field 2, wire type 2
		// Binary data typically has non-printable characters
		if (first_byte == 0x0A || first_byte == 0x12 ||
		    (first_byte < 0x20 && first_byte != '\n' && first_byte != '\r' && first_byte != '\t')) {
			return OTLPFormat::PROTOBUF;
		}
	}

	return OTLPFormat::UNKNOWN;
}

FormatDetector::SignalType FormatDetector::DetectProtobufSignalType(const char *data, size_t len) {
	// Try parsing as each type
	// ParseFromArray expects int, so we safely cast from size_t
	int size = static_cast<int>(len);

	opentelemetry::proto::trace::v1::TracesData traces_data;
	if (traces_data.ParseFromArray(data, size)) {
		return SignalType::TRACES;
	}

	opentelemetry::proto::metrics::v1::MetricsData metrics_data;
	if (metrics_data.ParseFromArray(data, size)) {
		return SignalType::METRICS;
	}

	opentelemetry::proto::logs::v1::LogsData logs_data;
	if (logs_data.ParseFromArray(data, size)) {
		return SignalType::LOGS;
	}

	return SignalType::UNKNOWN;
}

} // namespace duckdb
