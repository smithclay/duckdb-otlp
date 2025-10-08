#include "protobuf_parser.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"

// OpenTelemetry protobuf includes
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "opentelemetry/proto/metrics/v1/metrics.pb.h"
#include "opentelemetry/proto/logs/v1/logs.pb.h"
#include "opentelemetry/proto/common/v1/common.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"

#include <google/protobuf/util/json_util.h>

namespace duckdb {

OTLPProtobufParser::OTLPProtobufParser() : last_error("") {
}

OTLPProtobufParser::~OTLPProtobufParser() {
}

string OTLPProtobufParser::MessageToJSON(const google::protobuf::Message &msg) {
	string json_output;
	google::protobuf::util::JsonPrintOptions options;
	options.add_whitespace = false;
	options.preserve_proto_field_names = true;
	options.always_print_fields_with_no_presence = false;  // Don't print default values

	auto status = google::protobuf::util::MessageToJsonString(msg, &json_output, options);
	if (!status.ok()) {
		// Log error for debugging but return empty JSON
		last_error = "JSON conversion failed: " + string(status.message());
		return "{}";
	}
	return json_output;
}

idx_t OTLPProtobufParser::ParseTracesData(const char *data, size_t length, vector<timestamp_t> &timestamps,
                                           vector<string> &resources, vector<string> &datas) {
	last_error = "";

	opentelemetry::proto::trace::v1::TracesData traces_data;
	if (!traces_data.ParseFromArray(data, length)) {
		last_error = "Failed to parse TracesData protobuf";
		return 0;
	}

	// Convert entire TracesData to JSON once (instead of per-span)
	string traces_data_json = MessageToJSON(traces_data);

	idx_t row_count = 0;

	// Iterate through resource spans
	for (const auto &resource_span : traces_data.resource_spans()) {
		// Convert resource to JSON
		string resource_json = "{}";
		if (resource_span.has_resource()) {
			resource_json = MessageToJSON(resource_span.resource());
		}

		// Iterate through scope spans
		for (const auto &scope_span : resource_span.scope_spans()) {
			// Iterate through spans
			for (const auto &span : scope_span.spans()) {
				// Extract timestamp from startTimeUnixNano
				// Convert from nanoseconds to microseconds
				int64_t start_time_nanos = span.start_time_unix_nano();
				int64_t start_time_micros = start_time_nanos / 1000;

				timestamp_t ts;
				ts.value = start_time_micros;

				timestamps.push_back(ts);
				resources.push_back(resource_json);

				// Use pre-converted JSON data
				datas.push_back(traces_data_json);

				row_count++;
			}
		}
	}

	return row_count;
}

idx_t OTLPProtobufParser::ParseMetricsData(const char *data, size_t length, vector<timestamp_t> &timestamps,
                                            vector<string> &resources, vector<string> &datas) {
	last_error = "";

	opentelemetry::proto::metrics::v1::MetricsData metrics_data;
	if (!metrics_data.ParseFromArray(data, length)) {
		last_error = "Failed to parse MetricsData protobuf";
		return 0;
	}

	// Convert entire MetricsData to JSON once
	string metrics_data_json = MessageToJSON(metrics_data);

	idx_t row_count = 0;

	// Iterate through resource metrics
	for (const auto &resource_metric : metrics_data.resource_metrics()) {
		// Convert resource to JSON
		string resource_json = "{}";
		if (resource_metric.has_resource()) {
			resource_json = MessageToJSON(resource_metric.resource());
		}

		// Iterate through scope metrics
		for (const auto &scope_metric : resource_metric.scope_metrics()) {
			// Iterate through metrics
			for (const auto &metric : scope_metric.metrics()) {
				// Extract timestamp - metrics have different structures
				// Use current timestamp for now (will improve in later iterations)
				timestamp_t ts = Timestamp::GetCurrentTimestamp();

				timestamps.push_back(ts);
				resources.push_back(resource_json);

				// Use pre-converted JSON data
				datas.push_back(metrics_data_json);

				row_count++;
			}
		}
	}

	return row_count;
}

idx_t OTLPProtobufParser::ParseLogsData(const char *data, size_t length, vector<timestamp_t> &timestamps,
                                         vector<string> &resources, vector<string> &datas) {
	last_error = "";

	opentelemetry::proto::logs::v1::LogsData logs_data;
	if (!logs_data.ParseFromArray(data, length)) {
		last_error = "Failed to parse LogsData protobuf";
		return 0;
	}

	// Convert entire LogsData to JSON once
	string logs_data_json = MessageToJSON(logs_data);

	idx_t row_count = 0;

	// Iterate through resource logs
	for (const auto &resource_log : logs_data.resource_logs()) {
		// Convert resource to JSON
		string resource_json = "{}";
		if (resource_log.has_resource()) {
			resource_json = MessageToJSON(resource_log.resource());
		}

		// Iterate through scope logs
		for (const auto &scope_log : resource_log.scope_logs()) {
			// Iterate through log records
			for (const auto &log_record : scope_log.log_records()) {
				// Extract timestamp from time_unix_nano
				int64_t time_nanos = log_record.time_unix_nano();
				int64_t time_micros = time_nanos / 1000;

				timestamp_t ts;
				ts.value = time_micros;

				timestamps.push_back(ts);
				resources.push_back(resource_json);

				// Use pre-converted JSON data
				datas.push_back(logs_data_json);

				row_count++;
			}
		}
	}

	return row_count;
}

string OTLPProtobufParser::GetLastError() const {
	return last_error;
}

} // namespace duckdb
