#include "protobuf_parser.hpp"
#include "otlp_helpers.hpp"
#include "otlp_traces_schema.hpp"
#include "otlp_logs_schema.hpp"
#include "otlp_metrics_schemas.hpp"
#include "otlp_metrics_union_schema.hpp"
#include "otlp_types.hpp"
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
	options.always_print_fields_with_no_presence = false; // Don't print default values

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
	if (!traces_data.ParseFromArray(data, static_cast<int>(length))) {
		last_error = "Failed to parse TracesData protobuf";
		return 0;
	}

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
				// Extract timestamp from startTimeUnixNano (with rounding)
				timestamp_t ts = NanosToTimestamp(span.start_time_unix_nano());

				// Convert individual span to JSON (not entire TracesData)
				string span_json = MessageToJSON(span);

				timestamps.push_back(ts);
				resources.push_back(resource_json);
				datas.push_back(span_json);

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
	if (!metrics_data.ParseFromArray(data, static_cast<int>(length))) {
		last_error = "Failed to parse MetricsData protobuf";
		return 0;
	}

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

				// Convert individual metric to JSON (not entire MetricsData)
				string metric_json = MessageToJSON(metric);

				timestamps.push_back(ts);
				resources.push_back(resource_json);
				datas.push_back(metric_json);

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
	if (!logs_data.ParseFromArray(data, static_cast<int>(length))) {
		last_error = "Failed to parse LogsData protobuf";
		return 0;
	}

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
				// Extract timestamp from time_unix_nano (with rounding)
				timestamp_t ts = NanosToTimestamp(log_record.time_unix_nano());

				// Convert individual log record to JSON (not entire LogsData)
				string log_json = MessageToJSON(log_record);

				timestamps.push_back(ts);
				resources.push_back(resource_json);
				datas.push_back(log_json);

				row_count++;
			}
		}
	}

	return row_count;
}

string OTLPProtobufParser::GetLastError() const {
	return last_error;
}

//===--------------------------------------------------------------------===//
// V2 Schema: Typed Row Parsing Methods
//===--------------------------------------------------------------------===//

idx_t OTLPProtobufParser::ParseTracesToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	last_error = "";

	opentelemetry::proto::trace::v1::TracesData traces_data;
	if (!traces_data.ParseFromArray(data, static_cast<int>(length))) {
		last_error = "Failed to parse TracesData protobuf";
		return 0;
	}

	idx_t row_count = 0;

	// Iterate through resource spans (similar to otlp_receiver.cpp)
	for (const auto &resource_span : traces_data.resource_spans()) {
		const auto &resource = resource_span.resource();
		string service_name = ExtractServiceName(resource);
		Value resource_attrs = ConvertAttributesToMap(resource.attributes());

		// Iterate through scope spans
		for (const auto &scope_span : resource_span.scope_spans()) {
			const auto &scope = scope_span.scope();
			string scope_name = scope.name();
			string scope_version = scope.version();

			// Iterate through spans
			for (const auto &span : scope_span.spans()) {
				vector<Value> row(OTLPTracesSchema::COLUMN_COUNT);

				// Extract timestamp from span start time
				auto timestamp = NanosToTimestamp(span.start_time_unix_nano());
				auto duration = span.end_time_unix_nano() - span.start_time_unix_nano(); // nanoseconds

				// Populate row (22 columns)
				row[OTLPTracesSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
				row[OTLPTracesSchema::COL_TRACE_ID] = Value(BytesToHex(span.trace_id()));
				row[OTLPTracesSchema::COL_SPAN_ID] = Value(BytesToHex(span.span_id()));
				row[OTLPTracesSchema::COL_PARENT_SPAN_ID] = Value(BytesToHex(span.parent_span_id()));
				row[OTLPTracesSchema::COL_TRACE_STATE] = Value(span.trace_state());
				row[OTLPTracesSchema::COL_SPAN_NAME] = Value(span.name());
				row[OTLPTracesSchema::COL_SPAN_KIND] = Value(SpanKindToString(span.kind()));
				row[OTLPTracesSchema::COL_SERVICE_NAME] = Value(service_name);
				row[OTLPTracesSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
				row[OTLPTracesSchema::COL_SCOPE_NAME] = Value(scope_name);
				row[OTLPTracesSchema::COL_SCOPE_VERSION] = Value(scope_version);
				row[OTLPTracesSchema::COL_SPAN_ATTRIBUTES] = ConvertAttributesToMap(span.attributes());
				row[OTLPTracesSchema::COL_DURATION] = Value::BIGINT(duration);
				row[OTLPTracesSchema::COL_STATUS_CODE] = Value(StatusCodeToString(span.status().code()));
				row[OTLPTracesSchema::COL_STATUS_MESSAGE] = Value(span.status().message());

				// Events and Links - use NULL LISTs for now
				row[OTLPTracesSchema::COL_EVENTS_TIMESTAMP] = Value(LogicalType::LIST(LogicalType::TIMESTAMP_NS));
				row[OTLPTracesSchema::COL_EVENTS_NAME] = Value(LogicalType::LIST(LogicalType::VARCHAR));
				row[OTLPTracesSchema::COL_EVENTS_ATTRIBUTES] =
				    Value(LogicalType::LIST(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
				row[OTLPTracesSchema::COL_LINKS_TRACE_ID] = Value(LogicalType::LIST(LogicalType::VARCHAR));
				row[OTLPTracesSchema::COL_LINKS_SPAN_ID] = Value(LogicalType::LIST(LogicalType::VARCHAR));
				row[OTLPTracesSchema::COL_LINKS_TRACE_STATE] = Value(LogicalType::LIST(LogicalType::VARCHAR));
				row[OTLPTracesSchema::COL_LINKS_ATTRIBUTES] =
				    Value(LogicalType::LIST(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));

				rows.push_back(std::move(row));
				row_count++;
			}
		}
	}

	return row_count;
}

idx_t OTLPProtobufParser::ParseLogsToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	last_error = "";

	opentelemetry::proto::logs::v1::LogsData logs_data;
	if (!logs_data.ParseFromArray(data, static_cast<int>(length))) {
		last_error = "Failed to parse LogsData protobuf";
		return 0;
	}

	idx_t row_count = 0;

	// Iterate through resource logs
	for (const auto &resource_log : logs_data.resource_logs()) {
		const auto &resource = resource_log.resource();
		string service_name = ExtractServiceName(resource);
		Value resource_attrs = ConvertAttributesToMap(resource.attributes());
		string resource_schema_url = resource_log.schema_url();

		// Iterate through scope logs
		for (const auto &scope_log : resource_log.scope_logs()) {
			const auto &scope = scope_log.scope();
			string scope_name = scope.name();
			string scope_version = scope.version();
			string scope_schema_url = scope_log.schema_url();

			// Iterate through log records
			for (const auto &log_record : scope_log.log_records()) {
				vector<Value> row(OTLPLogsSchema::COLUMN_COUNT);

				// Extract timestamp
				auto timestamp = NanosToTimestamp(log_record.time_unix_nano());

				// Populate row (15 columns)
				row[OTLPLogsSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
				row[OTLPLogsSchema::COL_TRACE_ID] = Value(BytesToHex(log_record.trace_id()));
				row[OTLPLogsSchema::COL_SPAN_ID] = Value(BytesToHex(log_record.span_id()));
				row[OTLPLogsSchema::COL_TRACE_FLAGS] = Value::UINTEGER(log_record.flags());
				row[OTLPLogsSchema::COL_SEVERITY_TEXT] = Value(log_record.severity_text());
				row[OTLPLogsSchema::COL_SEVERITY_NUMBER] = Value::INTEGER(log_record.severity_number());
				row[OTLPLogsSchema::COL_SERVICE_NAME] = Value(service_name);
				row[OTLPLogsSchema::COL_BODY] = Value(AnyValueToString(log_record.body()));
				row[OTLPLogsSchema::COL_RESOURCE_SCHEMA_URL] = Value(resource_schema_url);
				row[OTLPLogsSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
				row[OTLPLogsSchema::COL_SCOPE_SCHEMA_URL] = Value(scope_schema_url);
				row[OTLPLogsSchema::COL_SCOPE_NAME] = Value(scope_name);
				row[OTLPLogsSchema::COL_SCOPE_VERSION] = Value(scope_version);
				row[OTLPLogsSchema::COL_SCOPE_ATTRIBUTES] = ConvertAttributesToMap(scope.attributes());
				row[OTLPLogsSchema::COL_LOG_ATTRIBUTES] = ConvertAttributesToMap(log_record.attributes());

				rows.push_back(std::move(row));
				row_count++;
			}
		}
	}

	return row_count;
}

idx_t OTLPProtobufParser::ParseMetricsToTypedRows(const char *data, size_t length, vector<vector<Value>> &rows) {
	last_error = "";

	opentelemetry::proto::metrics::v1::MetricsData metrics_data;
	if (!metrics_data.ParseFromArray(data, static_cast<int>(length))) {
		last_error = "Failed to parse MetricsData protobuf";
		return 0;
	}

	idx_t row_count = 0;

	// Iterate through resource metrics
	for (const auto &resource_metric : metrics_data.resource_metrics()) {
		const auto &resource = resource_metric.resource();
		string service_name = ExtractServiceName(resource);
		Value resource_attrs = ConvertAttributesToMap(resource.attributes());

		// Iterate through scope metrics
		for (const auto &scope_metric : resource_metric.scope_metrics()) {
			const auto &scope = scope_metric.scope();
			string scope_name = scope.name();
			string scope_version = scope.version();

			// Iterate through metrics
			for (const auto &metric : scope_metric.metrics()) {
				string metric_name = metric.name();
				string metric_description = metric.description();
				string metric_unit = metric.unit();

				// Route by metric type and create union schema rows
				if (metric.has_gauge()) {
					// Gauge metrics
					for (const auto &data_point : metric.gauge().data_points()) {
						vector<Value> row(OTLPMetricsUnionSchema::COLUMN_COUNT);

						auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

						// Base columns (9)
						row[OTLPMetricsBaseSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
						row[OTLPMetricsBaseSchema::COL_SERVICE_NAME] = Value(service_name);
						row[OTLPMetricsBaseSchema::COL_METRIC_NAME] = Value(metric_name);
						row[OTLPMetricsBaseSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
						row[OTLPMetricsBaseSchema::COL_METRIC_UNIT] = Value(metric_unit);
						row[OTLPMetricsBaseSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
						row[OTLPMetricsBaseSchema::COL_SCOPE_NAME] = Value(scope_name);
						row[OTLPMetricsBaseSchema::COL_SCOPE_VERSION] = Value(scope_version);
						row[OTLPMetricsBaseSchema::COL_ATTRIBUTES] = ConvertAttributesToMap(data_point.attributes());

						// Union discriminator
						row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("gauge");

						// Gauge-specific column
						double value = data_point.has_as_double() ? data_point.as_double()
						                                          : static_cast<double>(data_point.as_int());
						row[OTLPMetricsUnionSchema::COL_VALUE] = Value::DOUBLE(value);

						// NULL all other union columns
						for (idx_t i = OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY;
						     i < OTLPMetricsUnionSchema::COLUMN_COUNT; i++) {
							if (i == OTLPMetricsUnionSchema::COL_VALUE)
								continue; // Already set
							// Determine type for NULL value
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
							} else if (i == OTLPMetricsUnionSchema::COL_SUM || i == OTLPMetricsUnionSchema::COL_MIN ||
							           i == OTLPMetricsUnionSchema::COL_MAX) {
								row[i] = Value(LogicalType::DOUBLE);
							} else {
								row[i] = Value(LogicalType::LIST(LogicalType::UBIGINT)); // Lists
							}
						}

						rows.push_back(std::move(row));
						row_count++;
					}
				} else if (metric.has_sum()) {
					// Sum metrics
					for (const auto &data_point : metric.sum().data_points()) {
						vector<Value> row(OTLPMetricsUnionSchema::COLUMN_COUNT);

						auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

						// Base columns (9)
						row[OTLPMetricsBaseSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
						row[OTLPMetricsBaseSchema::COL_SERVICE_NAME] = Value(service_name);
						row[OTLPMetricsBaseSchema::COL_METRIC_NAME] = Value(metric_name);
						row[OTLPMetricsBaseSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
						row[OTLPMetricsBaseSchema::COL_METRIC_UNIT] = Value(metric_unit);
						row[OTLPMetricsBaseSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
						row[OTLPMetricsBaseSchema::COL_SCOPE_NAME] = Value(scope_name);
						row[OTLPMetricsBaseSchema::COL_SCOPE_VERSION] = Value(scope_version);
						row[OTLPMetricsBaseSchema::COL_ATTRIBUTES] = ConvertAttributesToMap(data_point.attributes());

						// Union discriminator
						row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("sum");

						// Sum-specific columns
						double value = data_point.has_as_double() ? data_point.as_double()
						                                          : static_cast<double>(data_point.as_int());
						row[OTLPMetricsUnionSchema::COL_VALUE] = Value::DOUBLE(value);
						row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] =
						    Value::INTEGER(static_cast<int32_t>(metric.sum().aggregation_temporality()));
						row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value::BOOLEAN(metric.sum().is_monotonic());

						// NULL all other union columns
						for (idx_t i = OTLPMetricsUnionSchema::COL_COUNT; i < OTLPMetricsUnionSchema::COLUMN_COUNT;
						     i++) {
							if (i == OTLPMetricsUnionSchema::COL_COUNT || i == OTLPMetricsUnionSchema::COL_ZERO_COUNT) {
								row[i] = Value(LogicalType::UBIGINT);
							} else if (i == OTLPMetricsUnionSchema::COL_SCALE ||
							           i == OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET ||
							           i == OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET) {
								row[i] = Value(LogicalType::INTEGER);
							} else if (i == OTLPMetricsUnionSchema::COL_SUM || i == OTLPMetricsUnionSchema::COL_MIN ||
							           i == OTLPMetricsUnionSchema::COL_MAX) {
								row[i] = Value(LogicalType::DOUBLE);
							} else {
								row[i] = Value(LogicalType::LIST(LogicalType::UBIGINT)); // Lists
							}
						}

						rows.push_back(std::move(row));
						row_count++;
					}
				}
				// TODO: Add histogram, exponential_histogram, and summary parsing
				// For now, skip other metric types
			}
		}
	}

	return row_count;
}

} // namespace duckdb
