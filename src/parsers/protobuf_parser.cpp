#include "parsers/protobuf_parser.hpp"
#include "receiver/otlp_helpers.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "schema/otlp_logs_schema.hpp"
#include "schema/otlp_metrics_schemas.hpp"
#include "schema/otlp_metrics_union_schema.hpp"
#include "receiver/row_builders_traces_logs.hpp"
#include "schema/otlp_types.hpp"
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
				// Extract best-effort timestamp from metric datapoints
				uint64_t nanos = 0;
				if (metric.has_gauge() && !metric.gauge().data_points().empty()) {
					nanos = metric.gauge().data_points(0).time_unix_nano();
				} else if (metric.has_sum() && !metric.sum().data_points().empty()) {
					nanos = metric.sum().data_points(0).time_unix_nano();
				} else if (metric.has_histogram() && !metric.histogram().data_points().empty()) {
					nanos = metric.histogram().data_points(0).time_unix_nano();
				} else if (metric.has_exponential_histogram() &&
				           !metric.exponential_histogram().data_points().empty()) {
					nanos = metric.exponential_histogram().data_points(0).time_unix_nano();
				} else if (metric.has_summary() && !metric.summary().data_points().empty()) {
					nanos = metric.summary().data_points(0).time_unix_nano();
				}

				timestamp_t ts =
				    nanos ? duckdb::NanosToTimestamp(static_cast<int64_t>(nanos)) : Timestamp::GetCurrentTimestamp();

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

	// Iterate through resource spans (using shared row builders)
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
				TracesRowData d;
				d.timestamp = NanosToTimestamp(span.start_time_unix_nano());
				d.trace_id = BytesToHex(span.trace_id());
				d.span_id = BytesToHex(span.span_id());
				d.parent_span_id = BytesToHex(span.parent_span_id());
				d.trace_state = span.trace_state();
				d.span_name = span.name();
				d.span_kind = SpanKindToString(span.kind());
				d.service_name = service_name;
				d.resource_attributes = resource_attrs;
				d.scope_name = scope_name;
				d.scope_version = scope_version;
				d.span_attributes = ConvertAttributesToMap(span.attributes());
				d.duration_ns = (int64_t)(span.end_time_unix_nano() - span.start_time_unix_nano());
				d.status_code = StatusCodeToString(span.status().code());
				d.status_message = span.status().message();

				// Events
				for (const auto &ev : span.events()) {
					d.events_timestamps.emplace_back(Value::TIMESTAMPNS(NanosToTimestamp(ev.time_unix_nano())));
					d.events_names.emplace_back(Value(ev.name()));
					d.events_attributes.emplace_back(ConvertAttributesToMap(ev.attributes()));
				}

				// Links
				for (const auto &lnk : span.links()) {
					d.links_trace_ids.emplace_back(Value(BytesToHex(lnk.trace_id())));
					d.links_span_ids.emplace_back(Value(BytesToHex(lnk.span_id())));
					d.links_trace_states.emplace_back(Value(lnk.trace_state()));
					d.links_attributes.emplace_back(ConvertAttributesToMap(lnk.attributes()));
				}

				rows.push_back(BuildTracesRow(d));
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
				LogsRowData d;
				d.timestamp = NanosToTimestamp(log_record.time_unix_nano());
				d.trace_id = BytesToHex(log_record.trace_id());
				d.span_id = BytesToHex(log_record.span_id());
				d.trace_flags = log_record.flags();
				d.severity_text = log_record.severity_text();
				d.severity_number = log_record.severity_number();
				d.service_name = service_name;
				d.body = AnyValueToJSONString(log_record.body());
				d.resource_schema_url = resource_schema_url;
				d.resource_attributes = resource_attrs;
				d.scope_schema_url = scope_schema_url;
				d.scope_name = scope_name;
				d.scope_version = scope_version;
				d.scope_attributes = ConvertAttributesToMap(scope.attributes());
				d.log_attributes = ConvertAttributesToMap(log_record.attributes());
				rows.push_back(BuildLogsRow(d));
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
				} else if (metric.has_histogram()) {
					// Histogram metrics
					for (const auto &data_point : metric.histogram().data_points()) {
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
						row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("histogram");

						// Histogram-specific columns
						row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] =
						    Value::INTEGER(static_cast<int32_t>(metric.histogram().aggregation_temporality()));
						row[OTLPMetricsUnionSchema::COL_COUNT] = Value::UBIGINT(data_point.count());
						row[OTLPMetricsUnionSchema::COL_SUM] =
						    Value::DOUBLE(data_point.has_sum() ? data_point.sum() : 0.0);
						row[OTLPMetricsUnionSchema::COL_MIN] =
						    Value::DOUBLE(data_point.has_min() ? data_point.min() : 0.0);
						row[OTLPMetricsUnionSchema::COL_MAX] =
						    Value::DOUBLE(data_point.has_max() ? data_point.max() : 0.0);

						// Bucket counts
						vector<Value> bucket_counts;
						for (uint64_t count : data_point.bucket_counts()) {
							bucket_counts.push_back(Value::UBIGINT(count));
						}
						row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] =
						    Value::LIST(LogicalType::UBIGINT, bucket_counts);

						// Explicit bounds
						vector<Value> explicit_bounds;
						for (double bound : data_point.explicit_bounds()) {
							explicit_bounds.push_back(Value::DOUBLE(bound));
						}
						row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
						    Value::LIST(LogicalType::DOUBLE, explicit_bounds);

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
						row_count++;
					}
				} else if (metric.has_exponential_histogram()) {
					// Exponential Histogram metrics
					for (const auto &data_point : metric.exponential_histogram().data_points()) {
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
						row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("exponential_histogram");

						// Exponential histogram-specific columns
						row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value::INTEGER(
						    static_cast<int32_t>(metric.exponential_histogram().aggregation_temporality()));
						row[OTLPMetricsUnionSchema::COL_COUNT] = Value::UBIGINT(data_point.count());
						row[OTLPMetricsUnionSchema::COL_SUM] =
						    Value::DOUBLE(data_point.has_sum() ? data_point.sum() : 0.0);
						row[OTLPMetricsUnionSchema::COL_SCALE] = Value::INTEGER(data_point.scale());
						row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value::UBIGINT(data_point.zero_count());
						row[OTLPMetricsUnionSchema::COL_MIN] =
						    Value::DOUBLE(data_point.has_min() ? data_point.min() : 0.0);
						row[OTLPMetricsUnionSchema::COL_MAX] =
						    Value::DOUBLE(data_point.has_max() ? data_point.max() : 0.0);

						// Positive buckets
						row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] =
						    Value::INTEGER(data_point.has_positive() ? data_point.positive().offset() : 0);
						vector<Value> positive_bucket_counts;
						if (data_point.has_positive()) {
							for (uint64_t count : data_point.positive().bucket_counts()) {
								positive_bucket_counts.push_back(Value::UBIGINT(count));
							}
						}
						row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] =
						    Value::LIST(LogicalType::UBIGINT, positive_bucket_counts);

						// Negative buckets
						row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] =
						    Value::INTEGER(data_point.has_negative() ? data_point.negative().offset() : 0);
						vector<Value> negative_bucket_counts;
						if (data_point.has_negative()) {
							for (uint64_t count : data_point.negative().bucket_counts()) {
								negative_bucket_counts.push_back(Value::UBIGINT(count));
							}
						}
						row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] =
						    Value::LIST(LogicalType::UBIGINT, negative_bucket_counts);

						// NULL other union columns
						row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
						row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
						row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] = Value(LogicalType::LIST(LogicalType::UBIGINT));
						row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
						    Value(LogicalType::LIST(LogicalType::DOUBLE));
						row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
						    Value(LogicalType::LIST(LogicalType::DOUBLE));
						row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
						    Value(LogicalType::LIST(LogicalType::DOUBLE));

						rows.push_back(std::move(row));
						row_count++;
					}
				} else if (metric.has_summary()) {
					// Summary metrics
					for (const auto &data_point : metric.summary().data_points()) {
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
						row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("summary");

						// Summary-specific columns
						row[OTLPMetricsUnionSchema::COL_COUNT] = Value::UBIGINT(data_point.count());
						row[OTLPMetricsUnionSchema::COL_SUM] = Value::DOUBLE(data_point.sum());

						// Quantile values and quantiles
						vector<Value> quantile_values;
						vector<Value> quantile_quantiles;
						for (const auto &quantile : data_point.quantile_values()) {
							quantile_quantiles.push_back(Value::DOUBLE(quantile.quantile()));
							quantile_values.push_back(Value::DOUBLE(quantile.value()));
						}
						row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
						    Value::LIST(LogicalType::DOUBLE, quantile_values);
						row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
						    Value::LIST(LogicalType::DOUBLE, quantile_quantiles);

						// NULL other union columns
						row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
						row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value(LogicalType::INTEGER);
						row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
						row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] = Value(LogicalType::LIST(LogicalType::UBIGINT));
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
						row_count++;
					}
				}
			}
		}
	}

	return row_count;
}

} // namespace duckdb
