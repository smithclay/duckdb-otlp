#include "parsers/protobuf_parser.hpp"
#include "receiver/otlp_helpers.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "schema/otlp_logs_schema.hpp"
#include "schema/otlp_metrics_schemas.hpp"
#include "schema/otlp_metrics_union_schema.hpp"
#include "receiver/row_builders_traces_logs.hpp"
#include "receiver/row_builders_metrics.hpp"
#include "receiver/row_builders.hpp"
#include "schema/otlp_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"

// OpenTelemetry protobuf includes
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "opentelemetry/proto/metrics/v1/metrics.pb.h"
#include "opentelemetry/proto/logs/v1/logs.pb.h"
#include "opentelemetry/proto/common/v1/common.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"

namespace duckdb {

OTLPProtobufParser::OTLPProtobufParser() : last_error("") {
}

OTLPProtobufParser::~OTLPProtobufParser() {
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
						auto timestamp = NanosToTimestamp(data_point.time_unix_nano());
						double value = data_point.has_as_double() ? data_point.as_double()
						                                          : static_cast<double>(data_point.as_int());

						// Build type-specific row using shared builder
						MetricsGaugeData d {timestamp,
						                    service_name,
						                    metric_name,
						                    metric_description,
						                    metric_unit,
						                    resource_attrs,
						                    scope_name,
						                    scope_version,
						                    ConvertAttributesToMap(data_point.attributes()),
						                    value};

						// Transform to union schema for file reading
						rows.push_back(TransformGaugeRow(BuildMetricsGaugeRow(d)));
						row_count++;
					}
				} else if (metric.has_sum()) {
					// Sum metrics
					for (const auto &data_point : metric.sum().data_points()) {
						auto timestamp = NanosToTimestamp(data_point.time_unix_nano());
						double value = data_point.has_as_double() ? data_point.as_double()
						                                          : static_cast<double>(data_point.as_int());

						// Build type-specific row using shared builder
						MetricsSumData d {
						    timestamp,
						    service_name,
						    metric_name,
						    metric_description,
						    metric_unit,
						    resource_attrs,
						    scope_name,
						    scope_version,
						    ConvertAttributesToMap(data_point.attributes()),
						    value,
						    std::optional<int32_t>(static_cast<int32_t>(metric.sum().aggregation_temporality())),
						    metric.sum().is_monotonic()};

						// Transform to union schema for file reading
						rows.push_back(TransformSumRow(BuildMetricsSumRow(d)));
						row_count++;
					}
				} else if (metric.has_histogram()) {
					// Histogram metrics
					for (const auto &data_point : metric.histogram().data_points()) {
						auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

						// Convert bucket counts to vector<Value>
						vector<Value> bucket_counts;
						for (uint64_t count : data_point.bucket_counts()) {
							bucket_counts.push_back(Value::UBIGINT(count));
						}

						// Convert explicit bounds to vector<Value>
						vector<Value> explicit_bounds;
						for (double bound : data_point.explicit_bounds()) {
							explicit_bounds.push_back(Value::DOUBLE(bound));
						}

						// Build type-specific row using shared builder
						MetricsHistogramData d {
						    timestamp,
						    service_name,
						    metric_name,
						    metric_description,
						    metric_unit,
						    resource_attrs,
						    scope_name,
						    scope_version,
						    ConvertAttributesToMap(data_point.attributes()),
						    data_point.count(),
						    data_point.has_sum() ? std::optional<double>(data_point.sum()) : std::optional<double>(),
						    bucket_counts,
						    explicit_bounds,
						    data_point.has_min() ? std::optional<double>(data_point.min()) : std::optional<double>(),
						    data_point.has_max() ? std::optional<double>(data_point.max()) : std::optional<double>()};

						// Transform to union schema for file reading
						rows.push_back(TransformHistogramRow(BuildMetricsHistogramRow(d)));
						row_count++;
					}
				} else if (metric.has_exponential_histogram()) {
					// Exponential Histogram metrics
					for (const auto &data_point : metric.exponential_histogram().data_points()) {
						auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

						// Convert positive bucket counts to vector<Value>
						vector<Value> positive_bucket_counts;
						if (data_point.has_positive()) {
							for (uint64_t count : data_point.positive().bucket_counts()) {
								positive_bucket_counts.push_back(Value::UBIGINT(count));
							}
						}

						// Convert negative bucket counts to vector<Value>
						vector<Value> negative_bucket_counts;
						if (data_point.has_negative()) {
							for (uint64_t count : data_point.negative().bucket_counts()) {
								negative_bucket_counts.push_back(Value::UBIGINT(count));
							}
						}

						// Build type-specific row using shared builder
						MetricsExpHistogramData d {
						    timestamp,
						    service_name,
						    metric_name,
						    metric_description,
						    metric_unit,
						    resource_attrs,
						    scope_name,
						    scope_version,
						    ConvertAttributesToMap(data_point.attributes()),
						    data_point.count(),
						    data_point.has_sum() ? std::optional<double>(data_point.sum()) : std::optional<double>(),
						    data_point.scale(),
						    data_point.zero_count(),
						    data_point.has_positive() ? data_point.positive().offset() : 0,
						    positive_bucket_counts,
						    data_point.has_negative() ? data_point.negative().offset() : 0,
						    negative_bucket_counts,
						    data_point.has_min() ? std::optional<double>(data_point.min()) : std::optional<double>(),
						    data_point.has_max() ? std::optional<double>(data_point.max()) : std::optional<double>()};

						// Transform to union schema for file reading
						rows.push_back(TransformExpHistogramRow(BuildMetricsExpHistogramRow(d)));
						row_count++;
					}
				} else if (metric.has_summary()) {
					// Summary metrics
					for (const auto &data_point : metric.summary().data_points()) {
						auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

						// Convert quantile values and quantiles to vector<Value>
						vector<Value> quantile_values;
						vector<Value> quantile_quantiles;
						for (const auto &quantile : data_point.quantile_values()) {
							quantile_quantiles.push_back(Value::DOUBLE(quantile.quantile()));
							quantile_values.push_back(Value::DOUBLE(quantile.value()));
						}

						// Build type-specific row using shared builder
						std::optional<double> sum_opt = data_point.sum();

						MetricsSummaryData d {
						    timestamp,          service_name,  metric_name,
						    metric_description, metric_unit,   resource_attrs,
						    scope_name,         scope_version, ConvertAttributesToMap(data_point.attributes()),
						    data_point.count(), sum_opt,       quantile_values,
						    quantile_quantiles};

						// Transform to union schema for file reading
						rows.push_back(TransformSummaryRow(BuildMetricsSummaryRow(d)));
						row_count++;
					}
				}
			}
		}
	}

	return row_count;
}

} // namespace duckdb
