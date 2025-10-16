#include "receiver/otlp_receiver.hpp"
#include "receiver/otlp_helpers.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "schema/otlp_logs_schema.hpp"
#include "schema/otlp_metrics_schemas.hpp"

// Generated gRPC service stubs
#include "opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.grpc.pb.h"
#include "opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h"
#include "opentelemetry/proto/common/v1/common.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"

#include <google/protobuf/util/json_util.h>
#include <grpcpp/server_builder.h>

namespace duckdb {

// Forward declaration for service implementations
namespace {

//! Generic OTLP service implementation
//! Handles all 3 signal types with a single implementation pattern
template <typename TService, typename TRequest, typename TResponse>
class GenericOTLPService final : public TService::Service {
public:
	GenericOTLPService(shared_ptr<OTLPStorageInfo> storage_info, OTLPSignalType signal_type)
	    : storage_info_(storage_info), signal_type_(signal_type) {
	}

	grpc::Status Export(grpc::ServerContext *context, const TRequest *request, TResponse *response) override {
		try {
			if (signal_type_ == OTLPSignalType::TRACES) {
				// Parse traces from protobuf request
				auto traces_request =
				    dynamic_cast<const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest *>(
				        request);
				if (!traces_request) {
					return grpc::Status(grpc::StatusCode::INTERNAL, "Invalid traces request");
				}

				auto buffer = storage_info_->GetBuffer(OTLPTableType::TRACES);
				if (!buffer) {
					return grpc::Status(grpc::StatusCode::INTERNAL, "Traces buffer not found");
				}

				// Iterate through all resource spans
				for (const auto &resource_span : traces_request->resource_spans()) {
					const auto &resource = resource_span.resource();
					string service_name = ExtractServiceName(resource);
					Value resource_attrs = ConvertAttributesToMap(resource.attributes());

					// Iterate through scope spans
					for (const auto &scope_span : resource_span.scope_spans()) {
						const auto &scope = scope_span.scope();
						string scope_name = scope.name();
						string scope_version = scope.version();

						// Iterate through spans (direct appender)
						auto app = buffer->GetAppender();
						for (const auto &span : scope_span.spans()) {
							app.BeginRow();
							app.SetTimestampNS(OTLPTracesSchema::COL_TIMESTAMP,
							                   NanosToTimestamp(span.start_time_unix_nano()));
							app.SetVarchar(OTLPTracesSchema::COL_TRACE_ID, BytesToHex(span.trace_id()));
							app.SetVarchar(OTLPTracesSchema::COL_SPAN_ID, BytesToHex(span.span_id()));
							app.SetVarchar(OTLPTracesSchema::COL_PARENT_SPAN_ID, BytesToHex(span.parent_span_id()));
							app.SetVarchar(OTLPTracesSchema::COL_TRACE_STATE, span.trace_state());
							app.SetVarchar(OTLPTracesSchema::COL_SPAN_NAME, span.name());
							app.SetVarchar(OTLPTracesSchema::COL_SPAN_KIND, SpanKindToString(span.kind()));
							app.SetVarchar(OTLPTracesSchema::COL_SERVICE_NAME, service_name);
							app.SetValue(OTLPTracesSchema::COL_RESOURCE_ATTRIBUTES, resource_attrs);
							app.SetVarchar(OTLPTracesSchema::COL_SCOPE_NAME, scope_name);
							app.SetVarchar(OTLPTracesSchema::COL_SCOPE_VERSION, scope_version);
							app.SetValue(OTLPTracesSchema::COL_SPAN_ATTRIBUTES,
							             ConvertAttributesToMap(span.attributes()));
							app.SetBigint(OTLPTracesSchema::COL_DURATION,
							              (int64_t)(span.end_time_unix_nano() - span.start_time_unix_nano()));
							app.SetVarchar(OTLPTracesSchema::COL_STATUS_CODE, StatusCodeToString(span.status().code()));
							app.SetVarchar(OTLPTracesSchema::COL_STATUS_MESSAGE, span.status().message());
							// Events/Links remain empty for now (could be filled similarly)
							app.CommitRow();
						}
					}
				}
			} else if (signal_type_ == OTLPSignalType::LOGS) {
				// Parse logs from protobuf request
				auto logs_request =
				    dynamic_cast<const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest *>(request);
				if (!logs_request) {
					return grpc::Status(grpc::StatusCode::INTERNAL, "Invalid logs request");
				}

				auto buffer = storage_info_->GetBuffer(OTLPTableType::LOGS);
				if (!buffer) {
					return grpc::Status(grpc::StatusCode::INTERNAL, "Logs buffer not found");
				}

				// Iterate through all resource logs
				for (const auto &resource_log : logs_request->resource_logs()) {
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

						// Iterate through log records (direct appender)
						auto app = buffer->GetAppender();
						for (const auto &log_record : scope_log.log_records()) {
							app.BeginRow();
							app.SetTimestampNS(OTLPLogsSchema::COL_TIMESTAMP,
							                   NanosToTimestamp(log_record.time_unix_nano()));
							app.SetVarchar(OTLPLogsSchema::COL_TRACE_ID, BytesToHex(log_record.trace_id()));
							app.SetVarchar(OTLPLogsSchema::COL_SPAN_ID, BytesToHex(log_record.span_id()));
							app.SetUInteger(OTLPLogsSchema::COL_TRACE_FLAGS, log_record.flags());
							app.SetVarchar(OTLPLogsSchema::COL_SEVERITY_TEXT, log_record.severity_text());
							app.SetInteger(OTLPLogsSchema::COL_SEVERITY_NUMBER, log_record.severity_number());
							app.SetVarchar(OTLPLogsSchema::COL_SERVICE_NAME, service_name);
							app.SetVarchar(OTLPLogsSchema::COL_BODY, AnyValueToJSONString(log_record.body()));
							app.SetVarchar(OTLPLogsSchema::COL_RESOURCE_SCHEMA_URL, resource_schema_url);
							app.SetValue(OTLPLogsSchema::COL_RESOURCE_ATTRIBUTES, resource_attrs);
							app.SetVarchar(OTLPLogsSchema::COL_SCOPE_SCHEMA_URL, scope_schema_url);
							app.SetVarchar(OTLPLogsSchema::COL_SCOPE_NAME, scope_name);
							app.SetVarchar(OTLPLogsSchema::COL_SCOPE_VERSION, scope_version);
							app.SetValue(OTLPLogsSchema::COL_SCOPE_ATTRIBUTES,
							             ConvertAttributesToMap(scope.attributes()));
							app.SetValue(OTLPLogsSchema::COL_LOG_ATTRIBUTES,
							             ConvertAttributesToMap(log_record.attributes()));
							app.CommitRow();
						}
					}
				}
			} else {
				// Parse metrics from protobuf request
				auto metrics_request =
				    dynamic_cast<const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest *>(
				        request);
				if (!metrics_request) {
					return grpc::Status(grpc::StatusCode::INTERNAL, "Invalid metrics request");
				}

				// Iterate through all resource metrics
				for (const auto &resource_metric : metrics_request->resource_metrics()) {
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

							// Route to appropriate buffer based on metric type
							if (metric.has_gauge()) {
								// Gauge metrics - route to gauge buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::GAUGE);
								if (!buffer) {
									continue;
								}

								auto mapp = buffer->GetAppender();
								for (const auto &data_point : metric.gauge().data_points()) {
									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());
									double value = data_point.has_as_double()
									                   ? data_point.as_double()
									                   : static_cast<double>(data_point.as_int());
									auto dp_attrs = ConvertAttributesToMap(data_point.attributes());

									// Populate type-specific buffer using Appender
									mapp.BeginRow();
									mapp.SetTimestampNS(OTLPMetricsGaugeSchema::COL_TIMESTAMP, timestamp);
									mapp.SetVarchar(OTLPMetricsGaugeSchema::COL_SERVICE_NAME, service_name);
									mapp.SetVarchar(OTLPMetricsGaugeSchema::COL_METRIC_NAME, metric_name);
									mapp.SetVarchar(OTLPMetricsGaugeSchema::COL_METRIC_DESCRIPTION, metric_description);
									mapp.SetVarchar(OTLPMetricsGaugeSchema::COL_METRIC_UNIT, metric_unit);
									mapp.SetValue(OTLPMetricsGaugeSchema::COL_RESOURCE_ATTRIBUTES, resource_attrs);
									mapp.SetVarchar(OTLPMetricsGaugeSchema::COL_SCOPE_NAME, scope_name);
									mapp.SetVarchar(OTLPMetricsGaugeSchema::COL_SCOPE_VERSION, scope_version);
									mapp.SetValue(OTLPMetricsGaugeSchema::COL_ATTRIBUTES, dp_attrs);
									mapp.SetDouble(OTLPMetricsGaugeSchema::COL_VALUE, value);
									mapp.CommitRow();

									// Union is provided as a read-time view; no union buffer writes.
								}
							} else if (metric.has_sum()) {
								// Sum metrics - route to sum buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::SUM);
								if (!buffer) {
									continue;
								}

								auto mapp_sum = buffer->GetAppender();
								for (const auto &data_point : metric.sum().data_points()) {
									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());
									double value = data_point.has_as_double()
									                   ? data_point.as_double()
									                   : static_cast<double>(data_point.as_int());
									auto dp_attrs = ConvertAttributesToMap(data_point.attributes());
									auto aggregation_temporality =
									    static_cast<int32_t>(metric.sum().aggregation_temporality());
									auto is_monotonic = metric.sum().is_monotonic();

									// Populate type-specific buffer using Appender
									mapp_sum.BeginRow();
									mapp_sum.SetTimestampNS(OTLPMetricsSumSchema::COL_TIMESTAMP, timestamp);
									mapp_sum.SetVarchar(OTLPMetricsSumSchema::COL_SERVICE_NAME, service_name);
									mapp_sum.SetVarchar(OTLPMetricsSumSchema::COL_METRIC_NAME, metric_name);
									mapp_sum.SetVarchar(OTLPMetricsSumSchema::COL_METRIC_DESCRIPTION,
									                    metric_description);
									mapp_sum.SetVarchar(OTLPMetricsSumSchema::COL_METRIC_UNIT, metric_unit);
									mapp_sum.SetValue(OTLPMetricsSumSchema::COL_RESOURCE_ATTRIBUTES, resource_attrs);
									mapp_sum.SetVarchar(OTLPMetricsSumSchema::COL_SCOPE_NAME, scope_name);
									mapp_sum.SetVarchar(OTLPMetricsSumSchema::COL_SCOPE_VERSION, scope_version);
									mapp_sum.SetValue(OTLPMetricsSumSchema::COL_ATTRIBUTES, dp_attrs);
									mapp_sum.SetDouble(OTLPMetricsSumSchema::COL_VALUE, value);
									mapp_sum.SetValue(OTLPMetricsSumSchema::COL_AGGREGATION_TEMPORALITY,
									                  Value::INTEGER(aggregation_temporality));
									mapp_sum.SetBoolean(OTLPMetricsSumSchema::COL_IS_MONOTONIC, is_monotonic);
									mapp_sum.CommitRow();

									// Union is provided as a read-time view; no union buffer writes.
								}
							} else if (metric.has_histogram()) {
								// Histogram metrics - route to histogram buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::HISTOGRAM);
								if (!buffer) {
									continue;
								}

								auto mapp_hist = buffer->GetAppender();
								for (const auto &data_point : metric.histogram().data_points()) {
									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());
									auto dp_attrs = ConvertAttributesToMap(data_point.attributes());

									// Convert bucket counts and bounds
									vector<Value> bucket_counts;
									bucket_counts.reserve(data_point.bucket_counts_size());
									for (auto &bc : data_point.bucket_counts()) {
										bucket_counts.emplace_back(Value::UBIGINT(bc));
									}
									vector<Value> explicit_bounds;
									explicit_bounds.reserve(data_point.explicit_bounds_size());
									for (auto &bd : data_point.explicit_bounds()) {
										explicit_bounds.emplace_back(Value::DOUBLE(bd));
									}

									// Populate type-specific buffer using Appender
									mapp_hist.BeginRow();
									mapp_hist.SetTimestampNS(OTLPMetricsHistogramSchema::COL_TIMESTAMP, timestamp);
									mapp_hist.SetVarchar(OTLPMetricsHistogramSchema::COL_SERVICE_NAME, service_name);
									mapp_hist.SetVarchar(OTLPMetricsHistogramSchema::COL_METRIC_NAME, metric_name);
									mapp_hist.SetVarchar(OTLPMetricsHistogramSchema::COL_METRIC_DESCRIPTION,
									                     metric_description);
									mapp_hist.SetVarchar(OTLPMetricsHistogramSchema::COL_METRIC_UNIT, metric_unit);
									mapp_hist.SetValue(OTLPMetricsHistogramSchema::COL_RESOURCE_ATTRIBUTES,
									                   resource_attrs);
									mapp_hist.SetVarchar(OTLPMetricsHistogramSchema::COL_SCOPE_NAME, scope_name);
									mapp_hist.SetVarchar(OTLPMetricsHistogramSchema::COL_SCOPE_VERSION, scope_version);
									mapp_hist.SetValue(OTLPMetricsHistogramSchema::COL_ATTRIBUTES, dp_attrs);
									mapp_hist.SetUBigint(OTLPMetricsHistogramSchema::COL_COUNT, data_point.count());
									if (data_point.has_sum()) {
										mapp_hist.SetDouble(OTLPMetricsHistogramSchema::COL_SUM, data_point.sum());
									} else {
										mapp_hist.SetNull(OTLPMetricsHistogramSchema::COL_SUM);
									}
									mapp_hist.SetValue(OTLPMetricsHistogramSchema::COL_BUCKET_COUNTS,
									                   Value::LIST(LogicalType::UBIGINT, bucket_counts));
									mapp_hist.SetValue(OTLPMetricsHistogramSchema::COL_EXPLICIT_BOUNDS,
									                   Value::LIST(LogicalType::DOUBLE, explicit_bounds));
									if (data_point.has_min())
										mapp_hist.SetDouble(OTLPMetricsHistogramSchema::COL_MIN, data_point.min());
									else
										mapp_hist.SetNull(OTLPMetricsHistogramSchema::COL_MIN);
									if (data_point.has_max())
										mapp_hist.SetDouble(OTLPMetricsHistogramSchema::COL_MAX, data_point.max());
									else
										mapp_hist.SetNull(OTLPMetricsHistogramSchema::COL_MAX);
									mapp_hist.CommitRow();

									// Union is provided as a read-time view; no union buffer writes.
								}
							} else if (metric.has_exponential_histogram()) {
								// Exponential Histogram metrics - route to exp_histogram buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::EXPONENTIAL_HISTOGRAM);
								if (!buffer) {
									continue;
								}

								auto mapp_exph = buffer->GetAppender();
								for (const auto &data_point : metric.exponential_histogram().data_points()) {
									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());
									auto dp_attrs = ConvertAttributesToMap(data_point.attributes());

									// Convert positive and negative bucket counts
									vector<Value> pos_bucket_counts;
									if (data_point.has_positive()) {
										auto &pos = data_point.positive();
										pos_bucket_counts.reserve(pos.bucket_counts_size());
										for (auto &v : pos.bucket_counts())
											pos_bucket_counts.emplace_back(Value::UBIGINT(v));
									}
									vector<Value> neg_bucket_counts;
									if (data_point.has_negative()) {
										auto &neg = data_point.negative();
										neg_bucket_counts.reserve(neg.bucket_counts_size());
										for (auto &v : neg.bucket_counts())
											neg_bucket_counts.emplace_back(Value::UBIGINT(v));
									}

									// Populate type-specific buffer using Appender
									mapp_exph.BeginRow();
									mapp_exph.SetTimestampNS(OTLPMetricsExpHistogramSchema::COL_TIMESTAMP, timestamp);
									mapp_exph.SetVarchar(OTLPMetricsExpHistogramSchema::COL_SERVICE_NAME, service_name);
									mapp_exph.SetVarchar(OTLPMetricsExpHistogramSchema::COL_METRIC_NAME, metric_name);
									mapp_exph.SetVarchar(OTLPMetricsExpHistogramSchema::COL_METRIC_DESCRIPTION,
									                     metric_description);
									mapp_exph.SetVarchar(OTLPMetricsExpHistogramSchema::COL_METRIC_UNIT, metric_unit);
									mapp_exph.SetValue(OTLPMetricsExpHistogramSchema::COL_RESOURCE_ATTRIBUTES,
									                   resource_attrs);
									mapp_exph.SetVarchar(OTLPMetricsExpHistogramSchema::COL_SCOPE_NAME, scope_name);
									mapp_exph.SetVarchar(OTLPMetricsExpHistogramSchema::COL_SCOPE_VERSION,
									                     scope_version);
									mapp_exph.SetValue(OTLPMetricsExpHistogramSchema::COL_ATTRIBUTES, dp_attrs);
									mapp_exph.SetUBigint(OTLPMetricsExpHistogramSchema::COL_COUNT, data_point.count());
									if (data_point.has_sum())
										mapp_exph.SetDouble(OTLPMetricsExpHistogramSchema::COL_SUM, data_point.sum());
									else
										mapp_exph.SetNull(OTLPMetricsExpHistogramSchema::COL_SUM);
									mapp_exph.SetInteger(OTLPMetricsExpHistogramSchema::COL_SCALE, data_point.scale());
									mapp_exph.SetUBigint(OTLPMetricsExpHistogramSchema::COL_ZERO_COUNT,
									                     data_point.zero_count());
									mapp_exph.SetInteger(OTLPMetricsExpHistogramSchema::COL_POSITIVE_OFFSET,
									                     data_point.has_positive() ? data_point.positive().offset()
									                                               : 0);
									mapp_exph.SetValue(OTLPMetricsExpHistogramSchema::COL_POSITIVE_BUCKET_COUNTS,
									                   Value::LIST(LogicalType::UBIGINT, pos_bucket_counts));
									mapp_exph.SetInteger(OTLPMetricsExpHistogramSchema::COL_NEGATIVE_OFFSET,
									                     data_point.has_negative() ? data_point.negative().offset()
									                                               : 0);
									mapp_exph.SetValue(OTLPMetricsExpHistogramSchema::COL_NEGATIVE_BUCKET_COUNTS,
									                   Value::LIST(LogicalType::UBIGINT, neg_bucket_counts));
									if (data_point.has_min())
										mapp_exph.SetDouble(OTLPMetricsExpHistogramSchema::COL_MIN, data_point.min());
									else
										mapp_exph.SetNull(OTLPMetricsExpHistogramSchema::COL_MIN);
									if (data_point.has_max())
										mapp_exph.SetDouble(OTLPMetricsExpHistogramSchema::COL_MAX, data_point.max());
									else
										mapp_exph.SetNull(OTLPMetricsExpHistogramSchema::COL_MAX);
									mapp_exph.CommitRow();

									// Union is provided as a read-time view; no union buffer writes.
								}
							} else if (metric.has_summary()) {
								// Summary metrics - route to summary buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::SUMMARY);
								if (!buffer) {
									continue;
								}

								auto mapp_sumry = buffer->GetAppender();
								for (const auto &data_point : metric.summary().data_points()) {
									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());
									auto dp_attrs = ConvertAttributesToMap(data_point.attributes());

									// Convert quantile values and quantiles
									vector<Value> quantile_values;
									vector<Value> quantile_quantiles;
									quantile_values.reserve(data_point.quantile_values_size());
									quantile_quantiles.reserve(data_point.quantile_values_size());
									for (const auto &qv : data_point.quantile_values()) {
										quantile_values.emplace_back(Value::DOUBLE(qv.value()));
										quantile_quantiles.emplace_back(Value::DOUBLE(qv.quantile()));
									}

									// Populate type-specific buffer using Appender
									mapp_sumry.BeginRow();
									mapp_sumry.SetTimestampNS(OTLPMetricsSummarySchema::COL_TIMESTAMP, timestamp);
									mapp_sumry.SetVarchar(OTLPMetricsSummarySchema::COL_SERVICE_NAME, service_name);
									mapp_sumry.SetVarchar(OTLPMetricsSummarySchema::COL_METRIC_NAME, metric_name);
									mapp_sumry.SetVarchar(OTLPMetricsSummarySchema::COL_METRIC_DESCRIPTION,
									                      metric_description);
									mapp_sumry.SetVarchar(OTLPMetricsSummarySchema::COL_METRIC_UNIT, metric_unit);
									mapp_sumry.SetValue(OTLPMetricsSummarySchema::COL_RESOURCE_ATTRIBUTES,
									                    resource_attrs);
									mapp_sumry.SetVarchar(OTLPMetricsSummarySchema::COL_SCOPE_NAME, scope_name);
									mapp_sumry.SetVarchar(OTLPMetricsSummarySchema::COL_SCOPE_VERSION, scope_version);
									mapp_sumry.SetValue(OTLPMetricsSummarySchema::COL_ATTRIBUTES, dp_attrs);
									mapp_sumry.SetUBigint(OTLPMetricsSummarySchema::COL_COUNT, data_point.count());
									mapp_sumry.SetDouble(OTLPMetricsSummarySchema::COL_SUM, data_point.sum());
									mapp_sumry.SetValue(OTLPMetricsSummarySchema::COL_QUANTILE_VALUES,
									                    Value::LIST(LogicalType::DOUBLE, quantile_values));
									mapp_sumry.SetValue(OTLPMetricsSummarySchema::COL_QUANTILE_QUANTILES,
									                    Value::LIST(LogicalType::DOUBLE, quantile_quantiles));
									mapp_sumry.CommitRow();

									// Union is provided as a read-time view; no union buffer writes.
								}
							}
						}
					}
				}
			}

			return grpc::Status::OK;
		} catch (std::exception &e) {
			return grpc::Status(grpc::StatusCode::INTERNAL, string("Error: ") + e.what());
		}
	}

private:
	shared_ptr<OTLPStorageInfo> storage_info_;
	OTLPSignalType signal_type_;
};

} // anonymous namespace

OTLPReceiver::OTLPReceiver(const string &host, uint16_t port, shared_ptr<OTLPStorageInfo> storage_info)
    : host_(host), port_(port), storage_info_(storage_info), running_(false), shutdown_requested_(false) {
}

OTLPReceiver::~OTLPReceiver() {
	Stop();
}

void OTLPReceiver::Start() {
	if (running_.load()) {
		return; // Already running
	}

	if (server_thread_.joinable()) {
		// Should not happen in normal operation, but join to avoid std::terminate
		server_thread_.join();
	}

	shutdown_requested_.store(false);
	startup_error_.clear();
	server_thread_ = std::thread(&OTLPReceiver::ServerThread, this);

	// Wait for server to start or fail with a timeout
	const auto start = std::chrono::steady_clock::now();
	const auto timeout = std::chrono::seconds(5);
	while (!running_.load() && !shutdown_requested_.load()) {
		if (std::chrono::steady_clock::now() - start > timeout) {
			// Timeout: request shutdown and report error
			shutdown_requested_.store(true);
			std::lock_guard<std::mutex> lock(error_mutex_);
			if (startup_error_.empty()) {
				startup_error_ = "Timed out waiting for gRPC server to start";
			}
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	// Check if startup failed
	if (shutdown_requested_.load() && !running_.load()) {
		if (server_thread_.joinable()) {
			server_thread_.join();
		}
		server_.reset();
		// Server failed to start
		std::lock_guard<std::mutex> lock(error_mutex_);
		if (!startup_error_.empty()) {
			throw IOException("Failed to start OTLP gRPC server: " + startup_error_);
		} else {
			throw IOException("Failed to start OTLP gRPC server on " + host_ + ":" + std::to_string(port_));
		}
	}
}

void OTLPReceiver::Stop() {
	shutdown_requested_.store(true);

	if (server_) {
		server_->Shutdown();
	}

	if (server_thread_.joinable()) {
		server_thread_.join();
	}

	server_.reset();
	running_.store(false);
}

void OTLPReceiver::ServerThread() {
	// Create service instances that insert directly into DuckDB tables
	auto trace_service =
	    std::make_unique<GenericOTLPService<opentelemetry::proto::collector::trace::v1::TraceService,
	                                        opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest,
	                                        opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse>>(
	        storage_info_, OTLPSignalType::TRACES);

	auto metrics_service = std::make_unique<
	    GenericOTLPService<opentelemetry::proto::collector::metrics::v1::MetricsService,
	                       opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest,
	                       opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse>>(
	    storage_info_, OTLPSignalType::METRICS);

	auto logs_service =
	    std::make_unique<GenericOTLPService<opentelemetry::proto::collector::logs::v1::LogsService,
	                                        opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest,
	                                        opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse>>(
	        storage_info_, OTLPSignalType::LOGS);

	// Build server
	grpc::ServerBuilder builder;
	string server_address = host_ + ":" + std::to_string(port_);
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(trace_service.get());
	builder.RegisterService(metrics_service.get());
	builder.RegisterService(logs_service.get());

	// Start server
	try {
		server_ = builder.BuildAndStart();
		if (!server_) {
			std::lock_guard<std::mutex> lock(error_mutex_);
			startup_error_ = "Failed to bind to " + server_address + " (port may be in use)";
			shutdown_requested_.store(true);
			return;
		}
	} catch (std::exception &e) {
		std::lock_guard<std::mutex> lock(error_mutex_);
		startup_error_ = string("gRPC server exception: ") + e.what();
		shutdown_requested_.store(true);
		return;
	}

	running_.store(true);

	// Wait for shutdown
	server_->Wait();

	running_.store(false);
}

} // namespace duckdb
