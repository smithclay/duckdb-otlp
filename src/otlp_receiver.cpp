#include "otlp_receiver.hpp"
#include "otlp_helpers.hpp"
#include "otlp_traces_schema.hpp"
#include "otlp_logs_schema.hpp"
#include "otlp_metrics_schemas.hpp"

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
							row[OTLPTracesSchema::COL_EVENTS_TIMESTAMP] =
							    Value(LogicalType::LIST(LogicalType::TIMESTAMP_NS));
							row[OTLPTracesSchema::COL_EVENTS_NAME] = Value(LogicalType::LIST(LogicalType::VARCHAR));
							row[OTLPTracesSchema::COL_EVENTS_ATTRIBUTES] =
							    Value(LogicalType::LIST(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
							row[OTLPTracesSchema::COL_LINKS_TRACE_ID] = Value(LogicalType::LIST(LogicalType::VARCHAR));
							row[OTLPTracesSchema::COL_LINKS_SPAN_ID] = Value(LogicalType::LIST(LogicalType::VARCHAR));
							row[OTLPTracesSchema::COL_LINKS_TRACE_STATE] =
							    Value(LogicalType::LIST(LogicalType::VARCHAR));
							row[OTLPTracesSchema::COL_LINKS_ATTRIBUTES] =
							    Value(LogicalType::LIST(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));

							buffer->Insert(row);
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

							buffer->Insert(row);
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

								for (const auto &data_point : metric.gauge().data_points()) {
									vector<Value> row(OTLPMetricsGaugeSchema::COLUMN_COUNT);

									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

									// Populate gauge schema (10 columns)
									row[OTLPMetricsGaugeSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
									row[OTLPMetricsGaugeSchema::COL_SERVICE_NAME] = Value(service_name);
									row[OTLPMetricsGaugeSchema::COL_METRIC_NAME] = Value(metric_name);
									row[OTLPMetricsGaugeSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
									row[OTLPMetricsGaugeSchema::COL_METRIC_UNIT] = Value(metric_unit);
									row[OTLPMetricsGaugeSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
									row[OTLPMetricsGaugeSchema::COL_SCOPE_NAME] = Value(scope_name);
									row[OTLPMetricsGaugeSchema::COL_SCOPE_VERSION] = Value(scope_version);
									row[OTLPMetricsGaugeSchema::COL_ATTRIBUTES] =
									    ConvertAttributesToMap(data_point.attributes());

									double value = data_point.has_as_double()
									                   ? data_point.as_double()
									                   : static_cast<double>(data_point.as_int());
									row[OTLPMetricsGaugeSchema::COL_VALUE] = Value::DOUBLE(value);

									buffer->Insert(row);
								}
							} else if (metric.has_sum()) {
								// Sum metrics - route to sum buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::SUM);
								if (!buffer) {
									continue;
								}

								for (const auto &data_point : metric.sum().data_points()) {
									vector<Value> row(OTLPMetricsSumSchema::COLUMN_COUNT);

									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

									// Populate sum schema (12 columns)
									row[OTLPMetricsSumSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
									row[OTLPMetricsSumSchema::COL_SERVICE_NAME] = Value(service_name);
									row[OTLPMetricsSumSchema::COL_METRIC_NAME] = Value(metric_name);
									row[OTLPMetricsSumSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
									row[OTLPMetricsSumSchema::COL_METRIC_UNIT] = Value(metric_unit);
									row[OTLPMetricsSumSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
									row[OTLPMetricsSumSchema::COL_SCOPE_NAME] = Value(scope_name);
									row[OTLPMetricsSumSchema::COL_SCOPE_VERSION] = Value(scope_version);
									row[OTLPMetricsSumSchema::COL_ATTRIBUTES] =
									    ConvertAttributesToMap(data_point.attributes());

									double value = data_point.has_as_double()
									                   ? data_point.as_double()
									                   : static_cast<double>(data_point.as_int());
									row[OTLPMetricsSumSchema::COL_VALUE] = Value::DOUBLE(value);
									row[OTLPMetricsSumSchema::COL_AGGREGATION_TEMPORALITY] =
									    Value::INTEGER(static_cast<int32_t>(metric.sum().aggregation_temporality()));
									row[OTLPMetricsSumSchema::COL_IS_MONOTONIC] =
									    Value::BOOLEAN(metric.sum().is_monotonic());

									buffer->Insert(row);
								}
							} else if (metric.has_histogram()) {
								// Histogram metrics - route to histogram buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::HISTOGRAM);
								if (!buffer) {
									continue;
								}

								for (const auto &data_point : metric.histogram().data_points()) {
									vector<Value> row(OTLPMetricsHistogramSchema::COLUMN_COUNT);

									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

									// Populate histogram schema (15 columns)
									row[OTLPMetricsHistogramSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
									row[OTLPMetricsHistogramSchema::COL_SERVICE_NAME] = Value(service_name);
									row[OTLPMetricsHistogramSchema::COL_METRIC_NAME] = Value(metric_name);
									row[OTLPMetricsHistogramSchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
									row[OTLPMetricsHistogramSchema::COL_METRIC_UNIT] = Value(metric_unit);
									row[OTLPMetricsHistogramSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
									row[OTLPMetricsHistogramSchema::COL_SCOPE_NAME] = Value(scope_name);
									row[OTLPMetricsHistogramSchema::COL_SCOPE_VERSION] = Value(scope_version);
									row[OTLPMetricsHistogramSchema::COL_ATTRIBUTES] =
									    ConvertAttributesToMap(data_point.attributes());

									row[OTLPMetricsHistogramSchema::COL_COUNT] = Value::UBIGINT(data_point.count());
									row[OTLPMetricsHistogramSchema::COL_SUM] = data_point.has_sum()
									                                               ? Value::DOUBLE(data_point.sum())
									                                               : Value(LogicalType::DOUBLE);
									row[OTLPMetricsHistogramSchema::COL_BUCKET_COUNTS] =
									    Value(LogicalType::LIST(LogicalType::UBIGINT));
									row[OTLPMetricsHistogramSchema::COL_EXPLICIT_BOUNDS] =
									    Value(LogicalType::LIST(LogicalType::DOUBLE));
									row[OTLPMetricsHistogramSchema::COL_MIN] = data_point.has_min()
									                                               ? Value::DOUBLE(data_point.min())
									                                               : Value(LogicalType::DOUBLE);
									row[OTLPMetricsHistogramSchema::COL_MAX] = data_point.has_max()
									                                               ? Value::DOUBLE(data_point.max())
									                                               : Value(LogicalType::DOUBLE);

									buffer->Insert(row);
								}
							} else if (metric.has_exponential_histogram()) {
								// Exponential Histogram metrics - route to exp_histogram buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::EXPONENTIAL_HISTOGRAM);
								if (!buffer) {
									continue;
								}

								for (const auto &data_point : metric.exponential_histogram().data_points()) {
									vector<Value> row(OTLPMetricsExpHistogramSchema::COLUMN_COUNT);

									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

									// Populate exp_histogram schema (19 columns)
									row[OTLPMetricsExpHistogramSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
									row[OTLPMetricsExpHistogramSchema::COL_SERVICE_NAME] = Value(service_name);
									row[OTLPMetricsExpHistogramSchema::COL_METRIC_NAME] = Value(metric_name);
									row[OTLPMetricsExpHistogramSchema::COL_METRIC_DESCRIPTION] =
									    Value(metric_description);
									row[OTLPMetricsExpHistogramSchema::COL_METRIC_UNIT] = Value(metric_unit);
									row[OTLPMetricsExpHistogramSchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
									row[OTLPMetricsExpHistogramSchema::COL_SCOPE_NAME] = Value(scope_name);
									row[OTLPMetricsExpHistogramSchema::COL_SCOPE_VERSION] = Value(scope_version);
									row[OTLPMetricsExpHistogramSchema::COL_ATTRIBUTES] =
									    ConvertAttributesToMap(data_point.attributes());

									row[OTLPMetricsExpHistogramSchema::COL_COUNT] = Value::UBIGINT(data_point.count());
									row[OTLPMetricsExpHistogramSchema::COL_SUM] = data_point.has_sum()
									                                                  ? Value::DOUBLE(data_point.sum())
									                                                  : Value(LogicalType::DOUBLE);
									row[OTLPMetricsExpHistogramSchema::COL_SCALE] = Value::INTEGER(data_point.scale());
									row[OTLPMetricsExpHistogramSchema::COL_ZERO_COUNT] =
									    Value::UBIGINT(data_point.zero_count());

									// Check existence of optional fields before accessing
									if (data_point.has_positive()) {
										row[OTLPMetricsExpHistogramSchema::COL_POSITIVE_OFFSET] =
										    Value::INTEGER(data_point.positive().offset());
									} else {
										row[OTLPMetricsExpHistogramSchema::COL_POSITIVE_OFFSET] = Value::INTEGER(0);
									}
									row[OTLPMetricsExpHistogramSchema::COL_POSITIVE_BUCKET_COUNTS] =
									    Value(LogicalType::LIST(LogicalType::UBIGINT));

									if (data_point.has_negative()) {
										row[OTLPMetricsExpHistogramSchema::COL_NEGATIVE_OFFSET] =
										    Value::INTEGER(data_point.negative().offset());
									} else {
										row[OTLPMetricsExpHistogramSchema::COL_NEGATIVE_OFFSET] = Value::INTEGER(0);
									}
									row[OTLPMetricsExpHistogramSchema::COL_NEGATIVE_BUCKET_COUNTS] =
									    Value(LogicalType::LIST(LogicalType::UBIGINT));

									row[OTLPMetricsExpHistogramSchema::COL_MIN] = data_point.has_min()
									                                                  ? Value::DOUBLE(data_point.min())
									                                                  : Value(LogicalType::DOUBLE);
									row[OTLPMetricsExpHistogramSchema::COL_MAX] = data_point.has_max()
									                                                  ? Value::DOUBLE(data_point.max())
									                                                  : Value(LogicalType::DOUBLE);

									buffer->Insert(row);
								}
							} else if (metric.has_summary()) {
								// Summary metrics - route to summary buffer
								auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::SUMMARY);
								if (!buffer) {
									continue;
								}

								for (const auto &data_point : metric.summary().data_points()) {
									vector<Value> row(OTLPMetricsSummarySchema::COLUMN_COUNT);

									auto timestamp = NanosToTimestamp(data_point.time_unix_nano());

									// Populate summary schema (13 columns)
									row[OTLPMetricsSummarySchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(timestamp);
									row[OTLPMetricsSummarySchema::COL_SERVICE_NAME] = Value(service_name);
									row[OTLPMetricsSummarySchema::COL_METRIC_NAME] = Value(metric_name);
									row[OTLPMetricsSummarySchema::COL_METRIC_DESCRIPTION] = Value(metric_description);
									row[OTLPMetricsSummarySchema::COL_METRIC_UNIT] = Value(metric_unit);
									row[OTLPMetricsSummarySchema::COL_RESOURCE_ATTRIBUTES] = resource_attrs;
									row[OTLPMetricsSummarySchema::COL_SCOPE_NAME] = Value(scope_name);
									row[OTLPMetricsSummarySchema::COL_SCOPE_VERSION] = Value(scope_version);
									row[OTLPMetricsSummarySchema::COL_ATTRIBUTES] =
									    ConvertAttributesToMap(data_point.attributes());

									row[OTLPMetricsSummarySchema::COL_COUNT] = Value::UBIGINT(data_point.count());
									row[OTLPMetricsSummarySchema::COL_SUM] = Value::DOUBLE(data_point.sum());
									row[OTLPMetricsSummarySchema::COL_QUANTILE_VALUES] =
									    Value(LogicalType::LIST(LogicalType::DOUBLE));
									row[OTLPMetricsSummarySchema::COL_QUANTILE_QUANTILES] =
									    Value(LogicalType::LIST(LogicalType::DOUBLE));

									buffer->Insert(row);
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

	shutdown_requested_.store(false);
	startup_error_.clear();
	server_thread_ = std::thread(&OTLPReceiver::ServerThread, this);

	// Wait for server to start or fail
	while (!running_.load() && !shutdown_requested_.load()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	// Check if startup failed
	if (shutdown_requested_.load() && !running_.load()) {
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
	if (!running_.load()) {
		return; // Not running
	}

	shutdown_requested_.store(true);

	if (server_) {
		server_->Shutdown();
	}

	if (server_thread_.joinable()) {
		server_thread_.join();
	}

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
