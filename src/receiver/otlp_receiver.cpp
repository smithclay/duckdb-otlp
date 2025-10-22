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

namespace {

//=============================================================================
// TraceServiceImpl - Processes OTLP trace exports
//=============================================================================

class TraceServiceImpl final : public opentelemetry::proto::collector::trace::v1::TraceService::Service {
public:
	explicit TraceServiceImpl(shared_ptr<OTLPStorageInfo> storage_info) : storage_info_(storage_info) {
	}

	grpc::Status Export(grpc::ServerContext *context,
	                    const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest *request,
	                    opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse *response) override {
		try {
			auto buffer = storage_info_->GetBuffer(OTLPTableType::TRACES);
			if (!buffer) {
				return grpc::Status(grpc::StatusCode::INTERNAL, "Traces buffer not found");
			}

			for (const auto &resource_span : request->resource_spans()) {
				ProcessResourceSpans(resource_span, buffer);
			}

			return grpc::Status::OK;
		} catch (std::exception &e) {
			return grpc::Status(grpc::StatusCode::INTERNAL, string("Error: ") + e.what());
		}
	}

private:
	void ProcessResourceSpans(const opentelemetry::proto::trace::v1::ResourceSpans &resource_span,
	                          shared_ptr<ColumnarRingBuffer> buffer) {
		// Extract resource context
		const auto &resource = resource_span.resource();
		ResourceContext res_ctx {ExtractServiceName(resource), ConvertAttributesToMap(resource.attributes())};

		// Process each scope span
		for (const auto &scope_span : resource_span.scope_spans()) {
			ProcessScopeSpans(scope_span, res_ctx, buffer);
		}
	}

	void ProcessScopeSpans(const opentelemetry::proto::trace::v1::ScopeSpans &scope_span,
	                       const ResourceContext &res_ctx, shared_ptr<ColumnarRingBuffer> buffer) {
		// Extract scope context
		const auto &scope = scope_span.scope();
		ScopeContext scope_ctx {res_ctx, scope.name(), scope.version()};

		// Process all spans in batch
		auto app = buffer->GetAppender();
		for (const auto &span : scope_span.spans()) {
			app.BeginRow();
			app.SetTimestampNS(OTLPTracesSchema::COL_TIMESTAMP, NanosToTimestamp(span.start_time_unix_nano()));
			app.SetVarchar(OTLPTracesSchema::COL_TRACE_ID, BytesToHex(span.trace_id()));
			app.SetVarchar(OTLPTracesSchema::COL_SPAN_ID, BytesToHex(span.span_id()));
			app.SetVarchar(OTLPTracesSchema::COL_PARENT_SPAN_ID, BytesToHex(span.parent_span_id()));
			app.SetVarchar(OTLPTracesSchema::COL_TRACE_STATE, span.trace_state());
			app.SetVarchar(OTLPTracesSchema::COL_SPAN_NAME, span.name());
			app.SetVarchar(OTLPTracesSchema::COL_SPAN_KIND, SpanKindToString(span.kind()));
			app.SetVarchar(OTLPTracesSchema::COL_SERVICE_NAME, scope_ctx.resource.service_name);
			app.SetValue(OTLPTracesSchema::COL_RESOURCE_ATTRIBUTES, scope_ctx.resource.resource_attrs);
			app.SetVarchar(OTLPTracesSchema::COL_SCOPE_NAME, scope_ctx.scope_name);
			app.SetVarchar(OTLPTracesSchema::COL_SCOPE_VERSION, scope_ctx.scope_version);
			app.SetValue(OTLPTracesSchema::COL_SPAN_ATTRIBUTES, ConvertAttributesToMap(span.attributes()));
			app.SetBigint(OTLPTracesSchema::COL_DURATION,
			              (int64_t)(span.end_time_unix_nano() - span.start_time_unix_nano()));
			app.SetVarchar(OTLPTracesSchema::COL_STATUS_CODE, StatusCodeToString(span.status().code()));
			app.SetVarchar(OTLPTracesSchema::COL_STATUS_MESSAGE, span.status().message());
			app.CommitRow();
		}
	}

	shared_ptr<OTLPStorageInfo> storage_info_;
};

//=============================================================================
// LogsServiceImpl - Processes OTLP log exports
//=============================================================================

class LogsServiceImpl final : public opentelemetry::proto::collector::logs::v1::LogsService::Service {
public:
	explicit LogsServiceImpl(shared_ptr<OTLPStorageInfo> storage_info) : storage_info_(storage_info) {
	}

	grpc::Status Export(grpc::ServerContext *context,
	                    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest *request,
	                    opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse *response) override {
		try {
			auto buffer = storage_info_->GetBuffer(OTLPTableType::LOGS);
			if (!buffer) {
				return grpc::Status(grpc::StatusCode::INTERNAL, "Logs buffer not found");
			}

			for (const auto &resource_log : request->resource_logs()) {
				ProcessResourceLogs(resource_log, buffer);
			}

			return grpc::Status::OK;
		} catch (std::exception &e) {
			return grpc::Status(grpc::StatusCode::INTERNAL, string("Error: ") + e.what());
		}
	}

private:
	void ProcessResourceLogs(const opentelemetry::proto::logs::v1::ResourceLogs &resource_log,
	                         shared_ptr<ColumnarRingBuffer> buffer) {
		// Extract resource context
		const auto &resource = resource_log.resource();
		ResourceContext res_ctx {ExtractServiceName(resource), ConvertAttributesToMap(resource.attributes())};
		string resource_schema_url = resource_log.schema_url();

		// Process each scope log
		for (const auto &scope_log : resource_log.scope_logs()) {
			ProcessScopeLogs(scope_log, res_ctx, resource_schema_url, buffer);
		}
	}

	void ProcessScopeLogs(const opentelemetry::proto::logs::v1::ScopeLogs &scope_log, const ResourceContext &res_ctx,
	                      const string &resource_schema_url, shared_ptr<ColumnarRingBuffer> buffer) {
		// Extract scope context
		const auto &scope = scope_log.scope();
		ScopeContext scope_ctx {res_ctx, scope.name(), scope.version()};
		string scope_schema_url = scope_log.schema_url();
		Value scope_attrs = ConvertAttributesToMap(scope.attributes());

		// Process all log records in batch
		auto app = buffer->GetAppender();
		for (const auto &log_record : scope_log.log_records()) {
			app.BeginRow();
			app.SetTimestampNS(OTLPLogsSchema::COL_TIMESTAMP, NanosToTimestamp(log_record.time_unix_nano()));
			app.SetVarchar(OTLPLogsSchema::COL_TRACE_ID, BytesToHex(log_record.trace_id()));
			app.SetVarchar(OTLPLogsSchema::COL_SPAN_ID, BytesToHex(log_record.span_id()));
			app.SetUInteger(OTLPLogsSchema::COL_TRACE_FLAGS, log_record.flags());
			app.SetVarchar(OTLPLogsSchema::COL_SEVERITY_TEXT, log_record.severity_text());
			app.SetInteger(OTLPLogsSchema::COL_SEVERITY_NUMBER, log_record.severity_number());
			app.SetVarchar(OTLPLogsSchema::COL_SERVICE_NAME, scope_ctx.resource.service_name);
			app.SetVarchar(OTLPLogsSchema::COL_BODY, AnyValueToJSONString(log_record.body()));
			app.SetVarchar(OTLPLogsSchema::COL_RESOURCE_SCHEMA_URL, resource_schema_url);
			app.SetValue(OTLPLogsSchema::COL_RESOURCE_ATTRIBUTES, scope_ctx.resource.resource_attrs);
			app.SetVarchar(OTLPLogsSchema::COL_SCOPE_SCHEMA_URL, scope_schema_url);
			app.SetVarchar(OTLPLogsSchema::COL_SCOPE_NAME, scope_ctx.scope_name);
			app.SetVarchar(OTLPLogsSchema::COL_SCOPE_VERSION, scope_ctx.scope_version);
			app.SetValue(OTLPLogsSchema::COL_SCOPE_ATTRIBUTES, scope_attrs);
			app.SetValue(OTLPLogsSchema::COL_LOG_ATTRIBUTES, ConvertAttributesToMap(log_record.attributes()));
			app.CommitRow();
		}
	}

	shared_ptr<OTLPStorageInfo> storage_info_;
};

//=============================================================================
// MetricsServiceImpl - Processes OTLP metric exports
//=============================================================================

class MetricsServiceImpl final : public opentelemetry::proto::collector::metrics::v1::MetricsService::Service {
public:
	explicit MetricsServiceImpl(shared_ptr<OTLPStorageInfo> storage_info) : storage_info_(storage_info) {
	}

	grpc::Status Export(grpc::ServerContext *context,
	                    const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest *request,
	                    opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse *response) override {
		try {
			for (const auto &resource_metric : request->resource_metrics()) {
				ProcessResourceMetrics(resource_metric);
			}

			return grpc::Status::OK;
		} catch (std::exception &e) {
			return grpc::Status(grpc::StatusCode::INTERNAL, string("Error: ") + e.what());
		}
	}

private:
	void ProcessResourceMetrics(const opentelemetry::proto::metrics::v1::ResourceMetrics &resource_metric) {
		// Extract resource context
		const auto &resource = resource_metric.resource();
		ResourceContext res_ctx {ExtractServiceName(resource), ConvertAttributesToMap(resource.attributes())};

		// Process each scope metric
		for (const auto &scope_metric : resource_metric.scope_metrics()) {
			ProcessScopeMetrics(scope_metric, res_ctx);
		}
	}

	void ProcessScopeMetrics(const opentelemetry::proto::metrics::v1::ScopeMetrics &scope_metric,
	                         const ResourceContext &res_ctx) {
		// Extract scope context
		const auto &scope = scope_metric.scope();
		ScopeContext scope_ctx {res_ctx, scope.name(), scope.version()};

		// Process each metric
		for (const auto &metric : scope_metric.metrics()) {
			ProcessMetric(metric, scope_ctx);
		}
	}

	void ProcessMetric(const opentelemetry::proto::metrics::v1::Metric &metric, const ScopeContext &scope_ctx) {
		// Build metric context
		MetricContext metric_ctx {scope_ctx, metric.name(), metric.description(), metric.unit()};

		// Route to type-specific handler
		if (metric.has_gauge()) {
			ProcessGaugeMetric(metric.gauge(), metric_ctx);
		} else if (metric.has_sum()) {
			ProcessSumMetric(metric.sum(), metric_ctx);
		} else if (metric.has_histogram()) {
			ProcessHistogramMetric(metric.histogram(), metric_ctx);
		} else if (metric.has_exponential_histogram()) {
			ProcessExpHistogramMetric(metric.exponential_histogram(), metric_ctx);
		} else if (metric.has_summary()) {
			ProcessSummaryMetric(metric.summary(), metric_ctx);
		}
	}

	void ProcessGaugeMetric(const opentelemetry::proto::metrics::v1::Gauge &gauge, const MetricContext &metric_ctx) {
		auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::GAUGE);
		if (!buffer) {
			return;
		}

		auto app = buffer->GetAppender();
		for (const auto &dp : gauge.data_points()) {
			auto timestamp = NanosToTimestamp(dp.time_unix_nano());
			double value = dp.has_as_double() ? dp.as_double() : static_cast<double>(dp.as_int());

			app.BeginRow();
			PopulateBaseMetricFields(app, timestamp, metric_ctx);
			app.SetValue(OTLPMetricsGaugeSchema::COL_ATTRIBUTES, ConvertAttributesToMap(dp.attributes()));
			app.SetDouble(OTLPMetricsGaugeSchema::COL_VALUE, value);
			app.CommitRow();
		}
	}

	void ProcessSumMetric(const opentelemetry::proto::metrics::v1::Sum &sum, const MetricContext &metric_ctx) {
		auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::SUM);
		if (!buffer) {
			return;
		}

		auto app = buffer->GetAppender();
		for (const auto &dp : sum.data_points()) {
			auto timestamp = NanosToTimestamp(dp.time_unix_nano());
			double value = dp.has_as_double() ? dp.as_double() : static_cast<double>(dp.as_int());

			app.BeginRow();
			PopulateBaseMetricFields(app, timestamp, metric_ctx);
			app.SetValue(OTLPMetricsSumSchema::COL_ATTRIBUTES, ConvertAttributesToMap(dp.attributes()));
			app.SetDouble(OTLPMetricsSumSchema::COL_VALUE, value);
			app.SetValue(OTLPMetricsSumSchema::COL_AGGREGATION_TEMPORALITY,
			             Value::INTEGER(static_cast<int32_t>(sum.aggregation_temporality())));
			app.SetBoolean(OTLPMetricsSumSchema::COL_IS_MONOTONIC, sum.is_monotonic());
			app.CommitRow();
		}
	}

	void ProcessHistogramMetric(const opentelemetry::proto::metrics::v1::Histogram &histogram,
	                            const MetricContext &metric_ctx) {
		auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::HISTOGRAM);
		if (!buffer) {
			return;
		}

		auto app = buffer->GetAppender();
		for (const auto &dp : histogram.data_points()) {
			auto timestamp = NanosToTimestamp(dp.time_unix_nano());

			// Convert bucket counts and bounds
			vector<Value> bucket_counts;
			bucket_counts.reserve(dp.bucket_counts_size());
			for (auto &bc : dp.bucket_counts()) {
				bucket_counts.emplace_back(Value::UBIGINT(bc));
			}
			vector<Value> explicit_bounds;
			explicit_bounds.reserve(dp.explicit_bounds_size());
			for (auto &bd : dp.explicit_bounds()) {
				explicit_bounds.emplace_back(Value::DOUBLE(bd));
			}

			app.BeginRow();
			PopulateBaseMetricFields(app, timestamp, metric_ctx);
			app.SetValue(OTLPMetricsHistogramSchema::COL_ATTRIBUTES, ConvertAttributesToMap(dp.attributes()));
			app.SetUBigint(OTLPMetricsHistogramSchema::COL_COUNT, dp.count());
			if (dp.has_sum()) {
				app.SetDouble(OTLPMetricsHistogramSchema::COL_SUM, dp.sum());
			} else {
				app.SetNull(OTLPMetricsHistogramSchema::COL_SUM);
			}
			app.SetValue(OTLPMetricsHistogramSchema::COL_BUCKET_COUNTS,
			             Value::LIST(LogicalType::UBIGINT, bucket_counts));
			app.SetValue(OTLPMetricsHistogramSchema::COL_EXPLICIT_BOUNDS,
			             Value::LIST(LogicalType::DOUBLE, explicit_bounds));
			if (dp.has_min()) {
				app.SetDouble(OTLPMetricsHistogramSchema::COL_MIN, dp.min());
			} else {
				app.SetNull(OTLPMetricsHistogramSchema::COL_MIN);
			}
			if (dp.has_max()) {
				app.SetDouble(OTLPMetricsHistogramSchema::COL_MAX, dp.max());
			} else {
				app.SetNull(OTLPMetricsHistogramSchema::COL_MAX);
			}
			app.CommitRow();
		}
	}

	void ProcessExpHistogramMetric(const opentelemetry::proto::metrics::v1::ExponentialHistogram &exp_histogram,
	                               const MetricContext &metric_ctx) {
		auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::EXPONENTIAL_HISTOGRAM);
		if (!buffer) {
			return;
		}

		auto app = buffer->GetAppender();
		for (const auto &dp : exp_histogram.data_points()) {
			auto timestamp = NanosToTimestamp(dp.time_unix_nano());

			// Convert positive and negative bucket counts
			vector<Value> pos_bucket_counts;
			if (dp.has_positive()) {
				auto &pos = dp.positive();
				pos_bucket_counts.reserve(pos.bucket_counts_size());
				for (auto &v : pos.bucket_counts()) {
					pos_bucket_counts.emplace_back(Value::UBIGINT(v));
				}
			}
			vector<Value> neg_bucket_counts;
			if (dp.has_negative()) {
				auto &neg = dp.negative();
				neg_bucket_counts.reserve(neg.bucket_counts_size());
				for (auto &v : neg.bucket_counts()) {
					neg_bucket_counts.emplace_back(Value::UBIGINT(v));
				}
			}

			app.BeginRow();
			PopulateBaseMetricFields(app, timestamp, metric_ctx);
			app.SetValue(OTLPMetricsExpHistogramSchema::COL_ATTRIBUTES, ConvertAttributesToMap(dp.attributes()));
			app.SetUBigint(OTLPMetricsExpHistogramSchema::COL_COUNT, dp.count());
			if (dp.has_sum()) {
				app.SetDouble(OTLPMetricsExpHistogramSchema::COL_SUM, dp.sum());
			} else {
				app.SetNull(OTLPMetricsExpHistogramSchema::COL_SUM);
			}
			app.SetInteger(OTLPMetricsExpHistogramSchema::COL_SCALE, dp.scale());
			app.SetUBigint(OTLPMetricsExpHistogramSchema::COL_ZERO_COUNT, dp.zero_count());
			app.SetInteger(OTLPMetricsExpHistogramSchema::COL_POSITIVE_OFFSET,
			               dp.has_positive() ? dp.positive().offset() : 0);
			app.SetValue(OTLPMetricsExpHistogramSchema::COL_POSITIVE_BUCKET_COUNTS,
			             Value::LIST(LogicalType::UBIGINT, pos_bucket_counts));
			app.SetInteger(OTLPMetricsExpHistogramSchema::COL_NEGATIVE_OFFSET,
			               dp.has_negative() ? dp.negative().offset() : 0);
			app.SetValue(OTLPMetricsExpHistogramSchema::COL_NEGATIVE_BUCKET_COUNTS,
			             Value::LIST(LogicalType::UBIGINT, neg_bucket_counts));
			if (dp.has_min()) {
				app.SetDouble(OTLPMetricsExpHistogramSchema::COL_MIN, dp.min());
			} else {
				app.SetNull(OTLPMetricsExpHistogramSchema::COL_MIN);
			}
			if (dp.has_max()) {
				app.SetDouble(OTLPMetricsExpHistogramSchema::COL_MAX, dp.max());
			} else {
				app.SetNull(OTLPMetricsExpHistogramSchema::COL_MAX);
			}
			app.CommitRow();
		}
	}

	void ProcessSummaryMetric(const opentelemetry::proto::metrics::v1::Summary &summary,
	                          const MetricContext &metric_ctx) {
		auto buffer = storage_info_->GetBufferForMetric(OTLPMetricType::SUMMARY);
		if (!buffer) {
			return;
		}

		auto app = buffer->GetAppender();
		for (const auto &dp : summary.data_points()) {
			auto timestamp = NanosToTimestamp(dp.time_unix_nano());

			// Convert quantile values and quantiles
			vector<Value> quantile_values;
			vector<Value> quantile_quantiles;
			quantile_values.reserve(dp.quantile_values_size());
			quantile_quantiles.reserve(dp.quantile_values_size());
			for (const auto &qv : dp.quantile_values()) {
				quantile_values.emplace_back(Value::DOUBLE(qv.value()));
				quantile_quantiles.emplace_back(Value::DOUBLE(qv.quantile()));
			}

			app.BeginRow();
			PopulateBaseMetricFields(app, timestamp, metric_ctx);
			app.SetValue(OTLPMetricsSummarySchema::COL_ATTRIBUTES, ConvertAttributesToMap(dp.attributes()));
			app.SetUBigint(OTLPMetricsSummarySchema::COL_COUNT, dp.count());
			app.SetDouble(OTLPMetricsSummarySchema::COL_SUM, dp.sum());
			app.SetValue(OTLPMetricsSummarySchema::COL_QUANTILE_VALUES,
			             Value::LIST(LogicalType::DOUBLE, quantile_values));
			app.SetValue(OTLPMetricsSummarySchema::COL_QUANTILE_QUANTILES,
			             Value::LIST(LogicalType::DOUBLE, quantile_quantiles));
			app.CommitRow();
		}
	}

	shared_ptr<OTLPStorageInfo> storage_info_;
};

} // anonymous namespace

//=============================================================================
// OTLPReceiver - Public interface for starting/stopping gRPC server
//=============================================================================

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
	auto trace_service = std::make_unique<TraceServiceImpl>(storage_info_);
	auto metrics_service = std::make_unique<MetricsServiceImpl>(storage_info_);
	auto logs_service = std::make_unique<LogsServiceImpl>(storage_info_);

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
