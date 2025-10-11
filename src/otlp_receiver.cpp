#include "otlp_receiver.hpp"

// Generated gRPC service stubs
#include "opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.grpc.pb.h"
#include "opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h"

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
	GenericOTLPService(shared_ptr<OTLPStorageInfo> storage_info, const string &table_name)
	    : storage_info_(storage_info), table_name_(table_name) {
	}

	grpc::Status Export(grpc::ServerContext *context, const TRequest *request, TResponse *response) override {
		try {
			// Convert protobuf → JSON
			string json_data;
			google::protobuf::util::JsonPrintOptions options;
			options.add_whitespace = false;
			options.preserve_proto_field_names = true;

			auto status = google::protobuf::util::MessageToJsonString(*request, &json_data, options);
			if (!status.ok()) {
				return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to convert to JSON");
			}

			// Insert into ring buffer (thread-safe)
			auto buffer = storage_info_->GetBuffer(table_name_);
			if (!buffer) {
				return grpc::Status(grpc::StatusCode::INTERNAL, "Ring buffer not found for " + table_name_);
			}

			auto timestamp = timestamp_t(std::chrono::duration_cast<std::chrono::microseconds>(
			                                 std::chrono::system_clock::now().time_since_epoch())
			                                 .count());

			// Insert into ring buffer (FIFO eviction when full)
			buffer->Insert(timestamp, "{}", json_data); // Empty resource for now

			return grpc::Status::OK;
		} catch (std::exception &e) {
			return grpc::Status(grpc::StatusCode::INTERNAL, string("Error: ") + e.what());
		}
	}

private:
	shared_ptr<OTLPStorageInfo> storage_info_;
	string table_name_;
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
	server_thread_ = std::thread(&OTLPReceiver::ServerThread, this);

	// Wait for server to start
	while (!running_.load() && !shutdown_requested_.load()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
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
	using namespace opentelemetry::proto::collector;

	// Create service instances that insert directly into DuckDB tables
	auto trace_service =
	    std::make_unique<GenericOTLPService<trace::v1::TraceService, trace::v1::ExportTraceServiceRequest,
	                                        trace::v1::ExportTraceServiceResponse>>(storage_info_, "traces");

	auto metrics_service =
	    std::make_unique<GenericOTLPService<metrics::v1::MetricsService, metrics::v1::ExportMetricsServiceRequest,
	                                        metrics::v1::ExportMetricsServiceResponse>>(storage_info_, "metrics");

	auto logs_service =
	    std::make_unique<GenericOTLPService<logs::v1::LogsService, logs::v1::ExportLogsServiceRequest,
	                                        logs::v1::ExportLogsServiceResponse>>(storage_info_, "logs");

	// Build server
	grpc::ServerBuilder builder;
	string server_address = host_ + ":" + std::to_string(port_);
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(trace_service.get());
	builder.RegisterService(metrics_service.get());
	builder.RegisterService(logs_service.get());

	// Start server
	server_ = builder.BuildAndStart();
	if (!server_) {
		shutdown_requested_.store(true);
		return;
	}

	running_.store(true);

	// Wait for shutdown
	server_->Wait();

	running_.store(false);
}

} // namespace duckdb
