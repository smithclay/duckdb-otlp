#pragma once

#ifndef DUCKSPAN_DISABLE_GRPC

#include "duckdb.hpp"
#include "otlp_storage_info.hpp"
#include <grpcpp/grpcpp.h>
#include <thread>
#include <atomic>
#include <memory>

namespace duckdb {

//! OTLP gRPC receiver that implements the 3 OTLP services
//! Follows Brooks principles: minimal, single responsibility, ~150 LOC total
class OTLPReceiver {
public:
	//! Constructor - prepares receiver but doesn't start server
	OTLPReceiver(const string &host, uint16_t port, shared_ptr<OTLPStorageInfo> storage_info);

	//! Destructor - ensures server is stopped
	~OTLPReceiver();

	//! Start the gRPC server in a background thread
	void Start();

	//! Stop the gRPC server and wait for shutdown
	void Stop();

	//! Check if server is running
	bool IsRunning() const {
		return running_.load();
	}

private:
	//! Server thread entry point
	void ServerThread();

	string host_;
	uint16_t port_;
	shared_ptr<OTLPStorageInfo> storage_info_;

	std::unique_ptr<grpc::Server> server_;
	std::thread server_thread_;
	std::atomic<bool> running_;
	std::atomic<bool> shutdown_requested_;
	string startup_error_;   // Stores error message if server fails to start
	std::mutex error_mutex_; // Protects startup_error_
};

} // namespace duckdb

#endif // DUCKSPAN_DISABLE_GRPC
