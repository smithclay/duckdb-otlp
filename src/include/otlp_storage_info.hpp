#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "ring_buffer.hpp"

namespace duckdb {

class OTLPReceiver;

//! Information about an attached OTLP database
struct OTLPStorageInfo : public StorageExtensionInfo {
	string host;
	uint16_t port;
	string schema_name; // Name of the attached schema (e.g., "live")

	// Ring buffers for each signal type (virtual table backing store)
	shared_ptr<RingBuffer> traces_buffer;
	shared_ptr<RingBuffer> metrics_buffer;
	shared_ptr<RingBuffer> logs_buffer;

	// gRPC receiver
	unique_ptr<OTLPReceiver> receiver;

	OTLPStorageInfo(const string &host_p, uint16_t port_p, idx_t buffer_capacity = 10000)
	    : host(host_p), port(port_p), schema_name("") {
		// Create ring buffers for each signal type
		traces_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		metrics_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		logs_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
	}

	~OTLPStorageInfo(); // Defined in otlp_storage_extension.cpp

	//! Get ring buffer by table name
	shared_ptr<RingBuffer> GetBuffer(const string &table_name) {
		if (table_name == "traces")
			return traces_buffer;
		if (table_name == "metrics")
			return metrics_buffer;
		if (table_name == "logs")
			return logs_buffer;
		return nullptr;
	}
};

} // namespace duckdb
