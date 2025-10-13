#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "ring_buffer.hpp"
#include "otlp_types.hpp"

namespace duckdb {

class OTLPReceiver;

//! Information about an attached OTLP database
struct OTLPStorageInfo : public StorageExtensionInfo {
	string host;
	uint16_t port;
	string schema_name; // Name of the attached schema (e.g., "live")

	// Ring buffers for each table type (3 total: traces, logs, metrics union)
	shared_ptr<RingBuffer> traces_buffer;
	shared_ptr<RingBuffer> logs_buffer;
	shared_ptr<RingBuffer> metrics_buffer; // Single buffer for all metrics (union schema)

	// gRPC receiver
	unique_ptr<OTLPReceiver> receiver;

	OTLPStorageInfo(const string &host_p, uint16_t port_p, idx_t buffer_capacity = 10000)
	    : host(host_p), port(port_p), schema_name("") {
		// Create ring buffers for all 3 table types
		traces_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		logs_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		metrics_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
	}

	~OTLPStorageInfo() override; // Defined in otlp_storage_extension.cpp

	//! Get ring buffer by table type enum (3-table union schema)
	shared_ptr<RingBuffer> GetBuffer(OTLPTableType type) {
		switch (type) {
		case OTLPTableType::TRACES:
			return traces_buffer;
		case OTLPTableType::LOGS:
			return logs_buffer;
		case OTLPTableType::METRICS:
			return metrics_buffer;
		default:
			return nullptr;
		}
	}

	//! Get ring buffer by signal type enum (old API - maps to appropriate buffer)
	shared_ptr<RingBuffer> GetBuffer(OTLPSignalType type) {
		switch (type) {
		case OTLPSignalType::TRACES:
			return traces_buffer;
		case OTLPSignalType::METRICS:
			return metrics_buffer;
		case OTLPSignalType::LOGS:
			return logs_buffer;
		default:
			return nullptr;
		}
	}

	//! Get ring buffer for metrics (all types go to same union table)
	shared_ptr<RingBuffer> GetBufferForMetric(OTLPMetricType metric_type) {
		(void)metric_type; // Unused - all metrics go to same buffer
		return metrics_buffer;
	}

	//! Get ring buffer by table name string (3-table union schema)
	shared_ptr<RingBuffer> GetBuffer(const string &table_name) {
		auto table_type = StringToTableType(table_name);
		if (table_type) {
			return GetBuffer(*table_type);
		}
		return nullptr;
	}
};

} // namespace duckdb
