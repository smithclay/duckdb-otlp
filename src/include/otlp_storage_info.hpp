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

	// Ring buffers for each table type (7 total: traces, logs, 5 metrics)
	shared_ptr<RingBuffer> traces_buffer;
	shared_ptr<RingBuffer> logs_buffer;
	shared_ptr<RingBuffer> metrics_gauge_buffer;
	shared_ptr<RingBuffer> metrics_sum_buffer;
	shared_ptr<RingBuffer> metrics_histogram_buffer;
	shared_ptr<RingBuffer> metrics_exp_histogram_buffer;
	shared_ptr<RingBuffer> metrics_summary_buffer;

	// gRPC receiver
	unique_ptr<OTLPReceiver> receiver;

	OTLPStorageInfo(const string &host_p, uint16_t port_p, idx_t buffer_capacity = 10000)
	    : host(host_p), port(port_p), schema_name("") {
		// Create ring buffers for all 7 table types
		traces_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		logs_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		metrics_gauge_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		metrics_sum_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		metrics_histogram_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		metrics_exp_histogram_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
		metrics_summary_buffer = make_shared_ptr<RingBuffer>(buffer_capacity);
	}

	~OTLPStorageInfo() override; // Defined in otlp_storage_extension.cpp

	//! Get ring buffer by table type enum (7-table schema)
	shared_ptr<RingBuffer> GetBuffer(OTLPTableType type) {
		switch (type) {
		case OTLPTableType::TRACES:
			return traces_buffer;
		case OTLPTableType::LOGS:
			return logs_buffer;
		case OTLPTableType::METRICS_GAUGE:
			return metrics_gauge_buffer;
		case OTLPTableType::METRICS_SUM:
			return metrics_sum_buffer;
		case OTLPTableType::METRICS_HISTOGRAM:
			return metrics_histogram_buffer;
		case OTLPTableType::METRICS_EXP_HISTOGRAM:
			return metrics_exp_histogram_buffer;
		case OTLPTableType::METRICS_SUMMARY:
			return metrics_summary_buffer;
		default:
			return nullptr;
		}
	}

	//! Get ring buffer for specific metric type (routes to correct buffer)
	shared_ptr<RingBuffer> GetBufferForMetric(OTLPMetricType metric_type) {
		switch (metric_type) {
		case OTLPMetricType::GAUGE:
			return metrics_gauge_buffer;
		case OTLPMetricType::SUM:
			return metrics_sum_buffer;
		case OTLPMetricType::HISTOGRAM:
			return metrics_histogram_buffer;
		case OTLPMetricType::EXPONENTIAL_HISTOGRAM:
			return metrics_exp_histogram_buffer;
		case OTLPMetricType::SUMMARY:
			return metrics_summary_buffer;
		default:
			return nullptr;
		}
	}

	//! Get ring buffer by table name string (7-table schema)
	shared_ptr<RingBuffer> GetBuffer(const string &table_name) {
		auto table_type = StringToTableType(table_name);
		if (table_type) {
			return GetBuffer(*table_type);
		}
		return nullptr;
	}
};

} // namespace duckdb
