#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "buffer/columnar_ring_buffer.hpp"
#include "schema/otlp_traces_schema.hpp"
#include "schema/otlp_logs_schema.hpp"
#include "schema/otlp_metrics_schemas.hpp"
#include "schema/otlp_metrics_union_schema.hpp"
#include "schema/otlp_types.hpp"

namespace duckdb {

class OTLPReceiver;

//! Information about an attached OTLP database
struct OTLPStorageInfo : public StorageExtensionInfo {
	string host;
	uint16_t port;
	string schema_name; // Name of the attached schema (e.g., "live")

	// Columnar buffers for each table type (7 total: traces, logs, 5 metrics)
	shared_ptr<ColumnarRingBuffer> traces_buffer;
	shared_ptr<ColumnarRingBuffer> logs_buffer;
	shared_ptr<ColumnarRingBuffer> metrics_gauge_buffer;
	shared_ptr<ColumnarRingBuffer> metrics_sum_buffer;
	shared_ptr<ColumnarRingBuffer> metrics_histogram_buffer;
	shared_ptr<ColumnarRingBuffer> metrics_exp_histogram_buffer;
	shared_ptr<ColumnarRingBuffer> metrics_summary_buffer;

	// Pre-flattened metrics union buffer (27-column union schema, columnar)
	shared_ptr<ColumnarRingBuffer> metrics_union_buffer;

	// gRPC receiver
	unique_ptr<OTLPReceiver> receiver;

	OTLPStorageInfo(const string &host_p, uint16_t port_p, idx_t buffer_capacity = 10000)
	    : host(host_p), port(port_p), schema_name("") {
		// Create columnar buffers for all 7 table types
		traces_buffer =
		    make_shared_ptr<ColumnarRingBuffer>(OTLPTracesSchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256,
		                                        OTLPTracesSchema::COL_SERVICE_NAME, DConstants::INVALID_INDEX);
		logs_buffer = make_shared_ptr<ColumnarRingBuffer>(OTLPLogsSchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256,
		                                                  OTLPLogsSchema::COL_SERVICE_NAME, DConstants::INVALID_INDEX);
		metrics_gauge_buffer = make_shared_ptr<ColumnarRingBuffer>(
		    OTLPMetricsGaugeSchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256,
		    OTLPMetricsBaseSchema::COL_SERVICE_NAME, OTLPMetricsBaseSchema::COL_METRIC_NAME);
		metrics_sum_buffer = make_shared_ptr<ColumnarRingBuffer>(
		    OTLPMetricsSumSchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256, OTLPMetricsBaseSchema::COL_SERVICE_NAME,
		    OTLPMetricsBaseSchema::COL_METRIC_NAME);
		metrics_histogram_buffer = make_shared_ptr<ColumnarRingBuffer>(
		    OTLPMetricsHistogramSchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256,
		    OTLPMetricsBaseSchema::COL_SERVICE_NAME, OTLPMetricsBaseSchema::COL_METRIC_NAME);
		metrics_exp_histogram_buffer = make_shared_ptr<ColumnarRingBuffer>(
		    OTLPMetricsExpHistogramSchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256,
		    OTLPMetricsBaseSchema::COL_SERVICE_NAME, OTLPMetricsBaseSchema::COL_METRIC_NAME);
		metrics_summary_buffer = make_shared_ptr<ColumnarRingBuffer>(
		    OTLPMetricsSummarySchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256,
		    OTLPMetricsBaseSchema::COL_SERVICE_NAME, OTLPMetricsBaseSchema::COL_METRIC_NAME);
		metrics_union_buffer = make_shared_ptr<ColumnarRingBuffer>(
		    OTLPMetricsUnionSchema::GetColumnTypes(), STANDARD_VECTOR_SIZE, 256,
		    OTLPMetricsUnionSchema::COL_SERVICE_NAME, OTLPMetricsUnionSchema::COL_METRIC_NAME);
	}

	~OTLPStorageInfo() override; // Defined in otlp_storage_extension.cpp

	//! Get columnar buffer by table type enum (7-table schema)
	shared_ptr<ColumnarRingBuffer> GetBuffer(OTLPTableType type) {
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

	//! Get columnar buffer for specific metric type (routes to correct buffer)
	shared_ptr<ColumnarRingBuffer> GetBufferForMetric(OTLPMetricType metric_type) {
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

	//! Get columnar buffer by table name string (7-table schema)
	shared_ptr<ColumnarRingBuffer> GetBuffer(const string &table_name) {
		auto table_type = StringToTableType(table_name);
		if (table_type) {
			return GetBuffer(*table_type);
		}
		return nullptr;
	}

	//! Get all metric buffers (for union view) - columnar
	vector<shared_ptr<ColumnarRingBuffer>> GetAllMetricBuffers() {
		return {metrics_gauge_buffer, metrics_sum_buffer, metrics_histogram_buffer, metrics_exp_histogram_buffer,
		        metrics_summary_buffer};
	}

	//! Get pre-flattened metrics union buffer
	shared_ptr<ColumnarRingBuffer> GetMetricsUnionBuffer() {
		return metrics_union_buffer;
	}
};

} // namespace duckdb
