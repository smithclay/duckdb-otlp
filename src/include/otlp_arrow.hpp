#pragma once

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

#include "otlp2records.h"

namespace duckdb {

// Release one metric batch's Arrow array+schema and mark it absent, so a later release pass
// over the four shapes cannot double-free it. Both metric callers (the read scan and the
// ingest server) keep one shape and release the rest; this is the shared teardown primitive.
inline void ReleaseOtlpArrowBatch(OtlpArrowBatch &batch) {
	if (!batch.present) {
		return;
	}
	if (batch.array.release) {
		batch.array.release(&batch.array);
	}
	if (batch.schema.release) {
		batch.schema.release(&batch.schema);
	}
	batch.present = 0;
}

//! The batch holding metric shape `signal_type` (OTLP_SIGNAL_METRICS_GAUGE and after).
inline OtlpArrowBatch &OtlpMetricShape(OtlpMetricsArrowBatches &batches, OtlpSignalType signal_type) {
	OtlpArrowBatch *shapes[] = {&batches.gauge, &batches.sum, &batches.histogram, &batches.exp_histogram};
	return *shapes[signal_type - OTLP_SIGNAL_METRICS_GAUGE];
}

//! Release every metric shape still present.
inline void ReleaseOtlpMetricBatches(OtlpMetricsArrowBatches &batches) {
	for (auto *batch : {&batches.gauge, &batches.sum, &batches.histogram, &batches.exp_histogram}) {
		ReleaseOtlpArrowBatch(*batch);
	}
}

//! One signal's columns as DuckDB's stock Arrow importer maps them. Every otlp2records call this
//! extension makes requests OTLP_ARROW_PROFILE_DUCKDB, whose Arrow types the stock importer already
//! Every signal the extension reads and ingests, in OtlpSignalType order (OTLP_SIGNAL_LOGS = 0 ...
//! OTLP_SIGNAL_METRICS_EXP_HISTOGRAM = 5). The name is also the ingest table's, and the file reader
//! is read_<name> for OTLP and read_otap_<name minus "otlp_"> for OTAP.
constexpr OtlpSignalType OTLP_SIGNALS[] = {OTLP_SIGNAL_LOGS,          OTLP_SIGNAL_TRACES,
                                           OTLP_SIGNAL_METRICS_GAUGE, OTLP_SIGNAL_METRICS_SUM,
                                           OTLP_SIGNAL_METRICS_HISTOGRAM, OTLP_SIGNAL_METRICS_EXP_HISTOGRAM};
constexpr const char *OTLP_SIGNAL_TABLES[] = {"otlp_logs",          "otlp_traces",
                                              "otlp_metrics_gauge", "otlp_metrics_sum",
                                              "otlp_metrics_histogram", "otlp_metrics_exp_histogram"};

//! maps to the extension's column types: hex VARCHAR ids, BIGINT nanosecond duration, signed counters.
struct OtlpArrowTable {
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;

	//! Resolves the signal's DuckDb-profile schema. Throws if any column would need an Arrow
	//! extension type: conversion consults the ClientContext only for those, which is what lets the
	//! ingest server share one context across its worker threads.
	static unique_ptr<OtlpArrowTable> ForSignal(ClientContext &context, OtlpSignalType signal_type);

	//! Throws unless `schema` has this table's column names, in order. Per-batch schemas are otherwise
	//! ignored: conversion uses the types resolved once by ForSignal.
	void CheckBatchSchema(const ArrowSchema &schema) const;
};

//! Takes ownership of `array` (its release callback is cleared); the wrapper releases it once no
//! vector produced from it is alive.
shared_ptr<ArrowArrayWrapper> OtlpOwnArrowBatch(ArrowArray &array);
//! Wraps `array` without taking ownership. The caller keeps it alive while any vector produced from
//! it is in use (e.g. until those rows have been appended into a ColumnDataCollection).
shared_ptr<ArrowArrayWrapper> OtlpBorrowArrowBatch(const ArrowArray &array);

//! Convert rows [offset, offset + count) of `batch` into `output`. `column_ids[i]` names the Arrow
//! column that fills output column i. Zero-copy where DuckDB's importer allows; the vectors keep
//! `batch` alive.
void OtlpArrowScan(ClientContext &context, const OtlpArrowTable &table, shared_ptr<ArrowArrayWrapper> batch,
                   const vector<column_t> &column_ids, idx_t offset, idx_t count, DataChunk &output);

} // namespace duckdb
