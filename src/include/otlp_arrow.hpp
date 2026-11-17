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

//! One signal's columns as DuckDB's stock Arrow importer maps them. Every otlp2records call this
//! extension makes requests OTLP_ARROW_PROFILE_DUCKDB, whose Arrow types the stock importer already
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
