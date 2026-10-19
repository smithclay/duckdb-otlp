#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "otlp2records.h"

namespace duckdb {

struct OtlpArrowSchemaOptions {
	bool timestamp_ns_as_timestamp = false;
	bool unsigned_as_signed = true;
	//! Opt-in: map the attribute-bag columns to DuckDB VARIANT instead of VARCHAR. The Rust
	//! backend has no VARIANT encoder -- it emits a JSON object per bag either way -- so this
	//! only moves the boundary: the JSON text is converted once per chunk by DuckDB's core
	//! JSON->VARIANT cast (see CopyArrowToDuckDB). Everything else in the schema is unchanged,
	//! including the JSON-array columns (events_json, links_json, exemplars_json) and `body`.
	bool attributes_as_variant = false;
};

//! The columns holding a JSON *object* of OTLP attributes: resource_attributes,
//! scope_attributes, log_attributes, span_attributes, metric_attributes. Matched on the
//! `_attributes` suffix so a bag added to a Rust schema later is covered without a second
//! list to keep in sync; `dropped_attributes_count` is a counter and does not match.
bool OtlpIsAttributeBagColumn(const string &name);

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

void GetArrowSchemaColumns(const ArrowSchema &schema, vector<LogicalType> &return_types, vector<string> &names,
                           const OtlpArrowSchemaOptions &options = OtlpArrowSchemaOptions());

void CopyArrowToDuckDB(const ArrowArray &array, const ArrowSchema &schema, Vector &output, idx_t count);

// Copy an Arrow struct array's children into a DuckDB DataChunk: `column_ids[out_col]` selects the
// source Arrow child column for output column `out_col`. The read scan passes DuckDB's projection
// list; the ingest server passes each signal buffer's precomputed identity projection. This is the
// single per-column dispatch routine shared by both.
void CopyProjectedArrowStructToDataChunk(const ArrowArray &array, const ArrowSchema &schema, DataChunk &output,
                                         const vector<column_t> &column_ids, idx_t offset, idx_t count);

} // namespace duckdb
