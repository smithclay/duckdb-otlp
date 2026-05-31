#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "otlp2records.h"

namespace duckdb {

struct OtlpArrowSchemaOptions {
	bool timestamp_ns_as_timestamp = false;
	bool unsigned_as_signed = true;
};

void GetArrowSchemaColumns(const ArrowSchema &schema, vector<LogicalType> &return_types, vector<string> &names,
                           const OtlpArrowSchemaOptions &options = OtlpArrowSchemaOptions());

void CopyArrowToDuckDB(const ArrowArray &array, const ArrowSchema &schema, Vector &output, idx_t count);

void CopyArrowStructToDataChunk(const ArrowArray &array, const ArrowSchema &schema, DataChunk &output, idx_t offset,
                                idx_t count);

// Projection-aware variant: `column_ids[out_col]` selects the source Arrow child column to copy into
// output column `out_col`. The non-projected `CopyArrowStructToDataChunk` above delegates here with
// identity column_ids. This is the single per-column dispatch routine shared by the scan and server.
void CopyProjectedArrowStructToDataChunk(const ArrowArray &array, const ArrowSchema &schema, DataChunk &output,
                                         const vector<column_t> &column_ids, idx_t offset, idx_t count);

} // namespace duckdb
