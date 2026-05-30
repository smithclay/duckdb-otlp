#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "otlp2records.h"

namespace duckdb {

LogicalType ArrowFormatToDuckDBType(const ArrowSchema &schema);

void GetArrowSchemaColumns(const ArrowSchema &schema, vector<LogicalType> &return_types, vector<string> &names);

void CopyArrowToDuckDB(const ArrowArray &array, const ArrowSchema &schema, Vector &output, idx_t count);

void CopyArrowStructToDataChunk(const ArrowArray &array, const ArrowSchema &schema, DataChunk &output, idx_t offset,
                                idx_t count);

} // namespace duckdb
