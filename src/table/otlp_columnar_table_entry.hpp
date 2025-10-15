#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "buffer/columnar_ring_buffer.hpp"

namespace duckdb {

class OTLPColumnarTableEntry : public TableCatalogEntry {
public:
	OTLPColumnarTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                       shared_ptr<ColumnarRingBuffer> buffer);

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	shared_ptr<ColumnarRingBuffer> buffer_;
};

} // namespace duckdb
