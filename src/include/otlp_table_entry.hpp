#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "ring_buffer.hpp"

namespace duckdb {

class OTLPTableEntry : public TableCatalogEntry {
public:
	OTLPTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, shared_ptr<RingBuffer> buffer);

	//! Get the table function used for scanning this table
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	//! Get statistics for a specific column
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	//! Get storage information
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	shared_ptr<RingBuffer> ring_buffer_;
};

} // namespace duckdb
