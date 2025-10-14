#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "ring_buffer.hpp"

namespace duckdb {

//! Table entry for the otel_metrics_union virtual view
//! This view combines all 5 metric type tables into a single union schema
class OTLPMetricsUnionTableEntry : public TableCatalogEntry {
public:
	OTLPMetricsUnionTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                           vector<shared_ptr<RingBuffer>> buffers);

	//! Get the table function used for scanning this table
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	//! Get statistics for a specific column
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	//! Get storage information
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	vector<shared_ptr<RingBuffer>> metric_buffers_; // All 5 metric buffers
};

} // namespace duckdb
