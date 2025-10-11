#include "otlp_table_entry.hpp"
#include "otlp_scan.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

OTLPTableEntry::OTLPTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                 shared_ptr<RingBuffer> buffer)
    : TableCatalogEntry(catalog, schema, info), ring_buffer_(buffer) {
}

TableFunction OTLPTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Create bind data with ring buffer pointer
	auto otlp_bind_data = make_uniq<OTLPScanBindData>();
	otlp_bind_data->buffer = ring_buffer_;
	otlp_bind_data->column_names = {"timestamp", "resource", "data"};
	otlp_bind_data->column_types = {LogicalType::TIMESTAMP, LogicalType::JSON(), LogicalType::JSON()};

	bind_data = std::move(otlp_bind_data);

	// Create table function - the bind function won't be called in this flow
	TableFunction scan_func("otlp_scan", {}, OTLPScanFunction, nullptr, OTLPScanInitGlobal);
	return scan_func;
}

unique_ptr<BaseStatistics> OTLPTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	// Return basic statistics - just return unknown stats
	auto stats = BaseStatistics::CreateUnknown(columns.GetColumn(LogicalIndex(column_id)).Type());
	return make_uniq<BaseStatistics>(std::move(stats));
}

TableStorageInfo OTLPTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = ring_buffer_ ? ring_buffer_->Size() : 0;
	return info;
}

} // namespace duckdb
