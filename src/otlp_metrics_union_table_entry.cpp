#include "otlp_metrics_union_table_entry.hpp"
#include "otlp_metrics_union_schema.hpp"
#include "otlp_metrics_union_scan.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

OTLPMetricsUnionTableEntry::OTLPMetricsUnionTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                       CreateTableInfo &info, vector<shared_ptr<RingBuffer>> buffers)
    : TableCatalogEntry(catalog, schema, info), metric_buffers_(buffers) {
}

TableFunction OTLPMetricsUnionTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Create bind data with all 5 metric buffers
	auto otlp_bind_data = make_uniq<OTLPMetricsUnionScanBindData>();
	otlp_bind_data->buffers = metric_buffers_;
	otlp_bind_data->column_names = OTLPMetricsUnionSchema::GetColumnNames();
	otlp_bind_data->column_types = OTLPMetricsUnionSchema::GetColumnTypes();

	bind_data = std::move(otlp_bind_data);

	// Create table function with union scan
	TableFunction function("otlp_metrics_union_scan", {}, OTLPMetricsUnionScanFunction, nullptr,
	                       OTLPMetricsUnionScanInitGlobal);
	return function;
}

unique_ptr<BaseStatistics> OTLPMetricsUnionTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	// No statistics available
	return nullptr;
}

TableStorageInfo OTLPMetricsUnionTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;

	// Sum cardinality from all 5 buffers
	idx_t total_rows = 0;
	for (auto &buffer : metric_buffers_) {
		total_rows += buffer->Size();
	}
	result.cardinality = total_rows;

	return result;
}

} // namespace duckdb
