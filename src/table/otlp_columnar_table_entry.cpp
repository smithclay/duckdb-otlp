#include "table/otlp_columnar_table_entry.hpp"
#include "table/otlp_columnar_scan.hpp"

namespace duckdb {

OTLPColumnarTableEntry::OTLPColumnarTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                               shared_ptr<ColumnarRingBuffer> buffer)
    : TableCatalogEntry(catalog, schema, info), buffer_(std::move(buffer)) {
}

TableFunction OTLPColumnarTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto bind = make_uniq<OTLPColumnarScanBindData>();
	bind->buffer = buffer_;

	for (auto &column : columns.Logical()) {
		bind->column_names.push_back(column.Name());
		bind->column_types.push_back(column.Type());
	}

	bind_data = std::move(bind);

	TableFunction scan_func("otlp_columnar_scan", {}, OTLPColumnarScanFunction, nullptr, OTLPColumnarScanInitGlobal);
	scan_func.init_local = OTLPColumnarScanInitLocal;
	scan_func.projection_pushdown = true;
	scan_func.filter_pushdown = true;
	scan_func.filter_prune = true;
	return scan_func;
}

unique_ptr<BaseStatistics> OTLPColumnarTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo OTLPColumnarTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = buffer_ ? buffer_->Size() : 0;
	return info;
}

} // namespace duckdb
