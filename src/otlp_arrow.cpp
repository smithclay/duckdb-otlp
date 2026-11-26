#include "otlp_arrow.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

unique_ptr<OtlpArrowTable> OtlpArrowTable::ForSignal(ClientContext &context, OtlpSignalType signal_type) {
	ArrowSchemaWrapper schema;
	OtlpStatus status = otlp_get_schema_with_profile(signal_type, OTLP_ARROW_PROFILE_DUCKDB, &schema.arrow_schema);
	if (status != OTLP_OK) {
		throw IOException("Failed to get OTLP schema: %s", otlp_status_message(status));
	}
	auto result = make_uniq<OtlpArrowTable>();
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, schema.arrow_schema);

	result->types = result->arrow_table.GetTypes();
	result->names = result->arrow_table.GetNames();
	for (auto &column : result->arrow_table.GetColumns()) {
		if (column.second->HasExtension()) {
			throw InternalException("OTLP column \"%s\" maps to an Arrow extension type", result->names[column.first]);
		}
	}
	return result;
}

void OtlpArrowTable::CheckBatchSchema(const ArrowSchema &schema) const {
	if (schema.n_children != static_cast<int64_t>(names.size()) || (schema.n_children > 0 && !schema.children)) {
		throw IOException("OTLP Arrow batch has %lld columns, expected %llu", static_cast<int64_t>(schema.n_children),
		                  static_cast<uint64_t>(names.size()));
	}
	for (idx_t i = 0; i < names.size(); i++) {
		auto child = schema.children[i];
		if (!child || !child->name || names[i] != child->name) {
			throw IOException("OTLP Arrow batch column %llu is \"%s\", expected \"%s\"", static_cast<uint64_t>(i),
			                  child && child->name ? child->name : "", names[i]);
		}
	}
}

shared_ptr<ArrowArrayWrapper> OtlpBorrowArrowBatch(const ArrowArray &array) {
	auto wrapper = make_shared_ptr<ArrowArrayWrapper>();
	wrapper->arrow_array = array;
	// A null release callback is what makes the wrapper non-owning.
	wrapper->arrow_array.release = nullptr;
	return wrapper;
}

void OtlpArrowScan(ClientContext &context, const arrow_column_map_t &columns, shared_ptr<ArrowArrayWrapper> batch,
                   const vector<column_t> &column_ids, idx_t offset, idx_t count, DataChunk &output) {
	if (column_ids.size() != output.ColumnCount()) {
		throw InternalException("OTLP Arrow scan: %llu column ids for %llu output columns",
		                        static_cast<uint64_t>(column_ids.size()), static_cast<uint64_t>(output.ColumnCount()));
	}
	auto &array = batch->arrow_array;
	if (array.n_children != static_cast<int64_t>(columns.size())) {
		throw IOException("OTLP Arrow batch has %lld columns, expected %llu", static_cast<int64_t>(array.n_children),
		                  static_cast<uint64_t>(columns.size()));
	}
	for (auto column_id : column_ids) {
		if (column_id >= columns.size() && column_id != COLUMN_IDENTIFIER_ROW_ID) {
			throw InternalException("OTLP Arrow scan: column id %llu out of range", static_cast<uint64_t>(column_id));
		}
	}
	output.SetChildCardinality(count);
	if (column_ids.empty()) {
		return;
	}
	ArrowScanLocalState state(unique_ptr<ArrowArrayWrapper>(), context);
	state.chunk = std::move(batch);
	state.chunk_offset = offset;
	state.column_ids = column_ids;
	// column_ids index the batch's children, which `columns` describes.
	ArrowTableFunction::ArrowToDuckDB(state, columns, output, /*arrow_scan_is_projected=*/false);
}

} // namespace duckdb
