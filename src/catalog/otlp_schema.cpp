#include "catalog/otlp_schema.hpp"

namespace duckdb {

vector<LogicalType> OTLPSchema::GetTypes() {
	return {
	    LogicalType::TIMESTAMP, // timestamp - event occurrence time (microsecond precision)
	    LogicalType::JSON(),    // resource - service/host metadata
	    LogicalType::JSON()     // data - signal-specific payload
	};
}

vector<string> OTLPSchema::GetNames() {
	return {"timestamp", "resource", "data"};
}

vector<ColumnDefinition> OTLPSchema::GetColumns() {
	auto types = GetTypes();
	auto names = GetNames();

	vector<ColumnDefinition> columns;
	for (idx_t i = 0; i < types.size(); i++) {
		columns.emplace_back(names[i], types[i]);
	}
	return columns;
}

} // namespace duckdb
