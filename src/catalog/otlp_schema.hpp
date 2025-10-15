#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! OTLPSchema defines the unified schema for all telemetry signals (traces, metrics, logs)
//! Schema: (timestamp TIMESTAMP, resource JSON, data JSON)
class OTLPSchema {
public:
	//! Get column types for OTLP schema
	static vector<LogicalType> GetTypes();

	//! Get column names for OTLP schema
	static vector<string> GetNames();

	//! Get full schema definition
	static vector<ColumnDefinition> GetColumns();

	//! Column indices for efficient access
	static constexpr idx_t TIMESTAMP_COL = 0;
	static constexpr idx_t RESOURCE_COL = 1;
	static constexpr idx_t DATA_COL = 2;
};

} // namespace duckdb
