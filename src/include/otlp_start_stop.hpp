#pragma once

#include "otlp_function_docs.hpp"

namespace duckdb {

//! Tuning options shared by otlp_serve and the daemon's DUCKDB_OTLP_* settings.
const vector<std::pair<string, LogicalType>> &OtlpServeTuningOptions();

class OtlpServeFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

//! otlp_stop(uri) / otlp_stop(): stop one server or all of them, sealing buffered rows first.
class OtlpStopFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

class OtlpServerListFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

class OtlpFlushFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

} // namespace duckdb
