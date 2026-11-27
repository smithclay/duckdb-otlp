#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterOtlpControlFunctions(ExtensionLoader &loader);

//! Tuning options shared by otlp_serve and the daemon's DUCKDB_OTLP_* settings.
const vector<std::pair<string, LogicalType>> &OtlpServeTuningOptions();

} // namespace duckdb
