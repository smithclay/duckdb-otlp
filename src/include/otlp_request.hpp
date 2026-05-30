#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class OtlpRequestKind : uint8_t { LOGS, TRACES, METRICS };

} // namespace duckdb
