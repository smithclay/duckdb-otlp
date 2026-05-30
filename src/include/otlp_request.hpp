#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class OtlpRequestKind : uint8_t { LOGS, TRACES, METRICS };

const char *OtlpRequestKindName(OtlpRequestKind kind);
bool OtlpRequestKindFromName(const string &name, OtlpRequestKind &kind);

} // namespace duckdb
