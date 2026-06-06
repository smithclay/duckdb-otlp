#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// SQL literal/identifier quoting shared by the OTLP ingest server (otlp_server.cpp) and the
// native daemon's mode-setup SQL generation (server_config.cpp, main.cpp). SQL escaping is an
// injection surface, so it has exactly one definition rather than a hand-copied one per TU.
inline string SqlEscape(const string &value) {
	return StringUtil::Replace(value, "'", "''");
}

inline string SqlQuote(const string &value) {
	return "'" + SqlEscape(value) + "'";
}

inline string QuoteIdentifier(const string &identifier) {
	return "\"" + StringUtil::Replace(identifier, "\"", "\"\"") + "\"";
}

} // namespace duckdb
