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

//! Build a quoted `[catalog.]schema.table` reference. An empty catalog means the connection's
//! default catalog; schema is assumed non-empty (the live-ingest catalog modes always set one).
inline string QualifiedTable(const string &catalog_name, const string &schema_name, const string &table_name) {
	string qualified;
	if (!catalog_name.empty()) {
		qualified += QuoteIdentifier(catalog_name) + ".";
	}
	qualified += QuoteIdentifier(schema_name) + "." + QuoteIdentifier(table_name);
	return qualified;
}

//! The `parquet` mode's on-disk contract, in one place.
//!
//! The seal path writes `<root>/<table>/year=/month=/day=/` (otlp_server.cpp) and two readers
//! have to match it exactly: the view the seal creates after each successful export, and the
//! one the CLI creates for a dataset whose control database does not have it. Hand-copying the
//! glob depth and the read_parquet options is how those two silently stopped agreeing.
inline string ParquetDatasetDirectory(const string &root, const string &table_name) {
	if (root.empty()) {
		return "";
	}
	auto value = root;
	while (!value.empty() && value[value.size() - 1] == '/') {
		value = value.substr(0, value.size() - 1);
	}
	return value + "/" + table_name;
}

inline string ParquetDatasetGlob(const string &root, const string &table_name) {
	return ParquetDatasetDirectory(root, table_name) + "/**/*.parquet";
}

//! `SELECT * FROM read_parquet(<glob>, ...)` over one signal's files.
//!
//! hive_partitioning=false is load-bearing: the COPY sets WRITE_PARTITION_COLUMNS false, so
//! year/month/day exist only in the path, and letting read_parquet infer them would append
//! three columns that the same signal's table in a catalog mode does not have.
//! union_by_name=true so a schema that gained a column between seals still reads as one
//! relation instead of failing on the first mismatched file.
inline string ParquetDatasetSelect(const string &root, const string &table_name) {
	return "SELECT * FROM read_parquet(" + SqlQuote(ParquetDatasetGlob(root, table_name)) +
	       ", hive_partitioning=false, union_by_name=true)";
}

} // namespace duckdb
