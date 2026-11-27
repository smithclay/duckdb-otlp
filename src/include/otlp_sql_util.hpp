#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"

#include <utility>

namespace duckdb {

//! Call from a catch handler. DuckDB exceptions store their message in a JSON envelope.
inline string CurrentExceptionMessage(const char *operation = nullptr) {
	try {
		throw;
	} catch (const Exception &ex) {
		return ErrorData(ex).RawMessage();
	} catch (const std::exception &ex) {
		return ex.what();
	} catch (...) {
		string message = "unknown (non-std) exception";
		return operation ? message + " during " + operation : message;
	}
}

//! Execute SQL text or an already parsed statement.
template <class Query>
unique_ptr<QueryResult> RunSQL(Connection &con, Query &&query, const string &error_prefix = "") {
	auto result = con.Query(std::forward<Query>(query));
	if (!result || result->HasError()) {
		throw IOException("%s%s", error_prefix, result ? result->GetError() : "DuckDB query failed");
	}
	return result;
}

// The one definition of SQL quoting for the extension and the daemon (an injection surface).
inline string SqlEscape(const string &value) {
	return StringUtil::Replace(value, "'", "''");
}

inline string SqlQuote(const string &value) {
	return "'" + SqlEscape(value) + "'";
}

inline string QuoteIdentifier(const string &identifier) {
	return "\"" + StringUtil::Replace(identifier, "\"", "\"\"") + "\"";
}

//! An empty catalog means the connection's default catalog; schema must be non-empty.
inline string QualifiedTable(const string &catalog_name, const string &schema_name, const string &table_name) {
	string qualified;
	if (!catalog_name.empty()) {
		qualified += QuoteIdentifier(catalog_name) + ".";
	}
	qualified += QuoteIdentifier(schema_name) + "." + QuoteIdentifier(table_name);
	return qualified;
}

//! The `parquet` mode's on-disk layout, `<root>/<table>/year=/month=/day=/`: the seal writes it and
//! every reader must match it, so the glob and read_parquet options live only here.
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
	auto directory = ParquetDatasetDirectory(root, table_name);
	// An empty root must not become "/**/*.parquet", a glob of the whole filesystem.
	return directory.empty() ? "" : directory + "/**/*.parquet";
}

//! hive_partitioning=false: year/month/day live only in the path (WRITE_PARTITION_COLUMNS false) and
//! must not become extra columns. union_by_name=true: a schema that gained a column still reads.
inline string ParquetDatasetSelect(const string &root, const string &table_name) {
	auto glob = ParquetDatasetGlob(root, table_name);
	if (glob.empty()) {
		return "";
	}
	return "SELECT * FROM read_parquet(" + SqlQuote(glob) + ", hive_partitioning=false, union_by_name=true)";
}

} // namespace duckdb
