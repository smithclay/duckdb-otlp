#include "otlp_column_promote.hpp"

#include "otlp_arrow.hpp"
#include "otlp_sql_util.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//! `attr_<sanitized>` body: lowercase-preserving, non-[A-Za-z0-9_] -> '_'.
string Sanitize(const string &key) {
	string out;
	out.reserve(key.size());
	for (char c : key) {
		bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
		out += ok ? c : '_';
	}
	return out;
}

//! JSONPath literal `'$."<key>"'`, escaping the inner quote/backslash so dotted keys stay one member.
string JsonPathLiteral(const string &key) {
	string escaped;
	escaped.reserve(key.size());
	for (char c : key) {
		if (c == '\\' || c == '"') {
			escaped += '\\';
		}
		escaped += c;
	}
	return SqlQuote("$.\"" + escaped + "\"");
}

} // namespace

string OtlpPromoteColumns(Connection &con, const OtlpPromoteConfig &config, const string &catalog, const string &schema,
                          idx_t &promoted_columns, string &failure) {
	vector<string> columns;
	string suffix;
	auto add = [&](const string &source, const char *prefix, const string &key) {
		if (key.empty()) {
			return;
		}
		auto target = prefix + Sanitize(key);
		// De-dupe (e.g. the same key listed twice, or two keys sanitizing alike).
		if (std::find(columns.begin(), columns.end(), target) != columns.end()) {
			return;
		}
		suffix += ", json_extract_string(" + QuoteIdentifier(source) + ", " + JsonPathLiteral(key) + ") AS " +
		          QuoteIdentifier(target);
		columns.push_back(std::move(target));
	};
	for (auto &key : config.resource_keys) {
		add("resource_attributes", "resource_attr_", key);
	}
	for (auto &key : config.scope_keys) {
		add("scope_attributes", "scope_attr_", key);
	}
	if (columns.empty()) {
		return "";
	}
	try {
		RunSQL(con, "LOAD json");
	} catch (...) {
		try {
			RunSQL(con, "SELECT json_extract_string('{\"a\":1}', '$.\"a\"')");
		} catch (std::exception &) {
			failure = "json extension unavailable: " + CurrentExceptionMessage();
			return "";
		}
	}
	// The first ALTER failure means the catalog cannot add columns at all, so disable rather than retry.
	for (auto *table : OTLP_SIGNAL_TABLES) {
		auto qualified = QualifiedTable(catalog, schema, table);
		for (auto &column : columns) {
			try {
				RunSQL(con, "ALTER TABLE " + qualified + " ADD COLUMN IF NOT EXISTS " + QuoteIdentifier(column) +
				                " VARCHAR");
			} catch (std::exception &) {
				failure = StringUtil::Format("ALTER ADD COLUMN on %s failed (catalog may not support it): %s", table,
				                             CurrentExceptionMessage());
				return "";
			}
		}
	}
	promoted_columns = columns.size();
	return suffix;
}

} // namespace duckdb
