#include "otlp_column_promote.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"

#include "otlp_arrow.hpp"
#include "otlp_sql_util.hpp"

namespace duckdb {

namespace {

void Exec(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (!result || result->HasError()) {
		throw IOException("%s", result ? result->GetError() : string("query failed"));
	}
}

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

OtlpColumnPromoter::OtlpColumnPromoter(OtlpPromoteConfig config_p, string catalog_name_p, string schema_name_p,
                                       std::function<void(const string &)> log_p)
    : catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_name_p)), log(std::move(log_p)) {
	auto add = [&](const string &source, const char *prefix, const string &key) {
		if (key.empty()) {
			return;
		}
		auto target = prefix + Sanitize(key);
		// De-dupe (e.g. the same key listed twice, or two keys sanitizing alike).
		for (auto &c : columns) {
			if (c.target_column == target) {
				return;
			}
		}
		columns.push_back({source, key, target});
	};
	for (auto &key : config_p.resource_keys) {
		add("resource_attributes", "resource_attr_", key);
	}
	for (auto &key : config_p.scope_keys) {
		add("scope_attributes", "scope_attr_", key);
	}
	for (auto &c : columns) {
		suffix += ", json_extract_string(" + QuoteIdentifier(c.source_column) + ", " + JsonPathLiteral(c.attr_key) +
		          ") AS " + QuoteIdentifier(c.target_column);
	}
}

void OtlpColumnPromoter::Disable(const string &reason) {
	enabled = false;
	if (log) {
		log("attribute promotion disabled: " + reason);
	}
}

void OtlpColumnPromoter::Initialize(Connection &con) {
	if (columns.empty()) {
		return;
	}
	try {
		Exec(con, "LOAD json");
	} catch (...) {
		try {
			Exec(con, "SELECT json_extract_string('{\"a\":1}', '$.\"a\"')");
		} catch (std::exception &ex) {
			Disable(string("json extension unavailable: ") + ex.what());
			return;
		}
	}
	// The first ALTER failure means the catalog cannot add columns at all, so disable rather than retry.
	for (auto *table : OTLP_SIGNAL_TABLES) {
		auto qualified = QualifiedTable(catalog_name, schema_name, table);
		for (auto &c : columns) {
			try {
				Exec(con, "ALTER TABLE " + qualified + " ADD COLUMN IF NOT EXISTS " + QuoteIdentifier(c.target_column) +
				              " VARCHAR");
			} catch (std::exception &ex) {
				Disable(StringUtil::Format("ALTER ADD COLUMN on %s failed (catalog may not support it): %s", table,
				                           ex.what()));
				return;
			}
		}
	}
	enabled = true;
	if (log) {
		log(StringUtil::Format("attribute promotion enabled: %llu column(s) per signal",
		                       static_cast<uint64_t>(columns.size())));
	}
}

} // namespace duckdb
