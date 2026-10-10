#include "otlp_column_promote.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include "otlp_sql_util.hpp"

namespace duckdb {

namespace {

//! The six signal tables every server creates; resource_attributes/scope_attributes exist on all.
const char *const SIGNAL_TABLES[] = {"otlp_logs",
                                     "otlp_traces",
                                     "otlp_metrics_gauge",
                                     "otlp_metrics_sum",
                                     "otlp_metrics_histogram",
                                     "otlp_metrics_exp_histogram"};

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

OtlpColumnPromoter::OtlpColumnPromoter(OtlpPromoteConfig config_p, bool attributes_as_variant_p, string catalog_name_p,
                                       string schema_name_p, std::function<void(const string &)> log_p)
    : attributes_as_variant(attributes_as_variant_p), catalog_name(std::move(catalog_name_p)),
      schema_name(std::move(schema_name_p)), log(std::move(log_p)) {
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
		suffix +=
		    ", " + ExtractSql(QuoteIdentifier(c.source_column), c.attr_key) + " AS " + QuoteIdentifier(c.target_column);
		target_list += ", " + QuoteIdentifier(c.target_column);
	}
}

string OtlpColumnPromoter::ExtractSql(const string &bag_expr, const string &key) const {
	// The promoted column stays VARCHAR either way, so nothing downstream (the ALTER, the INSERT
	// target list, a reader's COALESCE) depends on which of these it was.
	if (attributes_as_variant) {
		return "CAST(variant_extract(" + bag_expr + ", " + SqlQuote(key) + ") AS VARCHAR)";
	}
	return "json_extract_string(" + bag_expr + ", " + JsonPathLiteral(key) + ")";
}

void OtlpColumnPromoter::Disable(const string &reason) {
	// ProjectionSuffix()/PromotedColumnsTotal() are only consulted when Enabled(), so clearing
	// `enabled` is enough — no need to also wipe suffix/columns.
	enabled = false;
	if (log) {
		log("attribute promotion disabled: " + reason);
	}
}

void OtlpColumnPromoter::Initialize(Connection &con) {
	if (columns.empty()) {
		return;
	}
	// The extract has to be runnable on this connection before any column is added, so the probe is
	// the projection itself over a literal bag. Over JSON text that needs the json extension, hence
	// the best-effort LOAD; over VARIANT bags nothing has to be loaded, and the literal is built
	// from a struct rather than from JSON text because naming the JSON *type* in SQL would drag in
	// the very extension this path is free of.
	if (!attributes_as_variant) {
		try {
			Exec(con, "LOAD json");
		} catch (...) {
			// Not fatal on its own: the probe below is what decides.
		}
	}
	const string bag = attributes_as_variant ? "{'a': 1}::VARIANT" : "'{\"a\":1}'";
	try {
		Exec(con, "SELECT " + ExtractSql(bag, "a"));
	} catch (std::exception &ex) {
		Disable(StringUtil::Format("%s attribute extraction unavailable: %s",
		                           attributes_as_variant ? "VARIANT" : "JSON", ex.what()));
		return;
	}
	// Add every promoted column on every signal table (idempotent). A fixed config means the first
	// ALTER failure indicates the catalog cannot add columns (e.g. an Iceberg REST catalog without
	// write support), so disable rather than retry per column/table.
	for (auto *table : SIGNAL_TABLES) {
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
