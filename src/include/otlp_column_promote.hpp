#pragma once

#include "duckdb.hpp"

#include <functional>

namespace duckdb {

class Connection;

//! User-specified attribute promotion. At ingest the named resource/scope attribute keys are
//! extracted out of the `resource_attributes` / `scope_attributes` bags into first-class
//! VARCHAR columns (`resource_attr_<key>` / `scope_attr_<key>`) so they get zone-map pruning. The
//! bag is left intact (the residual); any value reconstructs by COALESCEing the column with the
//! same extract this promoter projects -- `json_extract_string(resource_attributes, '$."x"')` over
//! a JSON bag, `CAST(variant_extract(resource_attributes, 'x') AS VARCHAR)` over a VARIANT one,
//! since neither extract runs on the other's type. There is no auto-discovery: the promoted set is
//! exactly what the operator lists. Catalog mode only.
//!
//! The extract follows the bag's column type (see OtlpServerConfig::attributes_as_variant):
//! `json_extract_string(bag, '$."k"')` over JSON text, `CAST(variant_extract(bag, 'k') AS VARCHAR)`
//! over VARIANT. Both return NULL for an absent key, and variant_extract takes a literal key -- not
//! a path -- so a dotted OTLP key like `service.name` resolves as one member either way.
struct OtlpPromoteConfig {
	vector<string> resource_keys;
	vector<string> scope_keys;
	bool Enabled() const {
		return !resource_keys.empty() || !scope_keys.empty();
	}
};

//! Applies a fixed, operator-specified attribute promotion. Resolved once at startup; after
//! Initialize() all state is immutable, so the seal thread reads ProjectionSuffix() and
//! otlp_server_list reads PromotedColumnsTotal() without locking.
class OtlpColumnPromoter {
public:
	OtlpColumnPromoter(OtlpPromoteConfig config, bool attributes_as_variant, string catalog_name, string schema_name,
	                   std::function<void(const string &)> log);

	bool Enabled() const {
		return enabled;
	}

	//! Once at startup on the writer connection: ensure `json` is available and
	//! `ALTER TABLE ADD COLUMN IF NOT EXISTS` every promoted column on every signal table
	//! (idempotent, so a restart with the same config is a no-op). Best-effort: on failure (catalog
	//! cannot add columns, json missing) it logs and disables promotion rather than blocking start.
	void Initialize(Connection &con);

	//! INSERT...SELECT projection suffix, identical for every signal (resource/scope columns are
	//! common to all signal tables): `, json_extract_string("resource_attributes", '$."k"') AS
	//! "resource_attr_k", ...` (or the variant_extract form). Empty when disabled.
	const string &ProjectionSuffix() const {
		return suffix;
	}

	//! INSERT column-list additions parallel to ProjectionSuffix(), e.g. `, "resource_attr_k", ...`,
	//! so the seal can name the promoted target columns explicitly. Empty when disabled.
	const string &TargetColumnList() const {
		return target_list;
	}

	//! Number of promoted attribute columns per table (resource + scope), 0 when disabled.
	idx_t PromotedColumnsTotal() const {
		return enabled ? columns.size() : 0;
	}

private:
	struct Col {
		string source_column; //! resource_attributes | scope_attributes
		string attr_key;      //! the OTLP attribute key
		string target_column; //! resource_attr_<key> | scope_attr_<key>
	};
	void Disable(const string &reason);

	bool attributes_as_variant;
	string catalog_name;
	string schema_name;
	std::function<void(const string &)> log;
	vector<Col> columns;
	string suffix;
	string target_list;
	bool enabled = false;
};

} // namespace duckdb
