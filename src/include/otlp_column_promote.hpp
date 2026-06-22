#pragma once

#include "duckdb.hpp"

#include <functional>

namespace duckdb {

class Connection;

//! User-specified attribute promotion. At ingest the named resource/scope attribute keys are
//! extracted out of the JSON `resource_attributes` / `scope_attributes` blobs into first-class
//! VARCHAR columns (`resource_attr_<key>` / `scope_attr_<key>`) so they get zone-map pruning. The
//! blob is left intact (the residual); any value reconstructs with
//! `COALESCE(resource_attr_x, json_extract_string(resource_attributes, '$."x"'))`. There is no
//! auto-discovery: the promoted set is exactly what the operator lists. Catalog mode only.
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
	OtlpColumnPromoter(OtlpPromoteConfig config, string catalog_name, string schema_name,
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
	//! "resource_attr_k", ...`. Empty when disabled.
	const string &ProjectionSuffix() const {
		return suffix;
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

	string catalog_name;
	string schema_name;
	std::function<void(const string &)> log;
	vector<Col> columns;
	string suffix;
	bool enabled = false;
};

} // namespace duckdb
