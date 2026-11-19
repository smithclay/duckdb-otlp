#pragma once

#include "duckdb.hpp"

#include <functional>

namespace duckdb {

class Connection;

//! Operator-listed resource/scope attribute keys copied out of the JSON blobs into VARCHAR columns
//! (`resource_attr_<key>` / `scope_attr_<key>`) for zone-map pruning; the blob is left intact. Catalog mode only.
struct OtlpPromoteConfig {
	vector<string> resource_keys;
	vector<string> scope_keys;
	bool Enabled() const {
		return !resource_keys.empty() || !scope_keys.empty();
	}
};

//! Immutable after Initialize(), so the seal thread and otlp_server_list read it without locking.
class OtlpColumnPromoter {
public:
	OtlpColumnPromoter(OtlpPromoteConfig config, string catalog_name, string schema_name,
	                   std::function<void(const string &)> log);

	bool Enabled() const {
		return enabled;
	}

	//! Adds every promoted column to every signal table (idempotent). Best-effort: on failure it logs
	//! and disables promotion rather than blocking startup.
	void Initialize(Connection &con);

	//! `, json_extract_string(...) AS "resource_attr_k", ...`, the same for every signal. Empty when disabled.
	const string &ProjectionSuffix() const {
		return suffix;
	}

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
