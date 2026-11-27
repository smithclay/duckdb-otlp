#pragma once

#include "duckdb.hpp"

namespace duckdb {

class Connection;

//! Operator-listed resource/scope attribute keys copied out of the JSON blobs into VARCHAR columns
//! (`resource_attr_<key>` / `scope_attr_<key>`) for zone-map pruning; the blob is left intact. Catalog mode only.
struct OtlpPromoteConfig {
	vector<string> resource_keys;
	vector<string> scope_keys;
};

//! Adds the requested columns to each signal table and returns their common projection suffix.
//! On failure, returns an empty suffix and provides the reason for the caller to log.
string OtlpPromoteColumns(Connection &con, const OtlpPromoteConfig &config, const string &catalog, const string &schema,
                          idx_t &promoted_columns, string &failure);

} // namespace duckdb
