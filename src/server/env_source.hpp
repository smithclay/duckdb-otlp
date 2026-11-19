#pragma once

#include "duckdb/common/string.hpp"

#include <map>
#include <utility>
#include <vector>

namespace duckdb_otlp_server {

//! True for the recognized truthy spellings (1/true/yes/on and their upper-case forms).
bool IsTruthy(const duckdb::string &value);

//! The process environment with an overlay of command-line values. Every configuration read
//! goes through this rather than getenv(), so `flag > env > default` lives in one object and
//! nothing is written back with setenv(): a `--token` never becomes visible to getenv().
class EnvSource {
public:
	EnvSource() = default;
	//! The process environment with `overrides` layered on top; a later entry wins.
	explicit EnvSource(const std::vector<std::pair<duckdb::string, duckdb::string>> &overrides);

	//! `name`'s value, or `fallback` when it is unset or empty. An overlay entry set to the
	//! empty string reads as unset, matching how an empty environment variable behaves.
	duckdb::string Get(const char *name, const duckdb::string &fallback = "") const;
	bool Has(const char *name) const;
	//! True when `name` resolves to a recognized truthy spelling.
	bool Truthy(const char *name) const;

private:
	std::map<duckdb::string, duckdb::string> overlay;
};

} // namespace duckdb_otlp_server
