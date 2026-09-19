#pragma once

#include "duckdb/common/string.hpp"

#include <map>
#include <utility>
#include <vector>

namespace duckdb_otlp_server {

//! True for the recognized truthy spellings (1/true/yes/on and their upper-case forms).
//! One definition, shared by configuration resolution and the healthcheck subcommand.
bool IsTruthy(const duckdb::string &value);

//! The configuration environment the daemon resolves against: the process environment, with
//! an optional overlay of values supplied on the command line.
//!
//! Every configuration read goes through an EnvSource rather than getenv(), so a `--flag` is
//! applied by layering it here instead of calling setenv(). Two things follow from that.
//! Precedence (`flag > env > default`) lives in one object that is passed explicitly, rather
//! than in a global whose mutation order has to be maintained by hand — `validate` and
//! `serve` cannot drift because they resolve from the same source. And nothing is written
//! back to the process environment, so a `--token` never becomes visible to getenv()
//! elsewhere in the process, nor to anything this process might exec.
class EnvSource {
public:
	//! The process environment, with no overlay.
	EnvSource() = default;
	//! The process environment with `overrides` layered on top; a later entry wins.
	explicit EnvSource(const std::vector<std::pair<duckdb::string, duckdb::string>> &overrides);

	//! `name`'s value, or `fallback` when it is unset or empty. An overlay entry set to the
	//! empty string reads as unset, matching how an empty environment variable behaves.
	duckdb::string Get(const char *name, const duckdb::string &fallback = "") const;
	//! True when `name` resolves to a non-empty value.
	bool Has(const char *name) const;
	//! True when `name` resolves to a recognized truthy spelling.
	bool Truthy(const char *name) const;

private:
	std::map<duckdb::string, duckdb::string> overlay;
};

} // namespace duckdb_otlp_server
