#pragma once

#include "server_config.hpp"

#include "duckdb/common/string.hpp"

namespace duckdb {
class Connection;
class QueryResult;
} // namespace duckdb

namespace duckdb_otlp_server {

//! Throw if `result` (or any statement chained after it) carries an error, prefixing the
//! message with `label`. con.Query() returns one result per statement linked through `next`,
//! so checking only the head would silently swallow a failure in a later statement of a
//! multi-statement script.
void CheckResult(duckdb::QueryResult &result, const duckdb::string &label);

//! Run `sql` and throw on any error. Empty `sql` is a no-op (several config fields are
//! optional and render to an empty string). With `print_result`, the result is printed —
//! the serve path shows the listener table that otlp_serve returns.
void Execute(duckdb::Connection &con, const duckdb::string &sql, const duckdb::string &label,
             bool print_result = false);

//! Set an environment variable for this process.
void SetEnv(const char *name, const duckdb::string &value);

//! Bind each environment variable the mode's generated SQL reads through getvariable() as a
//! session variable on `con`.
//!
//! This is the other half of EnvSql() in server_config.cpp: the generated SQL says
//! `getvariable('env_<NAME>')` and only this binding makes it resolve. Every command that
//! runs mode setup needs it, so it lives here rather than being copied per command — a
//! missing call makes backend credentials silently resolve to the empty string.
void BindConfigEnvVariables(duckdb::Connection &con, const ServerConfig &config);

//! Create `path` and any missing parents, throwing an actionable error on failure.
void CreateDirectory(const duckdb::string &path);

//! Create the parent directory of `path` (a file), throwing an actionable error on failure.
void CreateParentDirectory(const duckdb::string &path);

} // namespace duckdb_otlp_server
