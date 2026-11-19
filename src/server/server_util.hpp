#pragma once

#include "server_config.hpp"

#include "duckdb/common/string.hpp"

#include <exception>

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

//! Bind each environment variable the mode's generated SQL reads through getvariable() as a
//! session variable on `con`, resolving its value through the same `env` that produced
//! `config`.
//!
//! This is the other half of EnvSql() in server_config.cpp: the generated SQL says
//! `getvariable('env_<NAME>')` and only this binding makes it resolve. Every command that
//! runs mode setup needs it, so it lives here rather than being copied per command — a
//! missing call makes backend credentials silently resolve to the empty string.
void BindConfigEnvVariables(duckdb::Connection &con, const ServerConfig &config, const EnvSource &env);

//! The message to print for `ex`, without DuckDB's echo of the offending (generated) statement.
duckdb::string CliErrorMessage(const std::exception &ex);

//! Read a SQL script from `path`, throwing an actionable error when it cannot be opened or
//! read. `what` names the setting that supplied the path. Returns the contents trimmed.
duckdb::string ReadSqlFile(const duckdb::string &path, const duckdb::string &what);

//! True when `path` carries a URI scheme ("s3://bucket/...", "gcss://...", "https://...").
//!
//! Object stores have no directories to create, and treating a URI as a local path does not
//! fail loudly -- `std::filesystem` happily builds a literal "s3:/bucket/prefix" tree under
//! the working directory -- so the filesystem helpers below check this rather than letting
//! every caller remember to.
bool IsRemotePath(const duckdb::string &path);

//! Create `path` and any missing parents, throwing an actionable error on failure.
//! A remote path is a no-op: there is no local directory to create.
void CreateDirectory(const duckdb::string &path);

//! Create the parent directory of `path` (a file), throwing an actionable error on failure.
//! A remote path is a no-op, for the same reason.
void CreateParentDirectory(const duckdb::string &path);

//! Probe a listener for `doctor`: true when <host>:<port> accepts a TCP connection within 2s
//! and, with a non-empty `path`, answers GET <path> with a 2xx/3xx status. Plain sockets
//! rather than httplib: compiling httplib into the daemon would give the linker a second,
//! zlib-less copy of its inline functions (see the link-order note in CMakeLists.txt).
bool ProbeListener(const duckdb::string &host, int port, const duckdb::string &path = "");

} // namespace duckdb_otlp_server
