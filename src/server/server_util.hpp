#pragma once

#include "server_config.hpp"

#include "duckdb/common/string.hpp"

#include <exception>

namespace duckdb {
class Connection;
class QueryResult;
} // namespace duckdb

namespace duckdb_otlp_server {

//! Throw if `result` or any statement chained after it through `next` carries an error,
//! prefixing the message with `label`. Checking only the head would miss a later statement.
void CheckResult(duckdb::QueryResult &result, const duckdb::string &label);

//! Run `sql` and throw on any error. Empty `sql` is a no-op.
void Execute(duckdb::Connection &con, const duckdb::string &sql, const duckdb::string &label,
             bool print_result = false);

//! Bind each variable the generated SQL reads as getvariable('env_<NAME>') (the other half of
//! EnvSql()). getenv() is absent from the embedded library, and this keeps secrets out of the
//! SQL text. Without it, backend credentials silently resolve to the empty string.
void BindConfigEnvVariables(duckdb::Connection &con, const ServerConfig &config, const EnvSource &env);

//! The message to print for `ex`, without DuckDB's echo of the offending (generated) statement.
duckdb::string CliErrorMessage(const std::exception &ex);

//! Read and trim a SQL script, throwing an actionable error naming setting `what` on failure.
duckdb::string ReadSqlFile(const duckdb::string &path, const duckdb::string &what);

//! True when `path` carries a URI scheme ("s3://..."). The filesystem helpers below no-op on one:
//! `std::filesystem` would otherwise build a literal "s3:/bucket" tree in the working directory.
bool IsRemotePath(const duckdb::string &path);

//! Create `path` and any missing parents, throwing an actionable error on failure.
void CreateDirectory(const duckdb::string &path);

//! Create the parent directory of the file `path`, throwing an actionable error on failure.
void CreateParentDirectory(const duckdb::string &path);

//! True when <host>:<port> accepts a TCP connection within 2s and, with a non-empty `path`,
//! answers GET <path> with 2xx/3xx. Plain sockets, not httplib: compiling httplib into the daemon
//! gives the linker a second, zlib-less copy of its inline functions (see CMakeLists.txt).
bool ProbeListener(const duckdb::string &host, int port, const duckdb::string &path = "");

} // namespace duckdb_otlp_server
