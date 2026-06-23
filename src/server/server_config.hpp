#pragma once

#include "duckdb/common/string.hpp"
#include "otlp_ingest_limits.hpp"

#include <cstdint>
#include <vector>

namespace duckdb_otlp_server {

//! True when environment variable `name` is set to a recognized truthy value
//! (1/true/yes/on and their upper-case spellings). Shared by FromEnv() and the
//! daemon's healthcheck subcommand so the accepted set has one definition.
bool EnvTruthy(const char *name);

struct ServerConfig {
	duckdb::string mode;
	duckdb::string database;
	duckdb::string data_dir;
	duckdb::string listen_uri;
	duckdb::string otel_http_addr;
	duckdb::string token;
	duckdb::string catalog;
	duckdb::string schema;
	duckdb::string quack_listen_uri;
	duckdb::string quack_http_addr;
	duckdb::string quack_token;
	duckdb::string parquet_export_path;
	bool quack_enabled = false;
	bool dry_run = false;
	//! True when the OTLP token fell back to the built-in development default. The daemon
	//! warns about this at startup; it is never a hard failure (see main.cpp banner).
	bool using_default_token = false;
	int startup_timeout_secs = 60;
	uint64_t http_threads = 0;
	uint64_t max_body_bytes = 16ULL * 1024ULL * 1024ULL;
	uint64_t max_buffered_bytes = 512ULL * 1024ULL * 1024ULL;
	uint64_t seal_target_bytes = 128ULL * 1024ULL * 1024ULL;
	int64_t seal_max_age_ms = 5000;
	uint64_t target_file_size = 256ULL * 1024ULL * 1024ULL;
	int64_t maintenance_retention_ms = 15LL * 60LL * 1000LL;
	//! Attribute promotion (opt-in): comma-separated resource / scope attribute keys to promote into
	//! first-class columns at ingest. Emitted into the otlp_serve() call when non-empty.
	duckdb::string promote_resource_attributes;
	duckdb::string promote_scope_attributes;

	duckdb::string mode_setup_sql;
	std::vector<duckdb::string> mode_extensions;
	//! Environment-variable names that the generated secret SQL reads via getvariable().
	//! The daemon binds each ("env_<NAME>" -> the env value) as a session variable before
	//! running mode setup, so secret values never appear in the generated SQL text.
	//! (getenv() is a CLI-only DuckDB function, absent in the embedded library.)
	std::vector<duckdb::string> env_variables;

	static ServerConfig FromEnv();

	duckdb::string StartOtlpSql() const;
	duckdb::string StartQuackSql() const;
	duckdb::string StopOtlpSql() const;
	duckdb::string StopQuackSql() const;
	duckdb::string BootSql() const;
};

} // namespace duckdb_otlp_server
