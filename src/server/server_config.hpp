#pragma once

#include "duckdb/common/string.hpp"

#include <cstdint>
#include <vector>

namespace duckdb_otlp_server {

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
