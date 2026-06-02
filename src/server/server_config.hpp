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
	int startup_timeout_secs = 60;
	uint64_t http_threads = 0;

	duckdb::string mode_setup_sql;
	std::vector<duckdb::string> mode_extensions;

	static ServerConfig FromEnv();

	duckdb::string StartOtlpSql() const;
	duckdb::string StartQuackSql() const;
	duckdb::string StopOtlpSql() const;
	duckdb::string StopQuackSql() const;
	duckdb::string BootSql() const;
};

} // namespace duckdb_otlp_server
