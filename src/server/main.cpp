#include "server_config.hpp"
#include "storage/otlp_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace {

volatile std::sig_atomic_t shutdown_requested = 0;

void HandleSignal(int) {
	shutdown_requested = 1;
}

void InstallSignalHandlers() {
	std::signal(SIGINT, HandleSignal);
	std::signal(SIGTERM, HandleSignal);
}

void SetEnv(const char *name, const duckdb::string &value) {
#ifdef _WIN32
	_putenv_s(name, value.c_str());
#else
	setenv(name, value.c_str(), 1);
#endif
}

void SetDefaultEnv(const char *name, const duckdb::string &value) {
	if (std::getenv(name)) {
		return;
	}
	SetEnv(name, value);
}

void CheckResult(duckdb::QueryResult &result, const duckdb::string &label) {
	if (result.HasError()) {
		result.ThrowError(label + ": ");
	}
	auto next = result.next.get();
	while (next) {
		if (next->HasError()) {
			next->ThrowError(label + ": ");
		}
		next = next->next.get();
	}
}

void Execute(duckdb::Connection &con, const duckdb::string &sql, const duckdb::string &label,
             bool print_result = false) {
	if (sql.empty()) {
		return;
	}
	auto result = con.Query(sql);
	CheckResult(*result, label);
	if (print_result) {
		result->Print();
	}
}

bool TryExecuteShutdown(duckdb::Connection &con, const duckdb::string &sql, const duckdb::string &label) {
	try {
		Execute(con, sql, label, true);
		return true;
	} catch (std::exception &ex) {
		std::cerr << "ERROR during " << label << ": " << ex.what() << std::endl;
		return false;
	}
}

duckdb::string SqlEscape(const duckdb::string &value) {
	return duckdb::StringUtil::Replace(value, "'", "''");
}

duckdb::string SqlQuote(const duckdb::string &value) {
	return "'" + SqlEscape(value) + "'";
}

struct OtlpHealth {
	bool found = false;
	bool listening = false;
	duckdb::string last_error;
};

OtlpHealth QueryOtlpHealth(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	auto result =
	    con.Query("SELECT is_listening, coalesce(last_error, '') FROM otlp_server_list() WHERE listen_uri = " +
	              SqlQuote(config.listen_uri) + " LIMIT 1");
	CheckResult(*result, "otlp readiness");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return {};
	}
	OtlpHealth health;
	health.found = true;
	health.listening = chunk->GetValue(0, 0).GetValue<bool>();
	health.last_error = chunk->GetValue(1, 0).GetValue<duckdb::string>();
	return health;
}

bool WaitForReady(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(config.startup_timeout_secs);
	while (!shutdown_requested && std::chrono::steady_clock::now() < deadline) {
		auto health = QueryOtlpHealth(con, config);
		if (health.found && health.listening) {
			return true;
		}
		if (!health.last_error.empty()) {
			throw std::runtime_error(duckdb::string("OTLP listener failed during startup: ") + health.last_error);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
	}
	return false;
}

bool WaitForShutdownOrListenerFailure(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	duckdb::idx_t ticks = 0;
	while (!shutdown_requested) {
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
		if (++ticks % 4 != 0) {
			continue;
		}
		auto health = QueryOtlpHealth(con, config);
		if (!health.found) {
			std::cerr << "ERROR: OTLP listener disappeared from server registry" << std::endl;
			return false;
		}
		if (!health.listening) {
			std::cerr << "ERROR: OTLP listener stopped";
			if (!health.last_error.empty()) {
				std::cerr << ": " << health.last_error;
			}
			std::cerr << std::endl;
			return false;
		}
	}
	return true;
}

void PrintUsage() {
	std::cout << R"HELP(Usage:

  duckdb-otlp-server

Required:

  DUCKDB_MODE=local-ducklake|parquet|r2-data-catalog|s3-tables|r2-neon-ducklake|r2-local-ducklake

Useful common settings:

  DUCKDB_DATABASE=/data/duckdb-otlp-control.duckdb
  OTEL_HTTP_ADDR=0.0.0.0:4318
  DUCKDB_OTLP_TOKEN=change-me-at-least-16-chars
  DUCKDB_QUACK_ENABLED=0
  DUCKDB_QUACK_ADDR=0.0.0.0:9494
  DUCKDB_QUACK_TOKEN=required-when-quack-enabled
  DUCKDB_OTLP_HTTP_THREADS=auto
  DUCKDB_OTLP_STARTUP_TIMEOUT=60
  DRY_RUN=1
)HELP";
}

} // namespace

int main(int argc, char **argv) {
	if (argc > 1) {
		auto arg = duckdb::string(argv[1]);
		if (arg == "help" || arg == "--help" || arg == "-h") {
			PrintUsage();
			return 0;
		}
		std::cerr << "ERROR: unsupported argument: " << arg << std::endl;
		PrintUsage();
		return 1;
	}

	try {
		auto config = duckdb_otlp_server::ServerConfig::FromEnv();
		SetDefaultEnv("NEON_PGPORT", "5432");
		SetDefaultEnv("NEON_PGSSLMODE", "require");

		std::cout << "Starting DuckDB OTEL Server\n\n";
		std::cout << "Mode: " << config.mode << "\n";
		std::cout << "Database: " << config.database << "\n\n";
		std::cout << "OTLP HTTP: " << config.otel_http_addr << "\n";
		if (config.quack_enabled) {
			std::cout << "Quack: " << config.quack_listen_uri << "\n\n";
			std::cout << "WARNING: Quack grants full SQL read/write access to every attached catalog over an "
			             "unencrypted connection.\n";
		} else {
			std::cout << "Quack: disabled\n";
		}
		if (!config.mode_extensions.empty()) {
			std::cout << "\nExtensions:\n";
			for (auto &extension : config.mode_extensions) {
				std::cout << "  " << extension << "\n";
			}
		}
		std::cout << std::endl;

		if (config.dry_run) {
			std::cout << "DRY_RUN=1; planned initialization only.\n\n";
			std::cout << "Generated initialization SQL:\n";
			std::cout << config.BootSql() << std::endl;
			return 0;
		}

		duckdb::DuckDB db(config.database);
		db.LoadStaticExtension<duckdb::OtlpExtension>();
		duckdb::Connection con(db);
		// Bind the tokens as session variables rather than interpolating them into the
		// startup SQL, so the secrets never appear in the generated SQL text (which
		// DRY_RUN prints and the engine can echo back in error messages). StartOtlpSql()/
		// StartQuackSql() read them back with getvariable(...).
		con.context->config.SetUserVariable("duckdb_otlp_effective_token", duckdb::Value(config.token));
		con.context->config.SetUserVariable("duckdb_quack_effective_token", duckdb::Value(config.quack_token));

		InstallSignalHandlers();
		Execute(con, config.mode_setup_sql, "mode setup");
		Execute(con, config.StartOtlpSql(), "otlp startup", true);
		Execute(con, config.StartQuackSql(), "quack startup", true);
		if (!WaitForReady(con, config) && !shutdown_requested) {
			throw std::runtime_error("Timed out waiting for OTLP listener readiness");
		}

		std::cout << "DuckDB initialization complete\n";
		std::cout << "Starting server..." << std::endl;

		auto listener_ok = WaitForShutdownOrListenerFailure(con, config);

		std::cout << "Stopping duckdb-otlp..." << std::endl;
		bool shutdown_ok = true;
		if (config.quack_enabled) {
			shutdown_ok = TryExecuteShutdown(con, config.StopQuackSql(), "quack shutdown") && shutdown_ok;
		}
		shutdown_ok = TryExecuteShutdown(con, config.StopOtlpSql(), "otlp shutdown") && shutdown_ok;
		return listener_ok && shutdown_ok ? 0 : 1;
	} catch (std::exception &ex) {
		std::cerr << "ERROR: " << ex.what() << std::endl;
		return 1;
	}
}
