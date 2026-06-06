#include "server_config.hpp"
#include "storage/otlp_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_result.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace duckdb {
// Defined in otlp_server_http.cpp (linked into the daemon). Declared here rather than
// including otlp_server.hpp, whose transitive includes are not on the daemon's include path.
bool OtlpLoopbackHttpStatusOk(int port, const string &path);
} // namespace duckdb

namespace {

// Written from a signal handler, so it must be a mutable global volatile sig_atomic_t.
volatile std::sig_atomic_t shutdown_requested = 0; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

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
		std::cerr << "ERROR during " << label << ": " << ex.what() << '\n';
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
	uint64_t seal_failures = 0;
	duckdb::string seal_last_error;
};

OtlpHealth QueryOtlpHealth(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	auto result =
	    con.Query("SELECT is_listening, coalesce(last_error, ''), seal_failures_total, coalesce(seal_last_error, '') "
	              "FROM otlp_server_list() WHERE listen_uri = " +
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
	health.seal_failures = chunk->GetValue(2, 0).GetValue<uint64_t>();
	health.seal_last_error = chunk->GetValue(3, 0).GetValue<duckdb::string>();
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
	// Seal failures are surfaced as WARNINGs rather than a process exit: a failed seal
	// re-buffers its rows and retries on the next trigger, so a transient backend outage
	// should not crash the daemon. But the HTTP /readyz probe only reports that the
	// listener is bound (it keeps returning 202 "buffered" while seals fail), so without
	// this log a credential/backend problem would be invisible until the buffer fills.
	uint64_t last_seal_failures = 0;
	while (!shutdown_requested) {
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
		if (++ticks % 4 != 0) {
			continue;
		}
		auto health = QueryOtlpHealth(con, config);
		if (!health.found) {
			std::cerr << "ERROR: OTLP listener disappeared from server registry" << '\n';
			return false;
		}
		if (!health.listening) {
			std::cerr << "ERROR: OTLP listener stopped";
			if (!health.last_error.empty()) {
				std::cerr << ": " << health.last_error;
			}
			std::cerr << '\n';
			return false;
		}
		if (health.seal_failures > last_seal_failures) {
			last_seal_failures = health.seal_failures;
			std::cerr << "WARNING: buffered rows are not committing (seal_failures_total=" << health.seal_failures
			          << ")";
			if (!health.seal_last_error.empty()) {
				std::cerr << ": " << health.seal_last_error;
			}
			std::cerr << '\n';
		}
	}
	return true;
}

duckdb::string EnvOr(const char *name, const duckdb::string &fallback) {
	auto value = std::getenv(name);
	return value && value[0] ? duckdb::string(value) : fallback;
}

bool EnvTruthy(const char *name) {
	auto value = std::getenv(name);
	if (!value || !value[0]) {
		return false;
	}
	duckdb::string lowered(value);
	return lowered == "1" || lowered == "true" || lowered == "TRUE" || lowered == "yes" || lowered == "YES" ||
	       lowered == "on" || lowered == "ON";
}

int PortFromAddr(const duckdb::string &addr, int fallback) {
	auto colon = addr.rfind(':');
	if (colon == duckdb::string::npos) {
		return fallback;
	}
	try {
		auto port = std::stoi(addr.substr(colon + 1));
		if (port > 0 && port <= 65535) {
			return port;
		}
	} catch (...) {
	}
	return fallback;
}

// Container HEALTHCHECK entry point. Distroless images ship no shell/curl, so the daemon
// probes itself over loopback. Mirrors the previous shell check: OTLP /readyz, plus the
// Quack root when Quack is enabled. Returns a process exit code (0 healthy, 1 unhealthy).
int RunHealthCheck() {
	auto otlp_port = PortFromAddr(EnvOr("OTEL_HTTP_ADDR", "0.0.0.0:4318"), 4318);
	if (!duckdb::OtlpLoopbackHttpStatusOk(otlp_port, "/readyz")) {
		return 1;
	}
	if (EnvTruthy("DUCKDB_QUACK_ENABLED") || EnvTruthy("QUACK_ENABLED")) {
		auto quack_addr = EnvOr("DUCKDB_QUACK_ADDR", EnvOr("QUACK_HTTP_ADDR", "0.0.0.0:9494"));
		if (!duckdb::OtlpLoopbackHttpStatusOk(PortFromAddr(quack_addr, 9494), "/")) {
			return 1;
		}
	}
	return 0;
}

void PrintUsage() {
	std::cout << R"HELP(Usage:

  duckdb-otlp-server

Required:

  DUCKDB_MODE=local-ducklake|aws-ducklake|parquet|r2-data-catalog|s3-tables|r2-neon-ducklake|r2-local-ducklake

Useful common settings:

  DUCKDB_DATABASE=/data/duckdb-otlp-control.duckdb
  OTEL_HTTP_ADDR=0.0.0.0:4318
  DUCKDB_OTLP_TOKEN=change-me-at-least-16-chars
  DUCKDB_QUACK_ENABLED=0
  DUCKDB_QUACK_ADDR=0.0.0.0:9494
  DUCKDB_QUACK_TOKEN=required-when-quack-enabled
  DUCKDB_OTLP_HTTP_THREADS=auto
  DUCKDB_OTLP_MAX_BODY_BYTES=16777216
  DUCKDB_OTLP_MAX_BUFFERED_BYTES=536870912
  DUCKDB_OTLP_SEAL_TARGET_BYTES=67108864
  DUCKDB_OTLP_SEAL_MAX_AGE_MS=5000
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
		if (arg == "healthcheck") {
			return RunHealthCheck();
		}
		std::cerr << "ERROR: unsupported argument: " << arg << '\n';
		PrintUsage();
		return 1;
	}

	try {
		auto config = duckdb_otlp_server::ServerConfig::FromEnv();
		SetDefaultEnv("NEON_PGPORT", "5432");
		SetDefaultEnv("NEON_PGSSLMODE", "require");

		std::cout << "Starting duckdb-otlp server\n\n";
		std::cout << "Mode: " << config.mode << "\n";
		std::cout << "Database: " << config.database << "\n\n";
		std::cout << "OTLP HTTP: " << config.otel_http_addr << "\n";
		if (config.using_default_token) {
			std::cout << "\nWARNING: using the built-in development OTLP token. Anyone who can reach "
			          << config.otel_http_addr
			          << " can ingest with a token that is public in this repo. Set DUCKDB_OTLP_TOKEN "
			             "(or OTEL_AUTH_TOKEN) to a private value before exposing this server.\n\n";
		}
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
		std::cout << '\n';

		if (config.dry_run) {
			std::cout << "DRY_RUN=1; planned initialization only.\n\n";
			std::cout << "Generated initialization SQL:\n";
			std::cout << config.BootSql() << '\n';
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
		// Bind each env var referenced by the mode's secret SQL as a session variable, so the
		// generated getvariable('env_<NAME>') resolves it at execution time. getenv() is a
		// CLI-only function (absent in the embedded library), and this keeps secret values out
		// of the generated SQL text.
		for (auto &name : config.env_variables) {
			auto value = std::getenv(name.c_str());
			con.context->config.SetUserVariable("env_" + name, duckdb::Value(value ? value : ""));
		}

		InstallSignalHandlers();

		// Startup runs setup SQL synchronously, so the signal-handler flag is only observed
		// between statements. A remote ATTACH/INSTALL can block for a long time, so a watcher
		// thread interrupts the connection as soon as a signal arrives — turning a slow
		// startup into a prompt, clean exit instead of waiting for Docker to SIGKILL.
		std::atomic<bool> startup_complete {false};
		std::thread interrupt_watcher([&con, &startup_complete] {
			while (!startup_complete.load()) {
				if (shutdown_requested) {
					con.Interrupt();
					return;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
		});
		// Always join the watcher, even if startup throws.
		struct WatcherGuard {
			std::atomic<bool> &done;
			std::thread &worker;
			~WatcherGuard() {
				done.store(true);
				if (worker.joinable()) {
					worker.join();
				}
			}
		} watcher_guard {startup_complete, interrupt_watcher};

		Execute(con, config.mode_setup_sql, "mode setup");
		// Make the mode's telemetry catalog the instance-wide default database. The Quack
		// server handles each external client on a *fresh* Connection spun up from the
		// DatabaseInstance (quack_server.cpp: make_uniq<Connection>(*db)), not on this
		// startup connection, and `USE`/`SET schema` only mutate the connection they run
		// on. Setting the DatabaseManager default lets those client connections resolve the
		// telemetry catalog by default, so callers can scan tables transparently
		// (ATTACH 'quack:...' AS x; FROM x.otlp_logs) instead of wrapping every statement in
		// x.query('...'). otlp_serve targets the catalog explicitly, so changing the default
		// is safe. Parquet mode has no catalog (its inspection views already live in the
		// default catalog) and is skipped.
		if (!config.catalog.empty()) {
			// SetDefaultDatabase resolves the catalog through the meta-transaction, so it must run
			// inside an explicit transaction on this connection.
			con.BeginTransaction();
			try {
				duckdb::DatabaseManager::Get(*con.context).SetDefaultDatabase(*con.context, config.catalog);
				con.Commit();
			} catch (...) {
				con.Rollback();
				throw;
			}
		}
		Execute(con, config.StartOtlpSql(), "otlp startup", true);
		Execute(con, config.StartQuackSql(), "quack startup", true);
		if (!WaitForReady(con, config) && !shutdown_requested) {
			throw std::runtime_error("Timed out waiting for OTLP listener readiness");
		}
		startup_complete.store(true);
		interrupt_watcher.join();

		std::cout << "DuckDB initialization complete\n";
		std::cout << "Starting server..." << '\n';

		auto listener_ok = WaitForShutdownOrListenerFailure(con, config);

		std::cout << "Stopping duckdb-otlp..." << '\n';
		bool shutdown_ok = true;
		if (config.quack_enabled) {
			shutdown_ok = TryExecuteShutdown(con, config.StopQuackSql(), "quack shutdown") && shutdown_ok;
		}
		shutdown_ok = TryExecuteShutdown(con, config.StopOtlpSql(), "otlp shutdown") && shutdown_ok;
		return listener_ok && shutdown_ok ? 0 : 1;
	} catch (std::exception &ex) {
		if (shutdown_requested) {
			// A signal interrupted startup (e.g. mid-ATTACH); treat it as a clean stop.
			std::cerr << "Shutdown requested during startup; exiting before the server became ready." << '\n';
			return 0;
		}
		// DuckDB exceptions stringify as a JSON blob; RawMessage() gives the plain text a
		// Docker user actually wants to read.
		std::cerr << "ERROR: " << duckdb::ErrorData(ex).RawMessage() << '\n';
		return 1;
	}
}
