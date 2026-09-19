#include "cli.hpp"
#include "commands.hpp"
#include "server_config.hpp"
#include "storage/otlp_extension.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_uri.hpp"

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
// Defined in otlp_server.cpp (linked into the daemon). Declared here rather than including
// otlp_server.hpp, whose transitive includes are not on the daemon's include path.
bool OtlpHttpStatusOk(const string &host, int port, const string &path);
bool OtlpTcpConnectOk(const string &host, int port);
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

// Run the OTLP shutdown SQL (otlp_stop), which seals remaining buffered rows. otlp_stop
// returns (status, dropped_rows): dropped_rows is non-zero only when the final drain failed
// and rows were dropped. Returns false (so main() exits non-zero) on either a thrown error
// OR a dropped-row count > 0, so an orchestrator can tell a clean shutdown from a
// data-dropping one (review finding M4).
bool TryExecuteOtlpShutdown(duckdb::Connection &con, const duckdb::string &sql, const duckdb::string &label) {
	try {
		auto result = con.Query(sql);
		CheckResult(*result, label);
		result->Print();
		auto chunk = result->Fetch();
		uint64_t dropped_rows = 0;
		if (chunk && chunk->size() > 0 && chunk->ColumnCount() >= 2) {
			dropped_rows = chunk->GetValue(1, 0).GetValue<uint64_t>();
		}
		if (dropped_rows > 0) {
			std::cerr << "ERROR: " << label << " dropped " << dropped_rows
			          << " un-sealed buffered rows (the final seal failed); shutting down NON-CLEAN.\n";
			return false;
		}
		return true;
	} catch (std::exception &ex) {
		std::cerr << "ERROR during " << label << ": " << ex.what() << '\n';
		return false;
	}
}

struct OtlpHealth {
	bool found = false;
	bool listening = false;
	duckdb::string last_error;
	uint64_t seal_failures = 0;
	duckdb::string seal_last_error;
	uint64_t maintenance_runs = 0;
	uint64_t maintenance_failures = 0;
	duckdb::string maintenance_last_error;
	uint64_t maintenance_contended = 0;
};

OtlpHealth QueryOtlpHealth(duckdb::Connection &con, const duckdb_otlp_server::IngestListener &listener) {
	auto result =
	    con.Query("SELECT is_listening, coalesce(last_error, ''), seal_failures_total, coalesce(seal_last_error, ''), "
	              "maintenance_runs_total, maintenance_failures_total, coalesce(maintenance_last_error, ''), "
	              "maintenance_contended_total "
	              "FROM otlp_server_list() WHERE listen_uri = " +
	              duckdb::SqlQuote(listener.uri) + " LIMIT 1");
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
	health.maintenance_runs = chunk->GetValue(4, 0).GetValue<uint64_t>();
	health.maintenance_failures = chunk->GetValue(5, 0).GetValue<uint64_t>();
	health.maintenance_last_error = chunk->GetValue(6, 0).GetValue<duckdb::string>();
	health.maintenance_contended = chunk->GetValue(7, 0).GetValue<uint64_t>();
	return health;
}

bool WaitForReady(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(config.startup_timeout_secs);
	while (!shutdown_requested && std::chrono::steady_clock::now() < deadline) {
		bool ready = true;
		for (const auto &listener : config.listeners) {
			auto health = QueryOtlpHealth(con, listener);
			ready = ready && health.found && health.listening;
			if (!health.last_error.empty()) {
				throw std::runtime_error("OTLP listener " + listener.uri +
				                         " failed during startup: " + health.last_error);
			}
		}
		if (ready) {
			return true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
	}
	return false;
}

bool WaitForShutdownOrListenerFailure(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	duckdb::idx_t ticks = 0;
	// Every listener feeds one server, so the seal and maintenance counters are server-wide:
	// each listener's otlp_server_list() row repeats them. Read them once (from the first
	// listener) so each outcome is logged once, not once per transport.
	//
	// Seal failures are surfaced as WARNINGs rather than a process exit: a failed seal
	// re-buffers its rows and retries on the next trigger, so a transient backend outage
	// should not crash the daemon. /readyz degrades only once the stall persists, so without
	// this log a credential/backend problem would be invisible until the buffer fills.
	uint64_t last_seal_failures = 0;
	// Catalog maintenance (CHECKPOINT) is what flushes DuckLake-inlined rows out of the metadata
	// catalog into Parquet and compacts small files. It runs on the sealer thread and only logs
	// to duckdb_logs, so a failing (or auto-disabled) checkpoint was silent: rows kept committing
	// into the catalog while no Parquet appeared. Print every outcome so it is visible in logs.
	uint64_t last_maintenance_runs = 0;
	uint64_t last_maintenance_failures = 0;
	// A contended pass means another process sharing the catalog (e.g. a second receiver during a
	// deploy) checkpointed first. Informational: it is not a failure and must not trip alerts.
	uint64_t last_maintenance_contended = 0;
	while (!shutdown_requested) {
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
		if (++ticks % 4 != 0) {
			continue;
		}
		OtlpHealth server_health;
		for (const auto &listener : config.listeners) {
			auto health = QueryOtlpHealth(con, listener);
			if (!health.found || !health.listening) {
				std::cerr << "ERROR: OTLP listener " << listener.uri << " stopped or disappeared: " << health.last_error
				          << '\n';
				return false;
			}
			if (&listener == &config.listeners.front()) {
				server_health = health;
			}
		}
		if (server_health.seal_failures > last_seal_failures) {
			last_seal_failures = server_health.seal_failures;
			std::cerr << "WARNING: buffered rows are not committing (catalog=" << config.catalog
			          << ", seal_failures_total=" << server_health.seal_failures << ")";
			if (!server_health.seal_last_error.empty()) {
				std::cerr << ": " << server_health.seal_last_error;
			}
			std::cerr << '\n';
		}
		if (server_health.maintenance_failures > last_maintenance_failures) {
			last_maintenance_failures = server_health.maintenance_failures;
			std::cerr << "WARNING: catalog maintenance CHECKPOINT failed; inlined rows are not being flushed "
			             "to Parquet (catalog="
			          << config.catalog << ", maintenance_failures_total=" << server_health.maintenance_failures << ")";
			if (!server_health.maintenance_last_error.empty()) {
				std::cerr << ": " << server_health.maintenance_last_error;
			}
			std::cerr << '\n';
		}
		if (server_health.maintenance_contended > last_maintenance_contended) {
			last_maintenance_contended = server_health.maintenance_contended;
			std::cerr << "catalog maintenance CHECKPOINT skipped: another writer checkpointed first (catalog="
			          << config.catalog << ", maintenance_contended_total=" << server_health.maintenance_contended
			          << ")\n";
		}
		if (server_health.maintenance_runs > last_maintenance_runs) {
			last_maintenance_runs = server_health.maintenance_runs;
			std::cerr << "catalog maintenance CHECKPOINT succeeded (catalog=" << config.catalog
			          << ", maintenance_runs_total=" << server_health.maintenance_runs << ")\n";
		}
	}
	return true;
}

duckdb::string EnvOr(const char *name, const duckdb::string &fallback) {
	auto value = std::getenv(name);
	return value && value[0] ? duckdb::string(value) : fallback;
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

// Host portion of a "host:port" / "[ipv6]:port" bind address (no scheme). Empty host -> "".
duckdb::string HostFromAddr(const duckdb::string &addr) {
	if (!addr.empty() && addr[0] == '[') {
		// [ipv6]:port — return the literal inside the brackets.
		auto close = addr.find(']');
		if (close != duckdb::string::npos) {
			return addr.substr(1, close - 1);
		}
		return addr;
	}
	auto colon = addr.rfind(':');
	if (colon == duckdb::string::npos) {
		return addr; // bare host, no port
	}
	return addr.substr(0, colon);
}

// The host the healthcheck should probe for a server bound to `addr`. A wildcard/unspecified bind
// (0.0.0.0, ::, empty) is reachable on loopback, so probe loopback (the previous behavior). An
// explicit interface (e.g. 192.168.1.5) is NOT reachable on loopback, so probe it directly —
// otherwise the container HEALTHCHECK fails forever on a healthy server (review finding M5).
duckdb::string HealthCheckHost(const duckdb::string &addr) {
	auto host = HostFromAddr(addr);
	if (host.empty() || host == "0.0.0.0" || host == "::" || host == "[::]") {
		return "127.0.0.1";
	}
	return host;
}

// Probe GET http://<host>:<port><path>. Loopback for a wildcard host, the configured host
// otherwise (review finding M5).
bool HealthProbe(const duckdb::string &addr, const duckdb::string &path, int port_fallback) {
	auto host = HealthCheckHost(addr);
	auto port = PortFromAddr(addr, port_fallback);
	return duckdb::OtlpHttpStatusOk(host, port, path);
}

// Container HEALTHCHECK entry point. Distroless images ship no shell/curl, so the daemon
// probes itself. It probes the CONFIGURED bind host (loopback for a 0.0.0.0/:: wildcard bind,
// the explicit interface otherwise — see HealthCheckHost), so a non-loopback bind is supported
// without a forever-failing loopback probe (review finding M5). HTTP listeners use /readyz,
// gRPC listeners use TCP connect, and Quack is checked when enabled. Returns 0 only when
// every configured listener is healthy.
int RunHealthCheck() {
	try {
		for (const auto &listener : duckdb_otlp_server::ListenersFromEnv()) {
			duckdb::OtlpUri uri(listener.uri);
			auto host = uri.Host();
			if (host == "0.0.0.0" || host == "::") {
				host = "127.0.0.1";
			}
			bool healthy = listener.transport == "grpc" ? duckdb::OtlpTcpConnectOk(host, uri.Port())
			                                            : duckdb::OtlpHttpStatusOk(host, uri.Port(), "/readyz");
			if (!healthy) {
				return 1;
			}
		}
	} catch (std::exception &ex) {
		std::cerr << "ERROR: " << duckdb::ErrorData(ex).RawMessage() << '\n';
		return 1;
	}
	if (duckdb_otlp_server::EnvTruthy("DUCKDB_QUACK_ENABLED") || duckdb_otlp_server::EnvTruthy("QUACK_ENABLED")) {
		auto quack_addr = EnvOr("DUCKDB_QUACK_ADDR", EnvOr("QUACK_HTTP_ADDR", "0.0.0.0:9494"));
		if (!HealthProbe(quack_addr, "/", 9494)) {
			return 1;
		}
	}
	return 0;
}

// The `serve` subcommand (and the bare invocation, which means the same thing): resolve
// configuration, run mode setup, start every listener, then block until a signal arrives.
int RunServe() {
	try {
		auto config = duckdb_otlp_server::ServerConfig::FromEnv();
		SetDefaultEnv("NEON_PGPORT", "5432");
		SetDefaultEnv("NEON_PGSSLMODE", "require");

		std::cout << "Starting duckdb-otlp server\n\n";
		std::cout << "Mode: " << config.mode << "\n";
		std::cout << "Database: " << config.database << "\n\n";
		for (const auto &listener : config.listeners) {
			std::cout << (listener.otap ? "OTAP " : "OTLP ") << listener.transport << ": " << listener.uri << '\n';
		}
		// Say WHICH setting chose the listener set. Without this a narrowed set (for example a
		// stray OTEL_EXPORTER_OTLP_PROTOCOL in the shell turning off the gRPC listener) looks
		// like the server simply ignored a flag.
		if (!config.transport_selection.empty()) {
			std::cout << "Listeners selected by: " << config.transport_selection << '\n';
		}
		if (config.auth_disabled_for_loopback) {
			std::cout << "\nAuthentication is DISABLED: no token was configured and every listener is bound to "
			             "loopback, so only this machine can reach them. Set DUCKDB_OTLP_TOKEN (or --token) to "
			             "require a bearer token.\n";
		} else if (config.disable_auth) {
			std::cout << "\nWARNING: authentication is DISABLED by request (--no-auth). Anyone who can reach a "
			             "listener can write to this catalog.\n";
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
		// Always stop + join the watcher, even if startup throws. This is the SOLE owner of the
		// join: Stop() also runs before listener cleanup so shutdown SQL cannot race an interrupt.
		struct WatcherGuard {
			std::atomic<bool> &done;
			std::thread &worker;
			void Stop() {
				done.store(true);
				if (worker.joinable()) {
					worker.join();
				}
			}
			~WatcherGuard() {
				Stop();
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
		// One otlp_serve call starts every listener against one server. If any listener fails to
		// bind, that call closes the listeners it already started and registers nothing; the stop
		// is still attempted on every exit so a server that did register is always drained.
		auto stop_listeners = [&] {
			return TryExecuteOtlpShutdown(con, config.StopOtlpSql(), "otlp shutdown");
		};
		try {
			Execute(con, config.StartOtlpSql(), "otlp startup", true);
			Execute(con, config.StartQuackSql(), "quack startup", true);
			if (!WaitForReady(con, config) && !shutdown_requested) {
				throw std::runtime_error("Timed out waiting for OTLP listener readiness");
			}
			watcher_guard.Stop();

			std::cout << "DuckDB initialization complete\n";
			std::cout << "Starting server..." << '\n';
			auto listener_ok = WaitForShutdownOrListenerFailure(con, config);

			std::cout << "Stopping duckdb-otlp..." << '\n';
			bool shutdown_ok = true;
			if (config.quack_enabled) {
				shutdown_ok = TryExecuteShutdown(con, config.StopQuackSql(), "quack shutdown");
			}
			shutdown_ok = stop_listeners() && shutdown_ok;
			return listener_ok && shutdown_ok ? 0 : 1;
		} catch (...) {
			watcher_guard.Stop();
			if (config.quack_enabled) {
				TryExecuteShutdown(con, config.StopQuackSql(), "quack startup cleanup");
			}
			if (!stop_listeners()) {
				return 1;
			}
			throw;
		}
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

} // namespace

int main(int argc, char **argv) {
	duckdb_otlp_server::CliOptions options;
	try {
		options = duckdb_otlp_server::ParseCli(argc, argv);
	} catch (std::exception &ex) {
		std::cerr << "ERROR: " << duckdb::ErrorData(ex).RawMessage() << '\n';
		std::cerr << "\nRun `duckdb-otlp help` for usage.\n";
		return 1;
	}
	// Flags are applied as environment overrides before any configuration is resolved, so
	// every command sees one consistent `flag > env > default` view.
	duckdb_otlp_server::ApplyEnvOverrides(options);

	using duckdb_otlp_server::Command;
	switch (options.command) {
	case Command::HELP:
		// `help <command>` prints that command's page; a bare `help` prints the overview.
		duckdb_otlp_server::PrintUsage(
		    std::cout, options.inputs.empty() ? Command::HELP : duckdb_otlp_server::CommandFromName(options.inputs[0]));
		return 0;
	case Command::VERSION:
		std::cout << "duckdb-otlp " << duckdb::OtlpExtension().Version() << " (DuckDB "
		          << duckdb::DuckDB::LibraryVersion() << ")\n";
		return 0;
	case Command::HEALTHCHECK:
		return RunHealthCheck();
	case Command::CONVERT:
	case Command::EXPORT:
	case Command::QUERY:
		try {
			switch (options.command) {
			case Command::CONVERT:
				return duckdb_otlp_server::RunConvert(options);
			case Command::EXPORT:
				return duckdb_otlp_server::RunExport(options);
			default:
				return duckdb_otlp_server::RunQuery(options);
			}
		} catch (std::exception &ex) {
			// DuckDB exceptions stringify as a JSON blob; RawMessage() is the plain text.
			std::cerr << "ERROR: " << duckdb::ErrorData(ex).RawMessage() << '\n';
			return 1;
		}
	case Command::VALIDATE:
		// `validate` is `serve` stopped just before anything is opened or bound. Reusing the
		// serve path (rather than a parallel implementation) is what makes it trustworthy:
		// what it prints is exactly what serve would run.
		SetEnv("DRY_RUN", "1");
		return RunServe();
	case Command::SERVE:
	default:
		return RunServe();
	}
}
