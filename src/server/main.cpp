#include "cli.hpp"
#include "env_source.hpp"
#include "server_config.hpp"
#include "server_util.hpp"
#include "storage/otlp_extension.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_uri.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_storage.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_result.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <vector>

namespace {

using duckdb_otlp_server::BindConfigEnvVariables;
using duckdb_otlp_server::CheckResult;
using duckdb_otlp_server::CliErrorMessage;
using duckdb_otlp_server::EnvSource;
using duckdb_otlp_server::Execute;

// Written from a signal handler, so it must be a mutable global volatile sig_atomic_t.
volatile std::sig_atomic_t shutdown_requested = 0; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void HandleSignal(int) {
	shutdown_requested = 1;
}

void InstallSignalHandlers() {
	std::signal(SIGINT, HandleSignal);
	std::signal(SIGTERM, HandleSignal);
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

// otlp_stop seals buffered rows and returns (status, dropped_rows). Returns false on an error OR
// on dropped rows, so the exit code tells an orchestrator a clean shutdown from a data-losing one.
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
	bool listening = false;
	duckdb::string last_error;
};

OtlpHealth QueryOtlpHealth(duckdb::Connection &con, const duckdb_otlp_server::IngestListener &listener) {
	auto result =
	    con.Query("SELECT is_listening, coalesce(last_error, '') FROM otlp_server_list() WHERE listen_uri = " +
	              duckdb::SqlQuote(listener.uri));
	CheckResult(*result, "otlp readiness");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return {false, "listener is not registered"};
	}
	return {chunk->GetValue(0, 0).GetValue<bool>(), chunk->GetValue(1, 0).GetValue<duckdb::string>()};
}

bool WaitForReady(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(config.startup_timeout_secs);
	while (!shutdown_requested && std::chrono::steady_clock::now() < deadline) {
		bool ready = true;
		for (const auto &listener : config.listeners) {
			auto health = QueryOtlpHealth(con, listener);
			ready = ready && health.listening;
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

//! Block until a signal arrives, checking once a second that every listener is still up. Seal
//! and maintenance outcomes are logged by the server itself (see OtlpEventsToStderr).
bool WaitForShutdownOrListenerFailure(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	for (duckdb::idx_t ticks = 1; !shutdown_requested; ticks++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
		if (ticks % 4 != 0) {
			continue;
		}
		for (const auto &listener : config.listeners) {
			auto health = QueryOtlpHealth(con, listener);
			if (!health.listening) {
				std::cerr << "ERROR: OTLP listener " << listener.uri << " stopped or disappeared: " << health.last_error
				          << '\n';
				return false;
			}
		}
	}
	return true;
}

//! Prints the extension's OTLP log entries to stderr, so seal, maintenance and listener errors
//! reach the process log (`docker logs`) as they happen.
class StderrLogStorage : public duckdb::LogStorage {
public:
	static constexpr const char *NAME = "duckdb_otlp_stderr";
	const duckdb::string GetStorageName() override {
		return NAME;
	}
	void WriteLogEntry(duckdb::timestamp_t, duckdb::LogLevel level, const duckdb::string &,
	                   const duckdb::string &message, const duckdb::RegisteredLoggingContext &) override {
		std::cerr << (level >= duckdb::LogLevel::LOG_WARNING ? "WARNING: " : "") << message << '\n';
	}
	void WriteLogEntries(duckdb::DataChunk &, const duckdb::RegisteredLoggingContext &) override {
	}
	void FlushAll() override {
	}
	void Flush(duckdb::LoggingTargetTable) override {
	}
	bool IsEnabled(duckdb::LoggingTargetTable) override {
		return true;
	}
};

void OtlpEventsToStderr(duckdb::DatabaseInstance &db) {
	auto &log_manager = db.GetLogManager();
	duckdb::shared_ptr<duckdb::LogStorage> storage = duckdb::make_shared_ptr<StderrLogStorage>();
	log_manager.RegisterLogStorage(StderrLogStorage::NAME, storage);
	log_manager.SetLogStorage(db, StderrLogStorage::NAME);
	std::unordered_set<duckdb::string> types {"OTLP"};
	log_manager.SetEnabledLogTypes(&types);
	log_manager.SetLogMode(duckdb::LogMode::ENABLE_SELECTED);
	log_manager.SetLogLevel(duckdb::LogLevel::LOG_INFO);
	log_manager.SetEnableLogging(true);
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

// Host portion of a "host:port" / "[ipv6]:port" bind address (no scheme).
duckdb::string HostFromAddr(const duckdb::string &addr) {
	if (!addr.empty() && addr[0] == '[') {
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

// The host to reach a server bound to `host` on: a wildcard bind is reachable on the loopback of
// its own family, an explicit interface is not and must be used directly. The probes and the
// banner all go through this one rule.
duckdb::string ReachableHost(const duckdb::string &host) {
	if (host.empty() || host == "0.0.0.0") {
		return "127.0.0.1";
	}
	if (host == "::" || host == "[::]") {
		return "::1";
	}
	return host;
}

duckdb::string HealthCheckHost(const duckdb::string &addr) {
	return ReachableHost(HostFromAddr(addr));
}

duckdb::string ListenerLabel(const duckdb_otlp_server::IngestListener &listener) {
	return duckdb::string("OTLP ") + listener.transport;
}

// Quack serves TLS (self-signed by default) on a non-loopback bind, so clients must pin the
// fingerprint printed here. Advisory only: a failure is a warning, never a reason to stop serving.
void PrintQuackEndpoint(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	auto result = con.Query(config.QuackEndpointSql());
	if (result->HasError()) {
		std::cerr << "WARNING: could not read the Quack endpoint from quack_server_list(): " << result->GetError()
		          << '\n';
		return;
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}
	auto fingerprint = chunk->GetValue(1, 0).GetValue<duckdb::string>();
	auto host = ReachableHost(HostFromAddr(config.quack_http_addr));
	if (host.find(':') != duckdb::string::npos) {
		host = "[" + host + "]";
	}
	auto uri = "quack:" + host + ":" + std::to_string(PortFromAddr(config.quack_http_addr, 9494));
	std::cout << "\nQuery over Quack at " << chunk->GetValue(0, 0).GetValue<duckdb::string>() << '\n';
	if (fingerprint.empty()) {
		std::cout << "  ATTACH '" << uri << "' AS otel (TYPE quack);\n";
		return;
	}
	std::cout << "TLS certificate SHA-256 fingerprint: " << fingerprint << '\n'
	          << "  ATTACH '" << uri << "' AS otel (TYPE quack, SSL_FINGERPRINT '" << fingerprint << "');\n";
}

// TCP connect, not an HTTP GET: Quack serves TLS on a non-loopback bind and this binary carries
// no TLS client.
bool QuackProbe(const duckdb::string &addr) {
	return duckdb_otlp_server::ProbeListener(HealthCheckHost(addr), PortFromAddr(addr, 9494));
}

// Collected rather than printed as found, so --json can emit one object.
struct DoctorCheck {
	duckdb::string what;
	duckdb::string endpoint;
	bool ok = false;
};

void PrintDoctorReport(const std::vector<DoctorCheck> &checks, bool healthy, bool as_json) {
	if (!as_json) {
		for (const auto &check : checks) {
			std::cout << (check.ok ? "ok    " : "FAIL  ") << check.what << "  " << check.endpoint
			          << (check.ok ? "" : "  (no response)") << '\n';
		}
		return;
	}
	// Hand-rolled: a fixed shape, and `doctor` must not need a database open to answer.
	std::cout << "{\"healthy\":" << (healthy ? "true" : "false") << ",\"checks\":[";
	for (duckdb::idx_t i = 0; i < checks.size(); i++) {
		const auto &check = checks[i];
		std::cout << (i ? "," : "") << "{\"check\":\"" << check.what << "\",\"endpoint\":\"" << check.endpoint
		          << "\",\"ok\":" << (check.ok ? "true" : "false") << "}";
	}
	std::cout << "]}\n";
}

// `doctor` (also `healthcheck`, the container HEALTHCHECK; the image has no shell/curl). Every
// check runs even after one fails, since Docker keeps this output in the health log. Never reads
// --init-sql: it runs on every probe. Exits 0 only when every check passed.
int RunDoctor(const EnvSource &env, bool as_json) {
	std::vector<DoctorCheck> checks;
	try {
		for (const auto &listener : duckdb_otlp_server::ListenersFromEnv(env)) {
			duckdb::OtlpUri uri(listener.uri);
			auto host = ReachableHost(uri.Host());
			// gRPC speaks HTTP/2 and has no /readyz, so a connect is all it can answer.
			bool ok =
			    duckdb_otlp_server::ProbeListener(host, uri.Port(), listener.transport == "grpc" ? "" : "/readyz");
			checks.push_back({ListenerLabel(listener), host + ":" + std::to_string(uri.Port()), ok});
		}
	} catch (std::exception &ex) {
		// Unresolvable configuration is a broken setup, not an unreachable listener.
		std::cerr << "ERROR: " << CliErrorMessage(ex) << '\n';
		return 1;
	}
	auto quack_addr = duckdb_otlp_server::QuackAddrFromEnv(env);
	if (!quack_addr.empty()) {
		checks.push_back({"Quack", quack_addr, QuackProbe(quack_addr)});
	}
	bool healthy = std::all_of(checks.begin(), checks.end(), [](const DoctorCheck &check) { return check.ok; });
	PrintDoctorReport(checks, healthy, as_json);
	return healthy ? 0 : 1;
}

int RunServe(const EnvSource &env) {
	try {
		auto config = duckdb_otlp_server::ServerConfig::FromEnv(env);

		std::cout << (config.dry_run ? "Checking duckdb-otlp configuration\n\n" : "Starting duckdb-otlp server\n\n");
		std::cout << "Mode: " << config.mode << "\n";
		if (!config.data_location.empty()) {
			std::cout << "Data: " << config.data_location << "\n";
		}
		if (!config.credentials_source.empty()) {
			std::cout << "Credentials: " << config.credentials_source << "\n";
		}
		if (!config.secret_dir.empty()) {
			std::cout << "Secrets: " << config.secret_dir << "\n";
		}
		std::cout << "Database: " << config.database << " (control)\n\n";
		for (const auto &listener : config.listeners) {
			std::cout << ListenerLabel(listener) << ": " << listener.uri << '\n';
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
			std::cout << "WARNING: Quack grants full SQL read/write access to every attached catalog.\n";
		} else {
			std::cout << "Quack: disabled\n";
		}
		if (!config.init_sql_path.empty()) {
			std::cout << "Init SQL: " << config.init_sql_path << "\n";
		}
		std::cout << '\n';

		if (config.dry_run) {
			std::cout << "DRY_RUN=1; planned initialization only.\n\n";
			std::cout << "Generated initialization SQL:\n";
			for (const auto &step : config.SetupSteps()) {
				std::cout << step.sql;
			}
			for (const auto &step : config.ServeSteps()) {
				std::cout << step.sql;
			}
			std::cout << '\n';
			return 0;
		}

		duckdb::DBConfig db_config;
		db_config.SetOptionByName("allow_unsigned_extensions",
		                          duckdb::Value::BOOLEAN(config.allow_unsigned_extensions));
		duckdb::DuckDB db(config.database, &db_config);
		db.LoadStaticExtension<duckdb::OtlpExtension>();
		OtlpEventsToStderr(*db.instance);
		duckdb::Connection con(db);
		// Tokens and the preset's env vars are bound as session variables (read back with
		// getvariable()), so secrets never appear in the generated SQL that DRY_RUN prints and errors echo.
		con.context->config.SetUserVariable("duckdb_otlp_effective_token", duckdb::Value(config.token));
		con.context->config.SetUserVariable("duckdb_quack_effective_token", duckdb::Value(config.quack_token));
		BindConfigEnvVariables(con, config, env);

		InstallSignalHandlers();

		// The signal flag is only observed between statements and a remote ATTACH/INSTALL can block
		// for long, so a watcher thread interrupts the connection as soon as a signal arrives.
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
		// Sole owner of the join. Stop() runs before listener cleanup so shutdown SQL cannot race an
		// interrupt.
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

		std::cout << "Setting up " << config.mode << " (installing extensions, attaching catalog)...\n";
		for (const auto &step : config.SetupSteps()) {
			Execute(con, step.sql, step.label);
		}
		// Instance-wide default, not `USE`: Quack serves each client on a fresh Connection, which copies
		// the DatabaseManager default, so Quack clients resolve the signal tables unqualified.
		if (!config.catalog.empty()) {
			duckdb::DatabaseManager::Get(*con.context).SetDefaultDatabase(duckdb::Identifier(config.catalog));
		}
		// Attempted on every exit, so a server that did register is always drained.
		auto stop_listeners = [&] {
			return TryExecuteOtlpShutdown(con, config.StopOtlpSql(), "otlp shutdown");
		};
		try {
			for (const auto &step : config.ServeSteps()) {
				Execute(con, step.sql, step.label);
			}
			if (!WaitForReady(con, config) && !shutdown_requested) {
				throw std::runtime_error("Timed out waiting for OTLP listener readiness");
			}
			watcher_guard.Stop();

			std::cout << "DuckDB initialization complete\n";
			std::cout << "Starting server..." << '\n';
			for (const auto &listener : config.listeners) {
				if (listener.transport == "http") {
					duckdb::OtlpUri uri(listener.uri);
					auto host = ReachableHost(uri.Host());
					std::cout << "\nSend OTLP/HTTP to http://" << host << ":" << uri.Port()
					          << "/v1/{logs,traces,metrics}\n";
					break;
				}
			}
			if (config.quack_enabled) {
				PrintQuackEndpoint(con, config);
			} else {
				std::cout << "Query it while the server runs by enabling Quack (--quack PORT --quack-token TOKEN)\n";
			}
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
		std::cerr << "ERROR: " << CliErrorMessage(ex) << '\n';
		return 1;
	}
}

} // namespace

int main(int argc, char **argv) {
	// std::cout is fully buffered when stdout is not a TTY, which held the banner out of
	// `docker logs` until exit. Flush per write.
	std::cout << std::unitbuf;

	duckdb_otlp_server::CliOptions options;
	try {
		options = duckdb_otlp_server::ParseCli(argc, argv);
	} catch (std::exception &ex) {
		auto message = CliErrorMessage(ex);
		std::cerr << "ERROR: " << message << '\n';
		if (message.find("duckdb-otlp help") == duckdb::string::npos) {
			std::cerr << "\nRun `duckdb-otlp help` for usage.\n";
		}
		// 2, not 1: a wrong command line, as opposed to work that failed (the getopt convention).
		return 2;
	}
	const EnvSource env(options.env_overrides);

	using duckdb_otlp_server::Command;
	switch (options.command) {
	case Command::HELP:
		duckdb_otlp_server::PrintUsage(std::cout, options.help_topic.empty()
		                                              ? Command::HELP
		                                              : duckdb_otlp_server::CommandFromName(options.help_topic));
		return 0;
	case Command::VERSION:
		std::cout << "duckdb-otlp " << duckdb::OtlpExtension().Version() << " (DuckDB "
		          << duckdb::DuckDB::LibraryVersion() << ")\n";
		return 0;
	case Command::DOCTOR:
		return RunDoctor(env, options.json_output);
	case Command::VALIDATE:
	// `validate` is `serve` with DRY_RUN (set by ParseCli), so it prints exactly what serve would run.
	case Command::SERVE:
	default:
		return RunServe(env);
	}
}
