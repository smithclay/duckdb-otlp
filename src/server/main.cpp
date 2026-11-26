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
#include <thread>
#include <unordered_set>
#include <vector>

namespace {

using duckdb_otlp_server::BindAddr;
using duckdb_otlp_server::BindConfigEnvVariables;
using duckdb_otlp_server::CheckResult;
using duckdb_otlp_server::CliErrorMessage;
using duckdb_otlp_server::EnvSource;
using duckdb_otlp_server::Execute;

// ClientContext::Interrupt() only updates interrupt_state. Every operation in the handler
// must be lock-free (atomic_flag is guaranteed lock-free).
static_assert(std::atomic<duckdb::ClientInterruptState>::is_always_lock_free);
static_assert(std::atomic<duckdb::ClientContext *>::is_always_lock_free);
static_assert(std::atomic<bool>::is_always_lock_free);
std::atomic<bool> shutdown_requested {false};
std::atomic<duckdb::ClientContext *> startup_context {nullptr};
std::atomic_flag startup_interrupt_active = ATOMIC_FLAG_INIT;

void HandleSignal(int) {
	shutdown_requested.store(true);
	if (!startup_interrupt_active.test_and_set()) {
		if (auto *context = startup_context.load()) {
			context->Interrupt();
		}
		startup_interrupt_active.clear();
	}
}

void InstallSignalHandlers() {
	std::signal(SIGINT, HandleSignal);
	std::signal(SIGTERM, HandleSignal);
}

bool StopServer(duckdb::Connection &con, const duckdb::string &sql, const char *label, bool reports_dropped_rows) {
	try {
		auto result = con.Query(sql);
		CheckResult(*result, label);
		result->Print();
		uint64_t dropped_rows = 0;
		if (reports_dropped_rows) {
			for (const auto &row : *result) {
				dropped_rows += row.GetValue<uint64_t>(1);
			}
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

//! Block until a signal arrives, checking once a second that every listener is still up. Seal
//! and maintenance outcomes are logged by the server itself (see OtlpEventsToStderr).
bool WaitForShutdownOrListenerFailure(duckdb::Connection &con, const duckdb_otlp_server::ServerConfig &config) {
	duckdb::vector<duckdb::string> quoted_uris;
	for (const auto &listener : config.listeners) {
		quoted_uris.push_back(duckdb::SqlQuote(listener.uri));
	}
	auto uris = duckdb::StringUtil::Join(quoted_uris, ", ");
	auto count_sql = "SELECT count(*) FROM otlp_server_list() WHERE is_listening AND listen_uri IN (" + uris + ")";
	for (duckdb::idx_t ticks = 1; !shutdown_requested.load(); ticks++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
		if (ticks % 4 != 0 || shutdown_requested.load()) {
			continue;
		}
		auto count = con.Query(count_sql);
		CheckResult(*count, "otlp listener health");
		if (count->Fetch()->GetValue(0, 0).GetValue<duckdb::idx_t>() == config.listeners.size()) {
			continue;
		}
		auto failures = con.Query(duckdb::StringUtil::Format(R"SQL(
SELECT uri, CASE WHEN listen_uri IS NULL THEN 'listener is not registered' ELSE coalesce(last_error, '') END
FROM unnest([%s]) AS expected(uri)
LEFT JOIN otlp_server_list() ON uri = listen_uri
WHERE is_listening IS DISTINCT FROM true
)SQL",
		                                                     uris));
		CheckResult(*failures, "otlp listener health");
		for (const auto &row : *failures) {
			std::cerr << "ERROR: OTLP listener " << row.GetValue<duckdb::string>(0)
			          << " stopped or disappeared: " << row.GetValue<duckdb::string>(1) << '\n';
		}
		return false;
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

// The host to reach a server bound to `host` on: a wildcard bind is reachable on the loopback of
// its own family, an explicit interface is not and must be used directly. The probes and the
// banner all go through this one rule.
duckdb::string ReachableHost(const duckdb::string &host) {
	if (host.empty() || host == "0.0.0.0") {
		return "127.0.0.1";
	}
	if (host == "::") {
		return "::1";
	}
	return host;
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
	auto uri = "quack:" + BindAddr(ReachableHost(config.host), config.quack_port);
	std::cout << "\nQuery over Quack at " << chunk->GetValue(0, 0).GetValue<duckdb::string>() << '\n';
	if (fingerprint.empty()) {
		std::cout << "  ATTACH '" << uri << "' AS otel (TYPE quack);\n";
		return;
	}
	std::cout << "TLS certificate SHA-256 fingerprint: " << fingerprint << '\n'
	          << "  ATTACH '" << uri << "' AS otel (TYPE quack, SSL_FINGERPRINT '" << fingerprint << "');\n";
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
		auto bind_host = duckdb_otlp_server::ListenHostFromEnv(env).Host();
		auto host = ReachableHost(bind_host);
		for (const auto &listener : duckdb_otlp_server::ListenersFromEnv(env, bind_host)) {
			// gRPC speaks HTTP/2 and has no /readyz, so a connect is all it can answer.
			bool ok =
			    duckdb_otlp_server::ProbeListener(host, listener.port, listener.transport == "grpc" ? "" : "/readyz");
			checks.push_back({ListenerLabel(listener), BindAddr(host, listener.port), ok});
		}
		auto quack_port = duckdb_otlp_server::QuackPortFromEnv(env);
		if (quack_port != 0) {
			// Quack serves TLS on a non-loopback bind. Probe with a TCP connection.
			checks.push_back(
			    {"Quack", BindAddr(host, quack_port), duckdb_otlp_server::ProbeListener(host, quack_port)});
		}
	} catch (std::exception &ex) {
		// Unresolvable configuration is a broken setup, not an unreachable listener.
		std::cerr << "ERROR: " << CliErrorMessage(ex) << '\n';
		return 1;
	}
	bool healthy = std::all_of(checks.begin(), checks.end(), [](const DoctorCheck &check) { return check.ok; });
	PrintDoctorReport(checks, healthy, as_json);
	return healthy ? 0 : 1;
}

int RunServe(const EnvSource &env, bool validate) {
	try {
		auto config = duckdb_otlp_server::ServerConfig::FromEnv(env);

		std::cout << (validate ? "Checking duckdb-otlp configuration\n\n" : "Starting duckdb-otlp server\n\n");
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
		if (config.quack_port != 0) {
			std::cout << "Quack: " << config.QuackUri() << "\n\n";
			std::cout << "WARNING: Quack grants full SQL read/write access to every attached catalog.\n";
		} else {
			std::cout << "Quack: disabled\n";
		}
		if (!config.init_sql_path.empty()) {
			std::cout << "Init SQL: " << config.init_sql_path << "\n";
		}
		std::cout << '\n';

		if (validate) {
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
		// getvariable()), so secrets never appear in the generated SQL that validate prints and errors echo.
		con.context->config.SetUserVariable("duckdb_otlp_effective_token", duckdb::Value(config.token));
		con.context->config.SetUserVariable("duckdb_quack_effective_token", duckdb::Value(config.quack_token));
		BindConfigEnvVariables(con, config, env);

		InstallSignalHandlers();
		startup_context.store(con.context.get());

		auto execute_steps = [&con](const std::vector<duckdb_otlp_server::BootStep> &steps) {
			for (const auto &step : steps) {
				if (shutdown_requested.load()) {
					return;
				}
				Execute(con, step.sql, step.label);
			}
		};
		bool started = false;
		bool run_ok = true;
		try {
			std::cout << "Setting up " << config.mode << " (installing extensions, attaching catalog)...\n";
			execute_steps(config.SetupSteps());
			// Quack opens a new connection per client. Set the instance-wide default so clients
			// can resolve signal tables without qualifying their catalog.
			if (!shutdown_requested.load() && !config.catalog.empty()) {
				duckdb::DatabaseManager::Get(*con.context).SetDefaultDatabase(duckdb::Identifier(config.catalog));
			}
			execute_steps(config.ServeSteps());
			startup_context.store(nullptr);

			if (!shutdown_requested.load()) {
				started = true;
				std::cout << "DuckDB initialization complete\n";
				std::cout << "Starting server..." << '\n';
				for (const auto &listener : config.listeners) {
					if (listener.transport == "http") {
						std::cout << "\nSend OTLP/HTTP to http://"
						          << BindAddr(ReachableHost(config.host), listener.port)
						          << "/v1/{logs,traces,metrics}\n";
						break;
					}
				}
				if (config.quack_port != 0) {
					PrintQuackEndpoint(con, config);
				} else {
					std::cout
					    << "Query it while the server runs by enabling Quack (--quack PORT --quack-token TOKEN)\n";
				}
				run_ok = WaitForShutdownOrListenerFailure(con, config);
			}
		} catch (const std::exception &ex) {
			if (started || !shutdown_requested.load()) {
				std::cerr << "ERROR: " << CliErrorMessage(ex) << '\n';
				run_ok = false;
			}
		}

		// A signal handler on another thread may already have read startup_context.
		// Let that interrupt finish before clearing it and running shutdown SQL.
		startup_context.store(nullptr);
		while (startup_interrupt_active.test_and_set()) {
			std::this_thread::yield();
		}
		startup_interrupt_active.clear();
		con.context->ClearInterrupt();
		// Interrupted init SQL can leave an explicit transaction aborted.
		if (con.HasActiveTransaction()) {
			try {
				con.Rollback();
			} catch (const std::exception &ex) {
				std::cerr << "ERROR during shutdown rollback: " << CliErrorMessage(ex) << '\n';
				run_ok = false;
			}
		}

		if (!started && shutdown_requested.load()) {
			std::cerr << "Shutdown requested during startup; exiting before the server became ready.\n";
		}
		std::cout << "Stopping duckdb-otlp...\n";
		const bool quack_ok = config.quack_port == 0 || !db.ExtensionIsLoaded("quack") ||
		                      StopServer(con, config.StopQuackSql(), "quack shutdown", false);
		const bool otlp_ok = StopServer(con, config.StopOtlpSql(), "otlp shutdown", true);
		return run_ok && quack_ok && otlp_ok ? 0 : 1;
	} catch (std::exception &ex) {
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
	case Command::SERVE:
	default:
		return RunServe(env, options.command == Command::VALIDATE);
	}
}
