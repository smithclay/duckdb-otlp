#include "cli.hpp"
#include "commands.hpp"
#include "env_source.hpp"
#include "server_config.hpp"
#include "server_util.hpp"
#include "storage/otlp_extension.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_uri.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
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
#include <vector>

namespace duckdb {
// Defined in otlp_server.cpp (linked into the daemon). Declared here rather than including
// otlp_server.hpp, whose transitive includes are not on the daemon's include path.
bool OtlpHttpStatusOk(const string &host, int port, const string &path);
bool OtlpTcpConnectOk(const string &host, int port);
} // namespace duckdb

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

// The host to reach a server bound to `host` on. A wildcard/unspecified bind is reachable on
// the loopback of its own family; an explicit interface (192.168.1.5) is NOT reachable on
// loopback and must be used directly, or the container HEALTHCHECK fails forever on a healthy
// server (review finding M5).
//
// One definition on purpose: this rule had grown three spellings in this file — the listener
// probe, the Quack probe and the startup banner's endpoint hint — which disagreed about `::`
// and about the empty host, so a change to the rule would have fixed one and left two.
duckdb::string ReachableHost(const duckdb::string &host) {
	if (host.empty() || host == "0.0.0.0") {
		return "127.0.0.1";
	}
	if (host == "::" || host == "[::]") {
		return "::1";
	}
	return host;
}

// The same rule for a "host:port" bind address rather than a bare host.
duckdb::string HealthCheckHost(const duckdb::string &addr) {
	return ReachableHost(HostFromAddr(addr));
}

// How a listener is named in the doctor report and in the startup banner.
duckdb::string ListenerLabel(const duckdb_otlp_server::IngestListener &listener) {
	return duckdb::string(listener.otap ? "OTAP " : "OTLP ") + listener.transport;
}

// Probe GET http://<host>:<port><path>. Loopback for a wildcard host, the configured host
// otherwise (review finding M5).
bool HealthProbe(const duckdb::string &addr, const duckdb::string &path, int port_fallback) {
	auto host = HealthCheckHost(addr);
	auto port = PortFromAddr(addr, port_fallback);
	return duckdb::OtlpHttpStatusOk(host, port, path);
}

// One check's result. Collected rather than printed as it is found, so --json can emit one
// object and the prose form still reports every check rather than only the first failure.
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
	// Hand-rolled rather than pulled from DuckDB's JSON: the fields are a fixed shape with no
	// user-supplied text beyond a host and a transport name, and `doctor` must not need a
	// database open to answer.
	std::cout << "{\"healthy\":" << (healthy ? "true" : "false") << ",\"checks\":[";
	for (duckdb::idx_t i = 0; i < checks.size(); i++) {
		const auto &check = checks[i];
		std::cout << (i ? "," : "") << "{\"check\":\"" << check.what << "\",\"endpoint\":\"" << check.endpoint
		          << "\",\"ok\":" << (check.ok ? "true" : "false") << "}";
	}
	std::cout << "]}\n";
}

// `doctor` (and its `healthcheck` spelling, which the container HEALTHCHECK and existing
// compose probes call): check every configured listener and report each one. Distroless
// images ship no shell/curl, so the daemon probes itself. It probes the CONFIGURED bind host
// (loopback for a 0.0.0.0/:: wildcard bind, the explicit interface otherwise — see
// HealthCheckHost), so a non-loopback bind is supported without a forever-failing loopback
// probe. HTTP listeners use /readyz, gRPC listeners use TCP connect, and Quack is checked
// when enabled.
//
// Every check runs even after one fails: a partial answer ("http is up, grpc is not") is the
// whole point of the command, and Docker keeps this output in the container's health log.
// Exits 0 only when every check passed.
int RunDoctor(const EnvSource &env, bool as_json) {
	std::vector<DoctorCheck> checks;
	try {
		for (const auto &listener : duckdb_otlp_server::ListenersFromEnv(env)) {
			duckdb::OtlpUri uri(listener.uri);
			auto host = ReachableHost(uri.Host());
			bool ok = listener.transport == "grpc" ? duckdb::OtlpTcpConnectOk(host, uri.Port())
			                                       : duckdb::OtlpHttpStatusOk(host, uri.Port(), "/readyz");
			checks.push_back({ListenerLabel(listener), host + ":" + std::to_string(uri.Port()), ok});
		}
	} catch (std::exception &ex) {
		// Configuration that cannot even be resolved is not a failed check, it is a broken
		// setup: report it as an error rather than as an unreachable listener.
		std::cerr << "ERROR: " << CliErrorMessage(ex) << '\n';
		return 1;
	}
	if (duckdb_otlp_server::QuackEnabledFromEnv(env)) {
		// Resolved by the same function startup uses, so the probe cannot target a different
		// port than the server bound.
		auto quack_addr = duckdb_otlp_server::QuackAddrFromEnv(env);
		checks.push_back({"Quack", quack_addr, HealthProbe(quack_addr, "/", 9494)});
	}
	// Derived rather than tracked: a check appended without a companion `healthy &&= ok` would
	// have printed "healthy" next to a FAIL, and nothing local showed the invariant.
	bool healthy = std::all_of(checks.begin(), checks.end(), [](const DoctorCheck &check) { return check.ok; });
	PrintDoctorReport(checks, healthy, as_json);
	return healthy ? 0 : 1;
}

// The `serve` subcommand (and the bare invocation, which means the same thing): resolve
// configuration, run mode setup, start every listener, then block until a signal arrives.
int RunServe(const EnvSource &env) {
	try {
		auto config = duckdb_otlp_server::ServerConfig::FromEnv(env);

		// `validate` runs this same path with DRY_RUN set, and announcing a server it will
		// never start made its output read like a successful launch in CI logs.
		std::cout << (config.dry_run ? "Checking duckdb-otlp configuration\n\n" : "Starting duckdb-otlp server\n\n");
		std::cout << "Mode: " << config.mode << "\n";
		if (!config.data_location.empty()) {
			// Where telemetry actually lands. The control database below is a small
			// bookkeeping file, so naming only that sent people looking in the wrong place.
			std::cout << "Data: " << config.data_location << "\n";
		}
		std::cout << "Database: " << config.database << " (control)\n\n";
		for (const auto &listener : config.listeners) {
			std::cout << ListenerLabel(listener) << ": " << listener.uri << '\n';
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
		if (!config.init_sql_path.empty()) {
			std::cout << "Init SQL: " << config.init_sql_path << "\n";
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
		BindConfigEnvVariables(con, config, env);

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

		// Mode setup installs extensions and attaches the catalog, which on a cold cache means
		// downloads: the longest silence in a cold start, and the one most likely to look hung.
		std::cout << "Setting up " << config.mode << " (installing extensions, attaching catalog)...\n";
		Execute(con, config.mode_setup_sql, "mode setup");
		// Operator SQL, after the mode's ATTACH so it can build on the telemetry catalog, and
		// before SetDefaultDatabase so a catalog the script itself attaches can still be named
		// by --catalog. Failures are fatal: a script that was meant to attach a second catalog
		// or widen a memory limit has no safe "carry on without it" reading.
		if (!config.init_sql.empty()) {
			std::cout << "Running init SQL from " << config.init_sql_path << "...\n";
			Execute(con, config.init_sql, "init SQL");
		}
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
			// Not printed: otlp_serve returns a listener table, which the banner above has
			// already said in prose. Dumping it made the one thing visible in a redirected
			// log a bare SQL result with no context.
			Execute(con, config.StartOtlpSql(), "otlp startup");
			Execute(con, config.StartQuackSql(), "quack startup");
			if (!WaitForReady(con, config) && !shutdown_requested) {
				throw std::runtime_error("Timed out waiting for OTLP listener readiness");
			}
			watcher_guard.Stop();

			std::cout << "DuckDB initialization complete\n";
			std::cout << "Starting server..." << '\n';
			// "It is running" is only half of what a first run needs to know; without this the
			// next step (send something, then read it back) was not discoverable from here.
			for (const auto &listener : config.listeners) {
				if (listener.transport == "http" && !listener.otap) {
					duckdb::OtlpUri uri(listener.uri);
					auto host = ReachableHost(uri.Host());
					std::cout << "\nSend OTLP/HTTP to http://" << host << ":" << uri.Port()
					          << "/v1/{logs,traces,metrics}\n";
					break;
				}
			}
			std::cout << "Read it back with: duckdb-otlp query \"SELECT * FROM otlp_logs LIMIT 10\"\n";
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
		// DuckDB exceptions stringify as a JSON blob, and the mode setup SQL is generated
		// here, so neither the blob nor the echoed statement helps the reader.
		std::cerr << "ERROR: " << CliErrorMessage(ex) << '\n';
		return 1;
	}
}

// convert/export/query differ only in which function runs; they share one error rendering,
// so the dispatch names each command exactly once. `convert` is stateless and takes no
// EnvSource, hence a callable rather than a uniform function pointer.
//
// `keep_sql_context` is true only for `query`, the one command whose failing statement the
// user actually wrote: there DuckDB's "LINE 1: ... ^" echo points at their own typo. For
// convert and export the statement is generated, so the echo showed the reader a COPY(...)
// they never asked for instead of the missing file or table.
template <typename Fn>
int RunDataCommand(Fn &&run, bool keep_sql_context = false) {
	try {
		return run();
	} catch (std::exception &ex) {
		// DuckDB exceptions stringify as a JSON blob; both helpers give the plain text.
		std::cerr << "ERROR: " << (keep_sql_context ? duckdb::ErrorData(ex).RawMessage() : CliErrorMessage(ex)) << '\n';
		return 1;
	}
}

} // namespace

int main(int argc, char **argv) {
	// std::cout is FULLY buffered when stdout is not a terminal, so a redirected or captured
	// stdout held the whole startup banner until the process exited: `docker logs` on a
	// healthy container showed a raw listener table and nothing else, and the mode, data
	// location, listeners and auth notice only appeared once the container stopped. Flush
	// per write — this CLI's stdout volume is banners and small result sets, and bulk data
	// goes through DuckDB's own COPY handle rather than here.
	std::cout << std::unitbuf;

	duckdb_otlp_server::CliOptions options;
	try {
		options = duckdb_otlp_server::ParseCli(argc, argv);
	} catch (std::exception &ex) {
		auto message = CliErrorMessage(ex);
		std::cerr << "ERROR: " << message << '\n';
		// The unknown-command message names the right next step itself; everything else gets
		// the generic pointer rather than two conflicting suggestions.
		if (message.find("duckdb-otlp help") == duckdb::string::npos) {
			std::cerr << "\nRun `duckdb-otlp help` for usage.\n";
		}
		// 2, not 1: a wrong command line is a different thing from work that failed, and a
		// caller that gets 1 for everything has to parse stderr to tell them apart. This is
		// the usual split (getopt-style tools exit 2 on usage), and it is the only one the
		// CLI can make honestly — everything past argv parsing is genuine runtime failure.
		return 2;
	}
	// Flag values are layered over the process environment rather than written into it, so
	// every command resolves configuration from one consistent `flag > env > default` view
	// and a --token is never published through getenv() to the rest of the process.
	const EnvSource env(options.env_overrides);

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
	case Command::DOCTOR:
		return RunDoctor(env, options.json_output);
	case Command::CONVERT:
		return RunDataCommand([&] { return duckdb_otlp_server::RunConvert(options); });
	case Command::EXPORT:
		return RunDataCommand([&] { return duckdb_otlp_server::RunExport(options, env); });
	case Command::QUERY:
		return RunDataCommand([&] { return duckdb_otlp_server::RunQuery(options, env); }, true);
	case Command::VALIDATE:
	// `validate` is `serve` stopped just before anything is opened or bound (ParseCli sets
	// DRY_RUN for it). Reusing the serve path rather than a parallel implementation is what
	// makes it trustworthy: what it prints is exactly what serve would run.
	case Command::SERVE:
	default:
		return RunServe(env);
	}
}
