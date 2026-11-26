#include "server_config.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "env_source.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_start_stop.hpp"
#include "otlp_uri.hpp"
#include "server_util.hpp"

#include <cstring>
#include <filesystem>
#include <map>
#include <sstream>
#include <utility>

namespace duckdb_otlp_server {
namespace {

using duckdb::InvalidInputException;
using duckdb::QuoteIdentifier;
using duckdb::SqlQuote;
using duckdb::string;
using duckdb::StringUtil;

//! Loopback, not a wildcard: the CLI runs on laptops. The container image sets 0.0.0.0.
constexpr const char *DEFAULT_HOST = "127.0.0.1";
constexpr int DEFAULT_HTTP_PORT = 4318;
constexpr int DEFAULT_GRPC_PORT = 4317;

//! The container pins this to /data; everywhere else it follows the XDG base-directory spec.
string DefaultDataDir(const EnvSource &env) {
	auto xdg = env.Get("XDG_DATA_HOME");
	if (!xdg.empty()) {
		return xdg + "/duckdb-otlp";
	}
	auto home = env.Get("HOME");
	return home.empty() ? "./duckdb-otlp-data" : home + "/.local/share/duckdb-otlp";
}

//! The bearer token in an OTEL_EXPORTER_OTLP_HEADERS value ("k=v,authorization=Bearer t").
string BearerTokenFromOtelHeaders(const string &headers) {
	for (const auto &entry : StringUtil::Split(headers, ',')) {
		auto eq = entry.find('=');
		if (eq == string::npos) {
			continue;
		}
		auto key = entry.substr(0, eq);
		auto value = entry.substr(eq + 1);
		StringUtil::Trim(key);
		StringUtil::Trim(value);
		if (!StringUtil::CIEquals(key, "authorization")) {
			continue;
		}
		auto space = value.find(' ');
		if (space != string::npos && StringUtil::CIEquals(value.substr(0, space), "bearer")) {
			value = value.substr(space + 1);
			StringUtil::Trim(value);
		}
		return value;
	}
	return "";
}

//! Reject OTLP exporter variables this server cannot honor, rather than silently ignore them.
//! OTEL_EXPORTER_OTLP_ENDPOINT is deliberately not read: it usually points at the user's collector.
void RejectUnsupportedOtelEnv(const EnvSource &env) {
	const char *per_signal = "per-signal endpoints are not supported: one server accepts every signal on the same "
	                         "listener. Use --http/--grpc";
	const char *tls = "this server does not terminate TLS. Put it behind a TLS-terminating proxy instead";
	const std::pair<const char *, const char *> unsupported[] = {
	    {"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", per_signal},
	    {"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", per_signal},
	    {"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", per_signal},
	    {"OTEL_EXPORTER_OTLP_CERTIFICATE", tls},
	    {"OTEL_EXPORTER_OTLP_CLIENT_KEY", tls},
	    {"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", tls},
	};
	for (const auto &entry : unsupported) {
		if (env.Has(entry.first)) {
			throw InvalidInputException("%s is set but cannot be honored: %s. Unset it to continue.", entry.first,
			                            entry.second);
		}
	}
}

string DatabaseCatalogName(const string &database) {
	if (database.empty() || database == ":memory:") {
		return "";
	}
	auto name = std::filesystem::path(database).filename().string();
	for (auto suffix : {".duckdb", ".db"}) {
		if (StringUtil::EndsWith(name, suffix)) {
			return name.substr(0, name.size() - strlen(suffix));
		}
	}
	return name;
}

void ValidateCatalogDoesNotShadowDatabase(const ServerConfig &config) {
	auto db_catalog = DatabaseCatalogName(config.database);
	if (!db_catalog.empty() && config.catalog == db_catalog) {
		throw InvalidInputException(
		    "DUCKDB catalog name conflict: DUCKDB_DATABASE=%s creates catalog \"%s\", so the mode catalog cannot also "
		    "be \"%s\". Change DUCKDB_CATALOG or DUCKDB_DATABASE.",
		    config.database, db_catalog, config.catalog);
	}
}

//! Expands a mode preset. The syntax is documented at the top of local-ducklake.sql.
class PresetExpander {
public:
	PresetExpander(const EnvSource &env, ServerConfig &config) : env(env), config(config) {
		vars["DUCKDB_OTLP_DATA_DIR"] = config.data_dir;
	}

	string Expand(const string &preset) {
		string sql;
		// One entry per open @if: whether its lines are kept.
		std::vector<bool> active;
		for (const auto &line : StringUtil::Split(preset, '\n')) {
			auto trimmed = line;
			StringUtil::Trim(trimmed);
			// Each entry already folds in the blocks around it.
			bool keep = active.empty() || active.back();
			if (StringUtil::StartsWith(trimmed, "-- @")) {
				auto words = StringUtil::Split(trimmed.substr(4), ' ');
				auto directive = words.empty() ? string() : words[0];
				if (directive == "end" && !active.empty()) {
					active.pop_back();
				} else if (directive == "if") {
					active.push_back(keep && If(words));
				} else if (directive == "else" && !active.empty()) {
					bool parent = active.size() < 2 || active[active.size() - 2];
					active.back() = parent && !active.back();
				} else if (directive == "set" && words.size() >= 2) {
					if (keep) {
						auto value = trimmed.substr(trimmed.find(words[1]) + words[1].size());
						StringUtil::Trim(value);
						vars[words[1]] = Substitute(value, false);
					}
				} else {
					throw duckdb::InternalException("Malformed mode preset directive: %s", trimmed);
				}
				continue;
			}
			if (!keep || trimmed.empty() || StringUtil::StartsWith(trimmed, "--")) {
				continue;
			}
			sql += Substitute(Secrets(line), true) + "\n";
		}
		if (!active.empty()) {
			throw duckdb::InternalException("Mode preset has an @if without an @end");
		}
		return sql;
	}

	string Var(const string &name) const {
		auto entry = vars.find(name);
		return entry == vars.end() ? "" : entry->second;
	}

	//! Throw one error naming every missing setting, not one per run.
	void ThrowIfMissing() const {
		if (missing.empty()) {
			return;
		}
		std::ostringstream msg;
		msg << "Missing " << missing.size() << " required setting" << (missing.size() == 1 ? "" : "s")
		    << " for DUCKDB_MODE=" << config.mode << ":";
		for (const auto &entry : missing) {
			msg << "\n  " << entry.first << (entry.second.empty() ? "" : "  (" + entry.second + ")");
		}
		throw InvalidInputException(msg.str());
	}

private:
	string Lookup(const string &name) const {
		auto entry = vars.find(name);
		return entry != vars.end() ? entry->second : env.Get(name.c_str());
	}

	void Missing(const string &name, const string &note) {
		for (const auto &entry : missing) {
			if (entry.first == name) {
				return;
			}
		}
		missing.emplace_back(name, note);
	}

	//! `-- @if A B ...`: keep the block when any of the names is set (the getenv() lines inside
	//! then require the rest). `-- @if A^=prefix`: keep it when A starts with prefix.
	bool If(const duckdb::vector<string> &words) {
		for (idx_t i = 1; i < words.size(); i++) {
			auto prefix = words[i].find("^=");
			auto value = Lookup(words[i].substr(0, prefix));
			if (prefix == string::npos ? !value.empty() : StringUtil::StartsWith(value, words[i].substr(prefix + 2))) {
				return true;
			}
		}
		return false;
	}

	//! getenv('NAME') -> getvariable('env_NAME'). getenv() exists only in the DuckDB CLI, and the
	//! session variable keeps the secret out of the SQL that `validate` prints.
	string Secrets(string line) {
		const string open = "getenv('";
		for (auto pos = line.find(open); pos != string::npos; pos = line.find(open, pos)) {
			auto close = line.find("')", pos + open.size());
			if (close == string::npos) {
				break;
			}
			auto name = line.substr(pos + open.size(), close - pos - open.size());
			if (env.Get(name.c_str()).empty()) {
				Missing(name, "");
			}
			config.env_variables.push_back(name);
			auto replacement = "getvariable('env_" + name + "')";
			line.replace(pos, close + 2 - pos, replacement);
			pos += replacement.size();
		}
		return line;
	}

	//! Replace ${NAME} and ${NAME:-default}. In SQL text the value is escaped for a single-quoted
	//! literal, which is where every preset uses it. `outer` names the setting whose default is
	//! being expanded, so a missing setting inside it says what could replace it.
	string Substitute(const string &text, bool sql, const string &outer = "") {
		string out;
		for (size_t i = 0; i < text.size();) {
			if (text.compare(i, 2, "${") != 0) {
				out += text[i++];
				continue;
			}
			size_t depth = 1;
			size_t end = i + 2;
			for (; end < text.size() && depth > 0; end++) {
				if (text.compare(end, 2, "${") == 0) {
					depth++;
					end++;
				} else if (text[end] == '}') {
					depth--;
				}
			}
			if (depth > 0) {
				throw duckdb::InternalException("Unterminated ${ in mode preset: %s", text);
			}
			auto body = text.substr(i + 2, end - i - 3);
			i = end;
			auto sep = body.find(":-");
			auto name = body.substr(0, sep);
			auto value = Lookup(name);
			auto fallback = sep == string::npos ? string() : body.substr(sep + 2);
			if (value.empty() && fallback.size() > 3 && fallback.find("${", 1) == string::npos &&
			    StringUtil::StartsWith(fallback, "${") && fallback.back() == '}') {
				// A plain alias, ${A:-${B}}: report A, the usual spelling.
				value = Lookup(fallback.substr(2, fallback.size() - 3));
				if (value.empty()) {
					Missing(name, "or " + fallback.substr(2, fallback.size() - 3));
				}
			} else if (value.empty() && sep != string::npos) {
				value = Substitute(fallback, false, outer.empty() ? name : outer);
			} else if (value.empty()) {
				Missing(name, outer.empty() ? "" : "or set " + outer);
			}
			out += sql ? duckdb::SqlEscape(value) : value;
		}
		return out;
	}

	const EnvSource &env;
	ServerConfig &config;
	std::map<string, string> vars;
	std::vector<std::pair<string, string>> missing;
};

void ConfigureMode(const EnvSource &env, ServerConfig &config) {
	const ModePreset *preset = MODE_PRESETS;
	while (preset->name && config.mode != preset->name) {
		preset++;
	}
	if (!preset->name) {
		duckdb::vector<string> names;
		for (auto entry = MODE_PRESETS; entry->name; entry++) {
			names.emplace_back(entry->name);
		}
		throw InvalidInputException("Unsupported DUCKDB_MODE \"%s\". Supported modes: %s", config.mode,
		                            StringUtil::Join(names, ", "));
	}
	PresetExpander expander(env, config);
	config.mode_setup_sql = expander.Expand(preset->sql);
	expander.ThrowIfMissing();
	config.catalog = expander.Var("CATALOG");
	config.schema = expander.Var("SCHEMA");
	config.parquet_export_path = expander.Var("PARQUET_EXPORT_PATH");
	config.credentials_source = expander.Var("CREDENTIALS");
	config.allow_unsigned_extensions = !expander.Var("ALLOW_UNSIGNED_EXTENSIONS").empty();
	auto data_path = expander.Var("DATA_PATH");
	auto catalog_path = expander.Var("CATALOG_PATH");
	if (!catalog_path.empty()) {
		CreateParentDirectory(catalog_path);
	}
	CreateDirectory(data_path);
	CreateDirectory(config.parquet_export_path);
	config.data_location = !data_path.empty()                    ? data_path
	                       : !config.parquet_export_path.empty() ? config.parquet_export_path
	                       : !config.catalog.empty()             ? "catalog " + config.catalog
	                                                             : config.database;
}

//! Parse a port setting. 0 means "disable this listener" (--http 0, --grpc 0).
int ParsePortEnv(const EnvSource &env, const char *name) {
	auto value = env.Get(name);
	size_t pos = 0;
	int64_t parsed = -1;
	try {
		parsed = std::stoll(value, &pos);
	} catch (...) {
	}
	if (pos != value.size() || parsed < 0 || parsed > 65535) {
		throw InvalidInputException("%s must be a port between 0 and 65535 (0 disables the listener)", name);
	}
	return static_cast<int>(parsed);
}

//! Ports must differ regardless of host: distinct host strings can name the same socket.
void ValidateDistinctPorts(const std::vector<IngestListener> &listeners) {
	for (duckdb::idx_t i = 0; i < listeners.size(); i++) {
		for (duckdb::idx_t j = i + 1; j < listeners.size(); j++) {
			if (listeners[i].port == listeners[j].port) {
				throw InvalidInputException("Listeners %s and %s must use different ports", listeners[i].transport,
				                            listeners[j].transport);
			}
		}
	}
}

} // namespace

string BindAddr(const string &host, int port) {
	return duckdb::UriHost(host) + ":" + std::to_string(port);
}

duckdb::OtlpUri ListenHostFromEnv(const EnvSource &env) {
	RejectUnsupportedOtelEnv(env);
	// Validate and normalize the host once with the extension's URI parser.
	return duckdb::OtlpUri("otlp:" + BindAddr(env.Get("DUCKDB_OTLP_HOST", DEFAULT_HOST), DEFAULT_HTTP_PORT));
}

std::vector<IngestListener> ListenersFromEnv(const EnvSource &env, const string &host) {
	struct PortSpec {
		const char *port_var;
		int default_port;
		const char *transport;
	};
	const PortSpec specs[] = {
	    {"DUCKDB_OTLP_HTTP_PORT", DEFAULT_HTTP_PORT, "http"},
	    {"DUCKDB_OTLP_GRPC_PORT", DEFAULT_GRPC_PORT, "grpc"},
	};
	// A non-zero port selects its transport and narrows the set to the ones selected. A port of 0
	// disables its transport without selecting one, so `--grpc 0` keeps the HTTP default.
	bool any_selected = false;
	for (const auto &spec : specs) {
		any_selected = any_selected || (env.Has(spec.port_var) && ParsePortEnv(env, spec.port_var) != 0);
	}
	std::vector<IngestListener> listeners;
	for (const auto &spec : specs) {
		bool set = env.Has(spec.port_var);
		int port = set ? ParsePortEnv(env, spec.port_var) : spec.default_port;
		bool enabled = set ? port != 0 : !any_selected;
		if (enabled) {
			listeners.push_back({"otlp:" + BindAddr(host, port), spec.transport, port});
		}
	}
	if (listeners.empty()) {
		throw InvalidInputException("Every listener is disabled. Enable --http or --grpc.");
	}
	ValidateDistinctPorts(listeners);
	return listeners;
}

int QuackPortFromEnv(const EnvSource &env) {
	return env.Has("DUCKDB_QUACK_PORT") ? ParsePortEnv(env, "DUCKDB_QUACK_PORT") : 0;
}

ServerConfig ServerConfig::FromEnv(const EnvSource &env) {
	ServerConfig config;
	config.mode = env.Get("DUCKDB_MODE", "local-ducklake");
	config.data_dir = env.Get("DUCKDB_OTLP_DATA_DIR", DefaultDataDir(env));
	config.database = env.Get("DUCKDB_DATABASE", config.data_dir + "/duckdb-otlp-control.duckdb");
	config.secret_dir = env.Get("DUCKDB_OTLP_SECRET_DIR", "");
	auto listen_host = ListenHostFromEnv(env);
	config.host = listen_host.Host();
	config.listeners = ListenersFromEnv(env, config.host);
	// Most specific first; OTEL_EXPORTER_OTLP_HEADERS lets one variable configure exporter and receiver.
	config.token = env.Get("DUCKDB_OTLP_TOKEN");
	if (config.token.empty() && env.Has("OTEL_EXPORTER_OTLP_HEADERS")) {
		config.token = BearerTokenFromOtelHeaders(env.Get("OTEL_EXPORTER_OTLP_HEADERS"));
	}
	config.disable_auth = IsTruthy(env.Get("DUCKDB_OTLP_DISABLE_AUTH", "0"));
	config.quack_port = QuackPortFromEnv(env);
	for (const auto &[name, type] : duckdb::OtlpServeTuningOptions()) {
		auto variable = "DUCKDB_OTLP_" + StringUtil::Upper(name);
		auto value = env.Get(variable.c_str());
		if (value.empty()) {
			continue;
		}
		if (type.IsIntegral()) {
			if (value.find_first_not_of("0123456789") != string::npos) {
				throw InvalidInputException("%s must contain only decimal digits", variable);
			}
		} else {
			value = SqlQuote(value);
		}
		config.serve_options.emplace_back(name, std::move(value));
	}
	// An empty script is a no-op, so a templated file can render empty.
	config.init_sql_path = env.Get("DUCKDB_OTLP_INIT_SQL", "");
	if (!config.init_sql_path.empty()) {
		config.init_sql = ReadSqlFile(config.init_sql_path, "--init-sql script");
	}

	config.quack_token = env.Get("DUCKDB_QUACK_TOKEN");
	if (config.quack_port != 0 && config.quack_token.empty()) {
		throw InvalidInputException(
		    "Quack (--quack PORT) requires a dedicated token. Set --quack-token (DUCKDB_QUACK_TOKEN).");
	}
	config.quack_ssl_cert_file = env.Get("DUCKDB_QUACK_SSL_CERT_FILE");
	config.quack_ssl_key_file = env.Get("DUCKDB_QUACK_SSL_KEY_FILE");
	// Half a pair is an error: falling back to a self-signed certificate would hide the typo.
	if (config.quack_ssl_cert_file.empty() != config.quack_ssl_key_file.empty()) {
		throw InvalidInputException("DUCKDB_QUACK_SSL_CERT_FILE and DUCKDB_QUACK_SSL_KEY_FILE must be set together.");
	}

	// No built-in default token: without one, auth is allowed off only when the bind host is
	// loopback. A wildcard bind is not local. OtlpUri::IsLocal() is the predicate otlp_serve's
	// allow_other_hostname gate uses, so the two cannot disagree.
	if (!config.disable_auth && config.token.empty()) {
		if (!listen_host.IsLocal()) {
			throw InvalidInputException(
			    "A bearer token is required when binding a non-loopback address. Set DUCKDB_OTLP_TOKEN (at least 16 "
			    "characters), or pass --no-auth to accept unauthenticated traffic on purpose. Binding loopback "
			    "(--host 127.0.0.1) disables authentication automatically.");
		}
		config.disable_auth = true;
		config.auth_disabled_for_loopback = true;
	}
	if (!config.disable_auth && config.token.size() < otlp_limits::MIN_TOKEN_LENGTH) {
		throw InvalidInputException("The OTLP token must be at least %llu characters (got %llu)",
		                            static_cast<uint64_t>(otlp_limits::MIN_TOKEN_LENGTH),
		                            static_cast<uint64_t>(config.token.size()));
	}

	CreateDirectory(config.data_dir);
	ConfigureMode(env, config);
	ValidateCatalogDoesNotShadowDatabase(config);
	return config;
}

string ServerConfig::StartOtlpSql() const {
	string options_sql;
	if (!parquet_export_path.empty()) {
		options_sql += ",\n    parquet_export_path := " + SqlQuote(parquet_export_path);
		// The inspection view reads every archived footer, so it is worth creating only when
		// something can query it.
		if (quack_port == 0) {
			options_sql += ",\n    parquet_views := false";
		}
	}
	for (const auto &option : serve_options) {
		options_sql += ",\n    " + option.first + " := " + option.second;
	}
	auto schema_target =
	    catalog.empty() ? QuoteIdentifier(schema) : QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema);
	string uris_sql;
	string transports_sql;
	for (const auto &listener : listeners) {
		uris_sql += (uris_sql.empty() ? "" : ", ") + SqlQuote(listener.uri);
		transports_sql += (transports_sql.empty() ? "" : ", ") + SqlQuote(listener.transport);
	}
	auto auth_sql =
	    disable_auth ? string("disable_auth := true") : string("token := getvariable('duckdb_otlp_effective_token')");
	return StringUtil::Format(R"SQL(
CREATE SCHEMA IF NOT EXISTS %s;
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
    [%s],
    transport := [%s],
    catalog := %s,
    schema := %s,
    %s,
    allow_other_hostname := true%s
);
)SQL",
	                          schema_target, uris_sql, transports_sql, SqlQuote(catalog), SqlQuote(schema), auth_sql,
	                          options_sql);
}

string ServerConfig::QuackUri() const {
	return "quack:" + BindAddr(host, quack_port);
}

string ServerConfig::StartQuackSql() const {
	if (quack_port == 0) {
		return "";
	}
	// No disable_ssl: Quack serves TLS on a non-loopback bind; the banner prints the fingerprint.
	string ssl_sql;
	string home_sql;
	string reset_sql;
	if (!quack_ssl_cert_file.empty()) {
		ssl_sql = StringUtil::Format(",\n    ssl_cert_file := %s,\n    ssl_key_file := %s",
		                             SqlQuote(quack_ssl_cert_file), SqlQuote(quack_ssl_key_file));
	} else {
		// Quack keeps its self-signed pair under <home_directory>; pointing that at the data volume
		// keeps the fingerprint across redeploys. Set only around quack_serve, so the extension cache
		// and secret store still resolve against $HOME.
		home_sql = StringUtil::Format("\nSET home_directory = %s;", SqlQuote(data_dir));
		reset_sql = "RESET home_directory;\n";
	}
	// httpfs first: every Quack server draws an HMAC key (and TLS needs one) from DuckDB's crypto
	// module, which is writable only once httpfs is loaded.
	return "\nINSTALL httpfs;\nINSTALL quack;\nLOAD httpfs;\nLOAD quack;" + home_sql +
	       StringUtil::Format(R"SQL(
SELECT listen_uri
FROM quack_serve(
    %s,
    token := getvariable('duckdb_quack_effective_token'),
    allow_other_hostname := true%s
);
)SQL",
	                          SqlQuote(QuackUri()), ssl_sql) +
	       reset_sql;
}

string ServerConfig::QuackEndpointSql() const {
	return StringUtil::Format("SELECT listen_url, coalesce(info['ssl_fingerprint'], '') FROM quack_server_list() "
	                          "WHERE listen_uri = %s LIMIT 1",
	                          SqlQuote(QuackUri()));
}

string ServerConfig::StopOtlpSql() const {
	// No argument: stops EVERY server, including ones started over Quack or by --init-sql. At
	// database teardown their final seal is a silent no-op, so this is the last chance to commit.
	return "SELECT status, dropped_rows FROM otlp_stop();";
}

string ServerConfig::StopQuackSql() const {
	return StringUtil::Format("CALL quack_stop(%s);", SqlQuote(QuackUri()));
}

std::vector<BootStep> ServerConfig::SetupSteps() const {
	// Set secret_directory before mode setup can initialize the secret manager.
	return {{"settings", secret_dir.empty() ? string() : "SET secret_directory = " + SqlQuote(secret_dir) + ";\n"},
	        {"mode setup", mode_setup_sql + "\n"},
	        {"init SQL", init_sql.empty() ? string() : init_sql + "\n"}};
}

std::vector<BootStep> ServerConfig::ServeSteps() const {
	return {{"otlp startup", StartOtlpSql() + "\n"}, {"quack startup", StartQuackSql()}};
}

} // namespace duckdb_otlp_server
