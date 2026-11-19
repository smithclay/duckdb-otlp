#include "server_config.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "env_source.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_uri.hpp"
#include "server_util.hpp"

#include <cstring>
#include <filesystem>
#include <limits>
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

//! Reject the standard OTLP exporter variables whose meaning this server cannot honor, rather
//! than let someone believe their listener is encrypted or split per signal.
//! OTEL_EXPORTER_OTLP_ENDPOINT is deliberately not read at all: it usually points at the
//! user's real collector.
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

//! `name` as a positive integer no larger than `max`, or `fallback` when it is unset.
uint64_t ParsePositiveEnv(const EnvSource &env, const char *name, uint64_t fallback,
                          uint64_t max = std::numeric_limits<int64_t>::max()) {
	auto value = env.Get(name);
	if (value.empty()) {
		return fallback;
	}
	size_t pos = 0;
	uint64_t parsed = 0;
	try {
		parsed = std::stoull(value, &pos);
	} catch (...) {
		pos = 0;
	}
	if (pos != value.size() || value[0] == '-' || parsed == 0 || parsed > max) {
		throw InvalidInputException("%s must be a positive integer", name);
	}
	return parsed;
}

//! Settings passed straight through to the otlp_serve option of the same name: DUCKDB_OTLP_<X>
//! sets option <x>. The extension validates and defaults them, so the daemon only checks that a
//! numeric one is a positive integer before rendering it into the generated SQL.
struct ServeOption {
	const char *variable;
	bool numeric;
};
const ServeOption SERVE_OPTIONS[] = {
    {"HTTP_THREADS", true},
    {"MAX_BODY_BYTES", true},
    {"MAX_BUFFERED_BYTES", true},
    {"SEAL_TARGET_BYTES", true},
    {"SEAL_MAX_AGE_MS", true},
    {"TARGET_FILE_SIZE", true},
    {"MAINTENANCE_RETENTION_MS", true},
    {"MAINTENANCE_INTERVAL_MS", true},
    {"PROMOTE_RESOURCE_ATTRIBUTES", false},
    {"PROMOTE_SCOPE_ATTRIBUTES", false},
};

//! Expands a mode preset (src/server/modes/<mode>.sql) against the configuration. The syntax
//! is documented at the top of local-ducklake.sql.
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

	//! A variable the preset assigned with @set, or "" when it did not.
	string Var(const string &name) const {
		auto entry = vars.find(name);
		return entry == vars.end() ? "" : entry->second;
	}

	//! Throw one error naming every missing setting, so configuring a remote lakehouse does not
	//! take one run per variable.
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

	//! getenv('NAME') -> getvariable('env_NAME'). getenv() exists only in the DuckDB CLI, and
	//! the session variable keeps the secret out of the SQL that `validate` prints.
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

//! One transport's resolved bind address. Whether that transport is switched ON is decided
//! by the caller (ListenersFromEnv), not here, so this struct holds no enablement flag.
struct ResolvedListener {
	string host;
	int port = 0;
};

//! Parse a port env var. Unlike the other numeric parsers, 0 is legal and means
//! "disable this listener", which is how --http 0 / --grpc 0 switch a transport off.
int ParsePortEnv(const EnvSource &env, const char *name) {
	auto value = env.Get(name);
	try {
		size_t pos = 0;
		auto parsed = std::stoll(value, &pos);
		if (pos != value.size() || parsed < 0 || parsed > 65535) {
			throw InvalidInputException("%s must be a port between 0 and 65535 (0 disables the listener)", name);
		}
		return static_cast<int>(parsed);
	} catch (InvalidInputException &) {
		throw;
	} catch (...) {
		throw InvalidInputException("%s must be a port between 0 and 65535 (0 disables the listener)", name);
	}
}

//! Resolve one transport's bind address from, in order: its DUCKDB_OTLP_*_PORT variable, its
//! legacy OTEL_*_ADDR variable, then the built-in default.
//!
//! Host precedence needs care because two settings name it. `--host` moves every listener at
//! once, so a flag beats everything. But DUCKDB_OTLP_HOST is also how the container image
//! sets its 0.0.0.0 default, and a host spelled out in OTEL_HTTP_ADDR is the more specific of
//! the two environment variables: an operator narrowing the container's bind with
//! OTEL_HTTP_ADDR=127.0.0.1:4318 must get loopback, not be silently widened back to the
//! wildcard the image asked for.
//!
//! The legacy "host:port" form is parsed by OtlpUri rather than by hand: it already validates
//! the host and the port range and understands bracketed IPv6, and the result is handed back
//! to OtlpUri to build the listener URI anyway. Splitting it here as well meant two parsers
//! disagreeing about the same string, with only one of them validating.
ResolvedListener ResolveListener(const EnvSource &env, const char *port_var, const char *addr_var, int default_port) {
	ResolvedListener resolved;
	auto explicit_host = env.Get("DUCKDB_OTLP_HOST");
	auto host_from_flag = env.IsOverride("DUCKDB_OTLP_HOST");
	auto addr = addr_var ? env.Get(addr_var) : string();

	if (env.Has(port_var)) {
		resolved.port = ParsePortEnv(env, port_var);
		resolved.host = explicit_host.empty() ? DEFAULT_HOST : explicit_host;
		return resolved;
	}
	if (addr.empty()) {
		resolved.port = default_port;
		resolved.host = explicit_host.empty() ? DEFAULT_HOST : explicit_host;
		return resolved;
	}
	// A bare host with no port keeps the transport's default port.
	auto has_port = addr.find(':') != string::npos && addr[addr.size() - 1] != ']';
	try {
		duckdb::OtlpUri parsed("otlp:" + addr + (has_port ? "" : ":" + std::to_string(default_port)));
		resolved.port = parsed.Port();
		resolved.host = host_from_flag ? explicit_host : parsed.Host();
	} catch (const std::exception &ex) {
		throw InvalidInputException("%s is not a valid bind address (\"%s\"): %s", addr_var, addr,
		                            duckdb::ErrorData(ex).RawMessage());
	}
	return resolved;
}

//! Build a listener URI from a resolved host/port. One spelling, so adding a transport or
//! changing the URI form is a single-site edit.
IngestListener MakeListener(const char *scheme, const ResolvedListener &resolved, const char *transport, bool otap) {
	// An IPv6 literal has to be bracketed before the port is appended, or "::1" + ":4318"
	// reads as one colon-separated string and the port parse fails. Both sources of a host
	// hand one over unbracketed — OtlpUri::Host() strips the brackets it parsed, and
	// DUCKDB_OTLP_HOST/--host take a bare literal.
	auto uri = string(scheme) + ":" + duckdb::UriHost(resolved.host) + ":" + std::to_string(resolved.port);
	return {duckdb::OtlpUri(uri).Uri(), transport, otap};
}

//! Transport selection from OTEL_EXPORTER_OTLP_PROTOCOL, consulted ONLY when nothing more
//! specific selected the transports. The variable is exporter-side in the OTel spec, and a
//! developer's shell often already carries it for their application's exporter, so letting
//! it silently narrow an explicitly-configured server would be surprising.
void ApplyOtelProtocol(const EnvSource &env, bool &http_enabled, bool &grpc_enabled, string &reason) {
	auto protocol = env.Get("OTEL_EXPORTER_OTLP_PROTOCOL");
	if (protocol.empty()) {
		return;
	}
	if (protocol == "grpc") {
		http_enabled = false;
		grpc_enabled = true;
	} else if (protocol == "http/protobuf" || protocol == "http/json") {
		http_enabled = true;
		grpc_enabled = false;
	} else {
		throw InvalidInputException(
		    "OTEL_EXPORTER_OTLP_PROTOCOL must be grpc, http/protobuf, or http/json (got \"%s\")", protocol);
	}
	reason = "OTEL_EXPORTER_OTLP_PROTOCOL=" + protocol;
}

//! The legacy DUCKDB_OTLP_TRANSPORTS list form, kept for the container image and for
//! existing deployments. Ports come from OTEL_HTTP_ADDR / OTEL_GRPC_ADDR as before.
std::vector<IngestListener> ListenersFromTransportList(const EnvSource &env, const string &override_uri) {
	auto transports = env.Get("DUCKDB_OTLP_TRANSPORTS", "http");
	std::vector<IngestListener> listeners;
	// Split explicitly so empty entries (including a trailing comma) are rejected.
	duckdb::idx_t offset = 0;
	while (true) {
		auto comma = transports.find(',', offset);
		auto transport = transports.substr(offset, comma == string::npos ? comma : comma - offset);
		StringUtil::Trim(transport);
		if (transport != "http" && transport != "grpc") {
			throw InvalidInputException("DUCKDB_OTLP_TRANSPORTS must be http, grpc, or http,grpc");
		}
		for (const auto &listener : listeners) {
			if (listener.transport == transport) {
				throw InvalidInputException("DUCKDB_OTLP_TRANSPORTS must not repeat a transport");
			}
		}
		auto resolved = transport == "http"
		                    ? ResolveListener(env, "DUCKDB_OTLP_HTTP_PORT", "OTEL_HTTP_ADDR", DEFAULT_HTTP_PORT)
		                    : ResolveListener(env, "DUCKDB_OTLP_GRPC_PORT", "OTEL_GRPC_ADDR", DEFAULT_GRPC_PORT);
		listeners.push_back(override_uri.empty()
		                        ? MakeListener("otlp", resolved, transport.c_str(), false)
		                        : IngestListener {duckdb::OtlpUri(override_uri).Uri(), transport, false});
		if (comma == string::npos) {
			break;
		}
		offset = comma + 1;
	}
	if (listeners.size() > 1 && !override_uri.empty()) {
		throw InvalidInputException("DUCKDB_OTLP_LISTEN_URI requires a single transport; use --http and --grpc "
		                            "(or OTEL_HTTP_ADDR and OTEL_GRPC_ADDR) for multiple listeners");
	}
	return listeners;
}

//! Reject two listeners sharing a port. Conservative: distinct host strings can still name
//! the same socket (a wildcard bind, an alias), so ports must differ regardless of host.
void ValidateDistinctPorts(const std::vector<IngestListener> &listeners) {
	for (duckdb::idx_t i = 0; i < listeners.size(); i++) {
		for (duckdb::idx_t j = i + 1; j < listeners.size(); j++) {
			if (duckdb::OtlpUri(listeners[i].uri).Port() == duckdb::OtlpUri(listeners[j].uri).Port()) {
				throw InvalidInputException("Listeners %s and %s must use different ports", listeners[i].transport,
				                            listeners[j].transport);
			}
		}
	}
}

} // namespace

std::vector<IngestListener> ListenersFromEnv(const EnvSource &env, string *selection_reason) {
	RejectUnsupportedOtelEnv(env);
	string reason;
	auto override_uri = env.Get("DUCKDB_OTLP_LISTEN_URI");
	bool ports_set =
	    env.Has("DUCKDB_OTLP_HTTP_PORT") || env.Has("DUCKDB_OTLP_GRPC_PORT") || env.Has("DUCKDB_OTLP_OTAP_PORT");

	auto finish = [&](std::vector<IngestListener> listeners) {
		ValidateDistinctPorts(listeners);
		if (selection_reason) {
			*selection_reason = reason;
		}
		return listeners;
	};

	// (1) Single-listener URI override. Most specific, so it wins over everything.
	if (!override_uri.empty()) {
		if (ports_set) {
			throw InvalidInputException("DUCKDB_OTLP_LISTEN_URI cannot be combined with --http/--grpc/--otap "
			                            "(DUCKDB_OTLP_*_PORT); use one or the other");
		}
		reason = "DUCKDB_OTLP_LISTEN_URI";
		// OTAP is a separate protocol, not another spelling for standard OTLP/gRPC.
		if (duckdb::OtlpUri(override_uri).Scheme() == "otap") {
			if (env.Has("DUCKDB_OTLP_TRANSPORTS")) {
				throw InvalidInputException("DUCKDB_OTLP_TRANSPORTS cannot be combined with an otap: listen URI");
			}
			return finish({{duckdb::OtlpUri(override_uri).Uri(), "grpc", true}});
		}
		return finish(ListenersFromTransportList(env, override_uri));
	}

	// (2) Legacy transport list, unless explicit ports supersede it. The container image sets
	// DUCKDB_OTLP_TRANSPORTS in its Dockerfile, so a container user passing --http/--grpc
	// still lands on the port path below.
	if (env.Has("DUCKDB_OTLP_TRANSPORTS") && !ports_set) {
		reason = "DUCKDB_OTLP_TRANSPORTS=" + env.Get("DUCKDB_OTLP_TRANSPORTS");
		return finish(ListenersFromTransportList(env, ""));
	}

	// (3) Port-based selection: the CLI path. Each port variable is parsed exactly once here,
	// and enablement is derived from the parsed value.
	//
	// A port of 0 DISABLES its transport but does not count as "selecting" one. That
	// distinction is what makes `--grpc 0` mean "turn gRPC off, keep the rest" rather than
	// "turn everything off": only a non-zero port narrows the set to what was named.
	struct PortSpec {
		const char *port_var;
		const char *addr_var;
		int default_port;
		const char *scheme;
		const char *transport;
		bool otap;
		bool enabled;
		ResolvedListener resolved;
	};
	PortSpec specs[] = {
	    {"DUCKDB_OTLP_HTTP_PORT", "OTEL_HTTP_ADDR", DEFAULT_HTTP_PORT, "otlp", "http", false, false, {}},
	    {"DUCKDB_OTLP_GRPC_PORT", "OTEL_GRPC_ADDR", DEFAULT_GRPC_PORT, "otlp", "grpc", false, false, {}},
	    {"DUCKDB_OTLP_OTAP_PORT", nullptr, DEFAULT_GRPC_PORT, "otap", "grpc", true, false, {}},
	};
	bool any_selected = false;
	for (auto &spec : specs) {
		spec.resolved = ResolveListener(env, spec.port_var, spec.addr_var, spec.default_port);
		any_selected = any_selected || (env.Has(spec.port_var) && spec.resolved.port != 0);
	}
	for (auto &spec : specs) {
		bool explicitly_off = env.Has(spec.port_var) && spec.resolved.port == 0;
		// OTAP is never on by default; it has to be asked for.
		bool on_by_default = !spec.otap && !any_selected;
		spec.enabled = !explicitly_off && (on_by_default || (env.Has(spec.port_var) && spec.resolved.port != 0));
	}
	if (any_selected) {
		reason = "explicit ports";
	} else {
		reason = "default (OTLP/HTTP and OTLP/gRPC)";
		// (4) Nothing selected a transport: let the standard exporter protocol variable narrow
		// the default pair. Transports switched off with an explicit 0 stay off.
		bool http_enabled = specs[0].enabled;
		bool grpc_enabled = specs[1].enabled;
		ApplyOtelProtocol(env, http_enabled, grpc_enabled, reason);
		specs[0].enabled = specs[0].enabled && http_enabled;
		specs[1].enabled = specs[1].enabled && grpc_enabled;
	}

	if (specs[2].enabled && (specs[0].enabled || specs[1].enabled)) {
		// OTAP/Arrow is served by otap_serve and standard OTLP by otlp_serve; a single
		// process starts one or the other, never both, so catch it here with a clear message
		// rather than at the SQL layer.
		throw InvalidInputException("OTAP/Arrow cannot be combined with standard OTLP listeners. Start OTAP alone "
		                            "(--otap PORT --http 0 --grpc 0), or run a second process for it.");
	}

	std::vector<IngestListener> listeners;
	for (const auto &spec : specs) {
		if (spec.enabled) {
			listeners.push_back(MakeListener(spec.scheme, spec.resolved, spec.transport, spec.otap));
		}
	}
	if (listeners.empty()) {
		throw InvalidInputException("Every listener is disabled. Enable at least one of --http, --grpc, or --otap.");
	}
	return finish(std::move(listeners));
}

bool QuackEnabledFromEnv(const EnvSource &env) {
	if (IsTruthy(env.Get("DUCKDB_QUACK_ENABLED", env.Get("QUACK_ENABLED", "0")))) {
		return true;
	}
	// A non-zero `--quack PORT` also enables it: the flag is the more specific signal, so it
	// wins over DUCKDB_QUACK_ENABLED=0 in the environment. Stated here, beside the setting it
	// is about, rather than as a name comparison inside the otherwise setting-agnostic flag
	// loop — and IsOverride is exactly the flag-versus-variable distinction it appealed to.
	// A bare DUCKDB_QUACK_PORT in the environment still does not enable Quack.
	return env.IsOverride("DUCKDB_QUACK_PORT") && ParsePortEnv(env, "DUCKDB_QUACK_PORT") != 0;
}

string QuackAddrFromEnv(const EnvSource &env) {
	// DUCKDB_QUACK_LISTEN_URI is what quack_serve binds whenever it is set (FromEnv), so it
	// wins here too: the banner's ATTACH line and the doctor probe derive from this address,
	// and they pointed at the default port while the server listened elsewhere.
	if (env.Has("DUCKDB_QUACK_LISTEN_URI")) {
		auto uri = env.Get("DUCKDB_QUACK_LISTEN_URI");
		for (auto prefix : {"quack://", "quack:"}) {
			if (StringUtil::StartsWith(uri, prefix)) {
				return uri.substr(strlen(prefix));
			}
		}
		return uri;
	}
	// --quack PORT sets DUCKDB_QUACK_PORT; the legacy DUCKDB_QUACK_ADDR / QUACK_HTTP_ADDR
	// host:port form still wins when no port was given explicitly. Port 0 means "off", as it
	// does for --http/--grpc, so it does not select a (zero, i.e. ephemeral) bind port.
	auto port = env.Has("DUCKDB_QUACK_PORT") ? ParsePortEnv(env, "DUCKDB_QUACK_PORT") : 0;
	if (port != 0) {
		return duckdb::UriHost(env.Get("DUCKDB_OTLP_HOST", DEFAULT_HOST)) + ":" + std::to_string(port);
	}
	return env.Get("DUCKDB_QUACK_ADDR", env.Get("QUACK_HTTP_ADDR", string(DEFAULT_HOST) + ":9494"));
}

ServerConfig ServerConfig::FromEnv(const EnvSource &env) {
	ServerConfig config;
	// DUCKDB_MODE is no longer required: a bare `duckdb-otlp` on a laptop should start a
	// working local lakehouse with no configuration at all. Every other mode still has to be
	// named explicitly, so this default cannot silently redirect an intended remote target.
	config.mode = env.Get("DUCKDB_MODE", "local-ducklake");
	config.data_dir = env.Get("DUCKDB_OTLP_DATA_DIR", DefaultDataDir(env));
	// Derived from data_dir rather than hard-coded to /data, so overriding the data directory
	// moves the control database with it. The container sets DUCKDB_OTLP_DATA_DIR=/data, which
	// reproduces the previous default path exactly.
	config.database = env.Get("DUCKDB_DATABASE", config.data_dir + "/duckdb-otlp-control.duckdb");
	// Where DuckDB loads persistent secrets from. Empty keeps DuckDB's default
	// ($HOME/.duckdb/stored_secrets), which already works unattended: a secret created once
	// with CREATE PERSISTENT SECRET is picked up by every later run, so credentials need not be
	// passed as environment variables at all. The knob exists for deployments that mount them
	// read-only somewhere else.
	config.secret_dir = env.Get("DUCKDB_OTLP_SECRET_DIR", "");
	config.listeners = ListenersFromEnv(env, &config.transport_selection);
	// Token resolution, most specific first. OTEL_EXPORTER_OTLP_HEADERS is the standard
	// exporter-side spelling ("Authorization=Bearer <token>"); accepting it lets one variable
	// configure both an exporter and this receiver.
	config.token = env.Get("OTEL_AUTH_TOKEN", env.Get("DUCKDB_OTLP_TOKEN"));
	if (config.token.empty() && env.Has("OTEL_EXPORTER_OTLP_HEADERS")) {
		config.token = BearerTokenFromOtelHeaders(env.Get("OTEL_EXPORTER_OTLP_HEADERS"));
	}
	config.disable_auth = IsTruthy(env.Get("DUCKDB_OTLP_DISABLE_AUTH", "0"));
	config.quack_enabled = QuackEnabledFromEnv(env);
	config.quack_http_addr = QuackAddrFromEnv(env);
	config.quack_listen_uri = env.Get("DUCKDB_QUACK_LISTEN_URI", "quack:" + config.quack_http_addr);
	config.dry_run = IsTruthy(env.Get("DRY_RUN", "0"));
	config.startup_timeout_secs = static_cast<int>(ParsePositiveEnv(env, "DUCKDB_OTLP_STARTUP_TIMEOUT",
	                                                                static_cast<uint64_t>(config.startup_timeout_secs),
	                                                                std::numeric_limits<int>::max()));
	for (const auto &option : SERVE_OPTIONS) {
		auto variable = string("DUCKDB_OTLP_") + option.variable;
		if (!env.Has(variable.c_str())) {
			continue;
		}
		auto name = StringUtil::Lower(option.variable);
		auto value = option.numeric ? std::to_string(ParsePositiveEnv(env, variable.c_str(), 0))
		                            : SqlQuote(env.Get(variable.c_str()));
		config.serve_options.emplace_back(name, value);
	}
	// An empty script is a no-op, so a templated file can render empty.
	config.init_sql_path = env.Get("DUCKDB_OTLP_INIT_SQL", "");
	if (!config.init_sql_path.empty()) {
		config.init_sql = ReadSqlFile(config.init_sql_path, "--init-sql script");
	}

	config.quack_token = env.Get("DUCKDB_QUACK_TOKEN");
	if (config.quack_enabled && config.quack_token.empty()) {
		throw InvalidInputException("DUCKDB_QUACK_ENABLED=1 requires a dedicated Quack token. Set DUCKDB_QUACK_TOKEN.");
	}
	config.quack_ssl_cert_file = env.Get("DUCKDB_QUACK_SSL_CERT_FILE");
	config.quack_ssl_key_file = env.Get("DUCKDB_QUACK_SSL_KEY_FILE");
	// Half a pair is a hard error rather than a fall-through to the self-signed certificate,
	// which would hide the typo until a client's fingerprint pin stopped matching.
	if (config.quack_ssl_cert_file.empty() != config.quack_ssl_key_file.empty()) {
		throw InvalidInputException("DUCKDB_QUACK_SSL_CERT_FILE and DUCKDB_QUACK_SSL_KEY_FILE must be set together.");
	}

	// Authentication. There is deliberately no built-in default token: a token published in
	// this repository authenticates nothing, and silently falling back to one gave servers the
	// appearance of being protected. Instead, an unauthenticated server is allowed only where
	// it cannot be reached from off the machine.
	// A wildcard bind (0.0.0.0 / ::) is NOT local: it accepts traffic from the whole network
	// and therefore always needs a token. OtlpUri::IsLocal() is the same predicate the
	// allow_other_hostname gate in otlp_serve uses, so the two cannot disagree about a URI.
	bool all_loopback = true;
	for (const auto &listener : config.listeners) {
		all_loopback = all_loopback && duckdb::OtlpUri(listener.uri).IsLocal();
	}
	if (!config.disable_auth && config.token.empty()) {
		if (!all_loopback) {
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
		if (!quack_enabled) {
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
	auto auth_sql = disable_auth ? string("disable_auth := true")
	                             : string("token := getvariable('duckdb_otlp_effective_token')");
	return StringUtil::Format(R"SQL(
CREATE SCHEMA IF NOT EXISTS %s;
SELECT listen_url, catalog_name, schema_name
FROM %s(
    [%s],
    transport := [%s],
    catalog := %s,
    schema := %s,
    %s,
    allow_other_hostname := true%s
);
)SQL",
	                          schema_target, listeners.front().otap ? "otap_serve" : "otlp_serve", uris_sql,
	                          transports_sql, SqlQuote(catalog), SqlQuote(schema), auth_sql, options_sql);
}

string ServerConfig::StartQuackSql() const {
	if (!quack_enabled) {
		return "";
	}
	// No disable_ssl: as of DuckDB 2.0 Quack serves TLS on every non-loopback bind, with a
	// self-signed certificate unless one is supplied. Clients pin its fingerprint, which the
	// startup banner prints (QuackEndpointSql).
	string ssl_sql;
	string home_sql;
	string reset_sql;
	if (!quack_ssl_cert_file.empty()) {
		ssl_sql = StringUtil::Format(",\n    ssl_cert_file := %s,\n    ssl_key_file := %s",
		                             SqlQuote(quack_ssl_cert_file), SqlQuote(quack_ssl_key_file));
	} else {
		// Quack keeps its self-signed pair under <home_directory>/.duckdb/extension_data/quack.
		// In the container $HOME is part of the image, so every new container generated a new
		// certificate and broke every client's fingerprint pin. The data directory (the /data
		// volume) outlives a redeploy, so the fingerprint now lasts exactly as long as the data
		// it guards. home_directory is connection-local by default, and it is set only after
		// INSTALL/LOAD quack and reset right after, so the extension cache and the secret store
		// still resolve against $HOME.
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
	                          SqlQuote(quack_listen_uri), ssl_sql) +
	       reset_sql;
}

string ServerConfig::QuackEndpointSql() const {
	return StringUtil::Format("SELECT listen_url, coalesce(info['ssl_fingerprint'], '') FROM quack_server_list() "
	                          "WHERE listen_uri = %s LIMIT 1",
	                          SqlQuote(quack_listen_uri));
}

string ServerConfig::StopOtlpSql() const {
	// dropped_rows is non-zero only when the final shutdown drain failed and rows were dropped;
	// main.cpp reads it to exit non-zero on a data-dropping shutdown (review finding M4).
	//
	// No argument, so this stops EVERY server on the instance, not just the one the daemon
	// started. A server started out-of-band -- over Quack, or by the operator's --init-sql --
	// was previously left to database teardown, where OtlpServer::db_ptr has already expired
	// and the final seal is a silent no-op, so its buffered rows were dropped. This is the
	// only shutdown point at which those rows can still be committed.
	return "SELECT status, dropped_rows FROM otlp_stop();";
}

string ServerConfig::StopQuackSql() const {
	return StringUtil::Format("CALL quack_stop(%s);", SqlQuote(quack_listen_uri));
}

string ServerConfig::BootSql() const {
	// Same order the serve path executes in, so `validate` shows exactly what would run.
	// secret_directory goes first: it has to be set before anything initializes the secret
	// manager, which the mode's own CREATE SECRET statements would do.
	auto prelude = secret_dir.empty() ? string() : "SET secret_directory = " + SqlQuote(secret_dir) + ";\n";
	auto init = init_sql.empty() ? string() : init_sql + "\n";
	return prelude + mode_setup_sql + "\n" + init + StartOtlpSql() + "\n" + StartQuackSql();
}

} // namespace duckdb_otlp_server
