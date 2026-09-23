#pragma once

#include "duckdb/common/string.hpp"
#include "env_source.hpp"
#include "otlp_ingest_limits.hpp"

#include <cstdint>
#include <vector>

namespace duckdb_otlp_server {

//! How a DuckDB extension becomes available to the daemon's session.
//!
//! This is the only thing that varies between extensions, and it decides everything derived
//! from the list: what setup SQL to emit (if any) and how the startup banner reports it.
//! Before this, each mode declared its extensions twice -- once as a list for the banner and
//! once as hand-written INSTALL/LOAD text -- so the two could disagree with no symptom until
//! someone read the banner.
enum class ExtensionSource {
	//! Linked into the binary; there is nothing to install or load. The otlp extension is
	//! statically embedded, which is why it must never appear in generated INSTALL SQL.
	BUILT_IN,
	//! DuckDB's core extension repository.
	CORE,
	//! The community repository, which `INSTALL` needs told about explicitly.
	COMMUNITY,
};

struct ModeExtension {
	duckdb::string name;
	ExtensionSource source = ExtensionSource::CORE;
};

struct IngestListener {
	duckdb::string uri;
	duckdb::string transport;
	bool otap = false;
};

//! Shared by startup and `doctor`; resolving listeners needs no storage credentials or I/O.
//! When `selection_reason` is non-null it receives a short human-readable note about WHICH
//! setting selected the transports, so the startup banner can say why a listener is (not) on.
std::vector<IngestListener> ListenersFromEnv(const EnvSource &env, duckdb::string *selection_reason = nullptr);

//! Whether Quack is enabled, and the address it binds. Exported for the same reason as
//! ListenersFromEnv: the `doctor` subcommand has to probe exactly what startup bound, and
//! resolving it needs no storage credentials or I/O. A second copy in the probe is how
//! DUCKDB_QUACK_PORT (what --quack sets) came to be honored by one and not the other.
bool QuackEnabledFromEnv(const EnvSource &env);
duckdb::string QuackAddrFromEnv(const EnvSource &env);

struct ServerConfig {
	duckdb::string mode;
	duckdb::string database;
	duckdb::string data_dir;
	std::vector<IngestListener> listeners;
	duckdb::string token;
	duckdb::string catalog;
	duckdb::string schema;
	duckdb::string quack_listen_uri;
	duckdb::string quack_http_addr;
	duckdb::string quack_token;
	duckdb::string parquet_export_path;
	//! Human-readable location the mode actually writes telemetry to, for the startup banner.
	//! Distinct from `database`, which is the small control DB: in the default local-ducklake
	//! mode the banner used to print only the control DB, so anyone who opened the one path it
	//! named found no telemetry in it.
	duckdb::string data_location;
	//! Where object-storage credentials came from, for the startup banner: an environment key
	//! pair, or DuckDB's own secret store when none was set. Empty for modes that need no
	//! storage credentials.
	duckdb::string credentials_source;
	//! Directory DuckDB loads persistent secrets from (DUCKDB_OTLP_SECRET_DIR / --secret-dir).
	//! Empty leaves DuckDB's default, $HOME/.duckdb/stored_secrets.
	duckdb::string secret_dir;
	bool quack_enabled = false;
	bool dry_run = false;
	//! True when the server accepts unauthenticated requests. Set explicitly by
	//! --no-auth / DUCKDB_OTLP_DISABLE_AUTH, or implicitly when no token is configured and
	//! every listener binds loopback. A non-loopback bind with no token is a hard error
	//! instead: there is no built-in default token.
	bool disable_auth = false;
	//! True when `disable_auth` was inferred from a loopback bind rather than requested.
	//! The daemon prints a one-line notice for this case (see main.cpp banner).
	bool auth_disabled_for_loopback = false;
	//! Short note naming which setting selected the transports, printed in the startup banner
	//! so a narrowed or unexpected listener set is never silent.
	duckdb::string transport_selection;
	int startup_timeout_secs = 60;
	uint64_t http_threads = 0;
	uint64_t max_body_bytes = 16ULL * 1024ULL * 1024ULL;
	uint64_t max_buffered_bytes = 512ULL * 1024ULL * 1024ULL;
	uint64_t seal_target_bytes = 128ULL * 1024ULL * 1024ULL;
	int64_t seal_max_age_ms = 5000;
	uint64_t target_file_size = 256ULL * 1024ULL * 1024ULL;
	int64_t maintenance_retention_ms = 15LL * 60LL * 1000LL;
	//! Attribute promotion (opt-in): comma-separated resource / scope attribute keys to promote into
	//! first-class columns at ingest. Emitted into the otlp_serve() call when non-empty.
	duckdb::string promote_resource_attributes;
	duckdb::string promote_scope_attributes;
	//! Whether either promotion list names a key. The daemon's half of OtlpPromoteConfig::Enabled(),
	//! which it cannot use directly: these arrive as unparsed comma-separated strings.
	bool PromotionRequested() const {
		return !promote_resource_attributes.empty() || !promote_scope_attributes.empty();
	}
	//! Opt-in: create the attribute-bag columns as VARIANT instead of VARCHAR holding JSON text
	//! (DUCKDB_OTLP_ATTRIBUTES_AS_VARIANT). Part of the destination table's shape: a catalog whose
	//! signal tables were created the other way is rejected at startup, not rewritten.
	bool attributes_as_variant = false;

	duckdb::string mode_setup_sql;
	//! Operator-supplied SQL run after mode setup and before the ingest server starts
	//! (`--init-sql` / DUCKDB_OTLP_INIT_SQL). The escape hatch for DuckDB configuration the
	//! modes do not model: extra ATTACHes, SET statements, secrets, views over the ingest
	//! tables. Empty when unset.
	//!
	//! Resolved to CONTENTS here rather than kept as a path so `validate` reports an
	//! unreadable script exactly as `serve` would and prints the SQL it would run. `doctor`
	//! never calls FromEnv, so a container HEALTHCHECK does not read (or execute) this.
	duckdb::string init_sql;
	//! Where init_sql came from, for error messages and the startup banner. Empty when unset.
	duckdb::string init_sql_path;
	//! Every extension this mode needs, declared once. `mode_setup_sql` derives its INSTALL/LOAD
	//! prelude from this list and the startup banner prints it, so neither can drift from it.
	std::vector<ModeExtension> mode_extensions;
	//! Environment-variable names that the generated secret SQL reads via getvariable().
	//! The daemon binds each ("env_<NAME>" -> the env value) as a session variable before
	//! running mode setup, so secret values never appear in the generated SQL text.
	//! (getenv() is a CLI-only DuckDB function, absent in the embedded library.)
	std::vector<duckdb::string> env_variables;

	//! Resolve the whole configuration from `env` — the process environment plus whatever the
	//! command line layered on top. Taking the source as a parameter is what lets every
	//! subcommand share one resolution path without any of them mutating the process
	//! environment first.
	static ServerConfig FromEnv(const EnvSource &env);

	//! One otlp_serve/otap_serve call starting every listener against one shared server.
	duckdb::string StartOtlpSql() const;
	duckdb::string StartQuackSql() const;
	//! Stops the whole server (every listener) through its first listener URI.
	duckdb::string StopOtlpSql() const;
	duckdb::string StopQuackSql() const;
	duckdb::string BootSql() const;

	//! Every extension this configuration will make available, for the startup banner: the
	//! mode's own, plus Quack when it is enabled. Quack is not a mode extension -- it is loaded
	//! by the serve path only, so `query`/`export` must not pull it in -- but it is still an
	//! extension the process ends up running, and the banner omitted it entirely.
	std::vector<ModeExtension> AllExtensions() const;
};

} // namespace duckdb_otlp_server
