#include "server_config.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "env_source.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_uri.hpp"
#include "server_util.hpp"

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <sstream>
#include <system_error>
#include <utility>

namespace duckdb_otlp_server {
namespace {

using duckdb::InvalidInputException;
using duckdb::QuoteIdentifier;
using duckdb::SqlQuote;
using duckdb::string;
using duckdb::StringUtil;

//! Default bind host. Loopback, not a wildcard: the CLI is used on laptops, where binding
//! every interface by default would expose an unauthenticated ingest port to the local
//! network. The container image opts back into 0.0.0.0 via DUCKDB_OTLP_HOST in its Dockerfile.
constexpr const char *DEFAULT_HOST = "127.0.0.1";
constexpr int DEFAULT_HTTP_PORT = 4318;
constexpr int DEFAULT_GRPC_PORT = 4317;

//! Default data directory. The container pins this to /data via its Dockerfile; everywhere
//! else it follows the XDG base-directory spec, which macOS tolerates and Linux expects.
string DefaultDataDir(const EnvSource &env) {
	auto xdg = env.Get("XDG_DATA_HOME");
	if (!xdg.empty()) {
		return xdg + "/duckdb-otlp";
	}
	auto home = env.Get("HOME");
	if (!home.empty()) {
		return home + "/.local/share/duckdb-otlp";
	}
	// No HOME (some init/container contexts): fall back to the working directory rather than
	// writing to an unpredictable absolute path.
	return "./duckdb-otlp-data";
}

//! Parse one `key=value` pair out of an OTEL_EXPORTER_OTLP_HEADERS value and return the
//! bearer token when the key is `authorization` and the value is a Bearer credential.
//! The W3C Baggage encoding used by the OTel spec is comma-separated `key=value`.
string BearerTokenFromOtelHeaders(const string &headers) {
	duckdb::idx_t offset = 0;
	while (offset <= headers.size()) {
		auto comma = headers.find(',', offset);
		auto entry = headers.substr(offset, comma == string::npos ? string::npos : comma - offset);
		StringUtil::Trim(entry);
		auto eq = entry.find('=');
		if (eq != string::npos) {
			auto key = entry.substr(0, eq);
			auto value = entry.substr(eq + 1);
			StringUtil::Trim(key);
			StringUtil::Trim(value);
			if (StringUtil::Lower(key) == "authorization") {
				// Accept "Bearer <token>" (the spec's form) and a bare token.
				auto space = value.find(' ');
				if (space != string::npos && StringUtil::Lower(value.substr(0, space)) == "bearer") {
					value = value.substr(space + 1);
					StringUtil::Trim(value);
				}
				return value;
			}
		}
		if (comma == string::npos) {
			break;
		}
		offset = comma + 1;
	}
	return "";
}

//! Reject the standard OTLP exporter variables whose semantics we cannot honor.
//!
//! Silently ignoring these is the dangerous option: a user who sets a TLS certificate
//! reasonably believes the listener is encrypted, and one who sets a per-signal endpoint
//! believes traces and logs land on different ports. We serve one process with one set of
//! listeners and no TLS, so both beliefs would be wrong. Fail loudly instead.
//!
//! OTEL_EXPORTER_OTLP_ENDPOINT is deliberately NOT read at all (neither honored nor
//! rejected): it is routinely set in a developer's shell to point at their real collector,
//! so treating it as a bind address would either fail confusingly or silently move the
//! listener. Use DUCKDB_OTLP_HOST / --http / --grpc instead.
void RejectUnsupportedOtelEnv(const EnvSource &env) {
	struct UnsupportedVar {
		const char *name;
		const char *reason;
	};
	const UnsupportedVar unsupported[] = {
	    {"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "per-signal endpoints are not supported: one server accepts every "
	                                           "signal on the same listener. Use --http/--grpc"},
	    {"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "per-signal endpoints are not supported: one server accepts every "
	                                         "signal on the same listener. Use --http/--grpc"},
	    {"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "per-signal endpoints are not supported: one server accepts every "
	                                            "signal on the same listener. Use --http/--grpc"},
	    {"OTEL_EXPORTER_OTLP_CERTIFICATE", "this server does not terminate TLS. Put it behind a TLS-terminating "
	                                       "proxy instead"},
	    {"OTEL_EXPORTER_OTLP_CLIENT_KEY", "this server does not terminate TLS. Put it behind a TLS-terminating "
	                                      "proxy instead"},
	    {"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "this server does not terminate TLS. Put it behind a "
	                                              "TLS-terminating proxy instead"},
	};
	for (const auto &entry : unsupported) {
		if (env.Has(entry.name)) {
			throw InvalidInputException("%s is set but cannot be honored: %s. Unset it to continue.", entry.name,
			                            entry.reason);
		}
	}
}

string NormalizeMode(const string &mode) {
	if (mode == "ducklake-local") {
		return "local-ducklake";
	}
	if (mode == "cloudflare") {
		return "r2-data-catalog";
	}
	if (mode == "s3tables") {
		return "s3-tables";
	}
	if (mode == "plain-s3" || mode == "s3-parquet" || mode == "s3") {
		return "parquet";
	}
	if (mode == "ducklake-s3" || mode == "s3-ducklake") {
		return "aws-ducklake";
	}
	return mode;
}

string FirstEnv(const EnvSource &env, std::initializer_list<const char *> names) {
	for (auto name : names) {
		if (env.Has(name)) {
			return name;
		}
	}
	return "";
}

string RequireEnv(const EnvSource &env, const char *name, const string &mode) {
	auto value = env.Get(name);
	if (value.empty()) {
		throw InvalidInputException("Missing required environment variable %s for DUCKDB_MODE=%s", name, mode);
	}
	return value;
}

string RequireAnyEnv(const EnvSource &env, const string &label, std::initializer_list<const char *> names) {
	auto name = FirstEnv(env, names);
	if (!name.empty()) {
		return name;
	}
	std::ostringstream msg;
	msg << "Missing required environment variable for " << label << ". Set one of:";
	for (auto candidate : names) {
		msg << " " << candidate;
	}
	throw InvalidInputException(msg.str());
}

string EndpointHost(string value) {
	if (StringUtil::StartsWith(value, "http://")) {
		value = value.substr(7);
	} else if (StringUtil::StartsWith(value, "https://")) {
		value = value.substr(8);
	}
	auto slash = value.find('/');
	if (slash != string::npos) {
		value = value.substr(0, slash);
	}
	return value;
}

string R2EndpointDefault(const EnvSource &env, const string &mode) {
	if (env.Has("CLOUDFLARE_R2_ENDPOINT")) {
		return EndpointHost(env.Get("CLOUDFLARE_R2_ENDPOINT"));
	}
	if (env.Has("CLOUDFLARE_S3_API_HOST")) {
		return EndpointHost(env.Get("CLOUDFLARE_S3_API_HOST"));
	}
	if (env.Has("R2_ENDPOINT")) {
		return EndpointHost(env.Get("R2_ENDPOINT"));
	}
	return RequireEnv(env, "CLOUDFLARE_ACCOUNT_ID", mode) + ".r2.cloudflarestorage.com";
}

string R2BucketValue(const EnvSource &env) {
	auto var = RequireAnyEnv(env, "Cloudflare R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	return env.Get(var.c_str());
}

string R2PrefixValue(const EnvSource &env) {
	auto prefix = env.Get("CLOUDFLARE_R2_PREFIX", env.Get("R2_PREFIX", "duckdb-otlp/"));
	while (!prefix.empty() && prefix[0] == '/') {
		prefix = prefix.substr(1);
	}
	return prefix;
}

string R2DataPath(const EnvSource &env) {
	auto bucket = R2BucketValue(env);
	auto prefix = R2PrefixValue(env);
	return prefix.empty() ? "s3://" + bucket + "/" : "s3://" + bucket + "/" + prefix;
}

string S3BucketValue(const EnvSource &env) {
	auto var = RequireAnyEnv(env, "S3 bucket", {"S3_BUCKET", "AWS_S3_BUCKET", "DUCKDB_OTLP_S3_BUCKET"});
	return env.Get(var.c_str());
}

string S3PrefixValue(const EnvSource &env) {
	auto prefix = env.Get("S3_PREFIX", env.Get("AWS_S3_PREFIX", env.Get("DUCKDB_OTLP_S3_PREFIX", "duckdb-otlp/")));
	while (!prefix.empty() && prefix[0] == '/') {
		prefix = prefix.substr(1);
	}
	while (!prefix.empty() && prefix[prefix.size() - 1] == '/') {
		prefix = prefix.substr(0, prefix.size() - 1);
	}
	return prefix;
}

string S3DataPath(const EnvSource &env) {
	auto bucket = S3BucketValue(env);
	auto prefix = S3PrefixValue(env);
	return prefix.empty() ? "s3://" + bucket : "s3://" + bucket + "/" + prefix;
}

bool IsS3Path(const string &path) {
	return StringUtil::StartsWith(StringUtil::Lower(path), "s3://");
}

string CatalogDefault(const EnvSource &env, const string &fallback) {
	return env.Get("DUCKDB_CATALOG", fallback);
}

string SchemaDefault(const EnvSource &env, const string &fallback) {
	return env.Get("DUCKDB_SCHEMA", fallback);
}

string DatabaseCatalogName(const string &database) {
	if (database.empty() || database == ":memory:") {
		return "";
	}
	auto name = std::filesystem::path(database).filename().string();
	if (StringUtil::EndsWith(name, ".duckdb")) {
		name = name.substr(0, name.size() - 7);
	} else if (StringUtil::EndsWith(name, ".db")) {
		name = name.substr(0, name.size() - 3);
	}
	return name;
}

void ValidateCatalogDoesNotShadowDatabase(const ServerConfig &config) {
	auto db_catalog = DatabaseCatalogName(config.database);
	if (!db_catalog.empty() && config.catalog == db_catalog) {
		throw InvalidInputException(
		    "DUCKDB catalog name conflict: DUCKDB_DATABASE=%s creates catalog \"%s\", so the mode catalog cannot also "
		    "be \"%s\". Change DUCKLAKE_NAME, DUCKDB_CATALOG, or DUCKDB_DATABASE.",
		    config.database, db_catalog, config.catalog);
	}
}

string EnvSql(const EnvSource &env, ServerConfig &config, const string &name, const string &fallback = "") {
	if (!env.Has(name.c_str())) {
		return SqlQuote(fallback);
	}
	// Read the value through a session variable the daemon binds from the environment, not
	// getenv(): getenv() is a CLI-only DuckDB function and is NOT registered in the embedded
	// library the daemon links, so it errors at mode setup. Recording only the NAME keeps the
	// secret value out of the generated SQL text (which DRY_RUN prints).
	config.env_variables.push_back(name);
	return "getvariable(" + SqlQuote("env_" + name) + ")";
}

int ParsePositiveIntEnv(const EnvSource &env, const char *name, int fallback) {
	auto value = env.Get(name);
	if (value.empty()) {
		return fallback;
	}
	try {
		size_t pos = 0;
		auto parsed = std::stoll(value, &pos);
		if (pos != value.size() || parsed <= 0 || parsed > std::numeric_limits<int>::max()) {
			throw InvalidInputException("%s must be a positive integer", name);
		}
		return static_cast<int>(parsed);
	} catch (InvalidInputException &) {
		throw;
	} catch (...) {
		throw InvalidInputException("%s must be a positive integer", name);
	}
}

uint64_t ParsePositiveUInt64Env(const EnvSource &env, const char *name, uint64_t fallback) {
	auto value = env.Get(name);
	if (value.empty()) {
		return fallback;
	}
	try {
		size_t pos = 0;
		auto parsed = std::stoull(value, &pos);
		if (pos != value.size() || parsed == 0) {
			throw InvalidInputException("%s must be a positive integer", name);
		}
		return parsed;
	} catch (InvalidInputException &) {
		throw;
	} catch (...) {
		throw InvalidInputException("%s must be a positive integer", name);
	}
}

int64_t ParsePositiveInt64Env(const EnvSource &env, const char *name, int64_t fallback) {
	auto value = env.Get(name);
	if (value.empty()) {
		return fallback;
	}
	try {
		size_t pos = 0;
		auto parsed = std::stoll(value, &pos);
		if (pos != value.size() || parsed <= 0) {
			throw InvalidInputException("%s must be a positive integer", name);
		}
		return parsed;
	} catch (InvalidInputException &) {
		throw;
	} catch (...) {
		throw InvalidInputException("%s must be a positive integer", name);
	}
}

// DuckLake data inlining: DuckLake writes small inserts into the metadata catalog instead of a
// Parquet file, and only CHECKPOINT flushes them out. DUCKLAKE_DATA_INLINING_ROW_LIMIT sets the
// per-insert row limit on ATTACH; 0 disables inlining so every seal writes Parquet directly.
// Unset keeps DuckLake's own default.
string DuckLakeInliningOption(const EnvSource &env) {
	auto name = "DUCKLAKE_DATA_INLINING_ROW_LIMIT";
	auto value = env.Get(name);
	if (value.empty()) {
		return "";
	}
	try {
		size_t pos = 0;
		auto parsed = std::stoull(value, &pos);
		if (pos != value.size() || value[0] == '-') {
			throw InvalidInputException("%s must be a non-negative integer", name);
		}
		return StringUtil::Format("DATA_INLINING_ROW_LIMIT %llu", static_cast<uint64_t>(parsed));
	} catch (InvalidInputException &) {
		throw;
	} catch (...) {
		throw InvalidInputException("%s must be a non-negative integer", name);
	}
}

// Extra option lines for `ATTACH ... (DATA_PATH ...)`.
string DuckLakePathAttachOptions(const EnvSource &env) {
	auto option = DuckLakeInliningOption(env);
	return option.empty() ? string() : ",\n  " + option;
}

// Option clause for `ATTACH 'ducklake:<secret>' AS <name>`.
string DuckLakeSecretAttachOptions(const EnvSource &env) {
	auto option = DuckLakeInliningOption(env);
	return option.empty() ? string() : " (" + option + ")";
}

// Cloudflare R2 access/secret-key env-var resolution. The candidate lists are identical across
// every R2 mode (r2-data-catalog, r2-local-ducklake, r2-neon-ducklake), so they live here once.
struct R2Credentials {
	string access_key_var;
	string secret_key_var;
};

R2Credentials ResolveR2Credentials(const EnvSource &env, const string &label) {
	R2Credentials creds;
	creds.access_key_var = RequireAnyEnv(env, label + " access key",
	                                     {"CLOUDFLARE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_ACCESS_KEY_ID",
	                                      "CLOUDFLARE_R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_KEY_ID"});
	creds.secret_key_var =
	    RequireAnyEnv(env, label + " secret key",
	                  {"CLOUDFLARE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_ACCESS_KEY",
	                   "CLOUDFLARE_R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_KEY"});
	return creds;
}

// The KEY_ID/SECRET R2 storage secret block, byte-identical across the three R2 modes. The key/secret
// are referenced through getvariable() (via EnvSql) so the values never appear in the generated SQL.
// Returns the secret statement with a leading newline and a trailing ");\n" so it can be injected via
// %s exactly where the modes previously inlined it.
string BuildR2StorageSecret(const EnvSource &env, ServerConfig &config, const string &secret_name,
                            const R2Credentials &creds, const string &endpoint) {
	return StringUtil::Format(R"SQL(
CREATE OR REPLACE SECRET %s (
  TYPE s3,
  KEY_ID %s,
  SECRET %s,
  REGION 'auto',
  ENDPOINT %s,
  URL_STYLE 'path'
);
)SQL",
	                          secret_name, EnvSql(env, config, creds.access_key_var),
	                          EnvSql(env, config, creds.secret_key_var), SqlQuote(endpoint));
}

// The PROVIDER credential_chain S3 secret block shared by the parquet and s3-tables modes. Both pick
// CHAIN config + PROFILE when an AWS profile is set, else CHAIN env, then emit the region; the parquet
// mode additionally appends an optional endpoint and url_style (s3-tables passes both empty). The block
// has a leading newline and a trailing ");\n" so it injects via %s exactly where it was inlined before.
string BuildCredentialChainSecret(const string &secret_name, const string &region, const string &profile,
                                  const string &endpoint, const string &url_style) {
	auto secret_sql = StringUtil::Format(R"SQL(
CREATE OR REPLACE SECRET %s (
  TYPE s3,
  PROVIDER credential_chain,
)SQL",
	                                     secret_name);
	if (!profile.empty()) {
		secret_sql += StringUtil::Format("  CHAIN config,\n  PROFILE %s,\n", SqlQuote(profile));
	} else {
		secret_sql += "  CHAIN env,\n";
	}
	secret_sql += StringUtil::Format("  REGION %s", SqlQuote(region));
	if (!endpoint.empty()) {
		secret_sql += StringUtil::Format(",\n  ENDPOINT %s", SqlQuote(EndpointHost(endpoint)));
	}
	if (!url_style.empty()) {
		secret_sql += StringUtil::Format(",\n  URL_STYLE %s", SqlQuote(url_style));
	}
	secret_sql += "\n);\n";
	return secret_sql;
}

void ConfigureLocalDuckLake(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"ducklake", "otlp"};
	config.catalog = CatalogDefault(env, env.Get("DUCKLAKE_NAME", "otel"));
	config.schema = SchemaDefault(env, "main");
	auto catalog_path = env.Get("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = env.Get("DUCKLAKE_DATA_PATH", config.data_dir + "/ducklake/storage");
	config.data_location = data_path;
	CreateParentDirectory(catalog_path);
	CreateDirectory(data_path);

	config.mode_setup_sql = StringUtil::Format(R"SQL(
INSTALL ducklake;
LOAD ducklake;
ATTACH %s AS %s (
  DATA_PATH %s%s
);
)SQL",
	                                           SqlQuote("ducklake:" + catalog_path), QuoteIdentifier(config.catalog),
	                                           SqlQuote(data_path), DuckLakePathAttachOptions(env));
}

void ConfigureAwsDuckLake(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"ducklake", "aws", "httpfs", "otlp"};
	config.catalog = CatalogDefault(env, env.Get("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault(env, "otlp");
	auto catalog_path = env.Get("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = env.Get("DUCKLAKE_DATA_PATH");
	config.data_location = data_path;
	if (!IsS3Path(data_path)) {
		throw InvalidInputException("DUCKDB_MODE=%s requires DUCKLAKE_DATA_PATH=s3://bucket/prefix", config.mode);
	}
	auto region = env.Get("AWS_REGION", env.Get("AWS_DEFAULT_REGION"));
	if (region.empty()) {
		throw InvalidInputException("Missing AWS region for DUCKDB_MODE=%s. Set AWS_REGION or AWS_DEFAULT_REGION",
		                            config.mode);
	}
	CreateParentDirectory(catalog_path);
	config.mode_setup_sql =
	    StringUtil::Format(R"SQL(
INSTALL ducklake;
INSTALL aws;
INSTALL httpfs;
LOAD ducklake;
LOAD aws;
LOAD httpfs;
CREATE OR REPLACE SECRET aws_ducklake_storage (
  TYPE s3,
  PROVIDER credential_chain,
  CHAIN instance,
  REGION %s
);
ATTACH %s AS %s (
  DATA_PATH %s%s
);
)SQL",
	                       SqlQuote(region), SqlQuote("ducklake:" + catalog_path), QuoteIdentifier(config.catalog),
	                       SqlQuote(data_path), DuckLakePathAttachOptions(env));
}

void ConfigureR2DataCatalog(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"iceberg", "httpfs", "otlp"};
	config.catalog = CatalogDefault(env, env.Get("CLOUDFLARE_CATALOG_NAME", "r2catalog"));
	config.schema = SchemaDefault(env, "otlp");
	auto catalog_token_var =
	    RequireAnyEnv(env, "Cloudflare catalog token", {"CLOUDFLARE_CATALOG_TOKEN", "CLOUDFLARE_API_TOKEN"});
	auto creds = ResolveR2Credentials(env, "Cloudflare R2");
	RequireAnyEnv(env, "Cloudflare R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	RequireEnv(env, "CLOUDFLARE_ACCOUNT_ID", config.mode);
	auto catalog_uri = RequireEnv(env, "CLOUDFLARE_CATALOG_URI", config.mode);
	auto warehouse = env.Get("CLOUDFLARE_WAREHOUSE", env.Get("R2_WAREHOUSE"));
	if (warehouse.empty() && env.Has("CLOUDFLARE_ACCOUNT_ID")) {
		warehouse = env.Get("CLOUDFLARE_ACCOUNT_ID") + "_" + R2BucketValue(env);
	}
	if (warehouse.empty()) {
		throw InvalidInputException("Missing Cloudflare warehouse. Set CLOUDFLARE_WAREHOUSE or provide "
		                            "CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_R2_BUCKET");
	}
	auto endpoint = R2EndpointDefault(env, config.mode);
	auto storage_secret = BuildR2StorageSecret(env, config, "cloudflare_r2_secret", creds, endpoint);

	config.mode_setup_sql = StringUtil::Format(
	    R"SQL(
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;%sCREATE OR REPLACE SECRET cloudflare_catalog_secret (
  TYPE ICEBERG,
  TOKEN %s
);
ATTACH %s AS %s (
  TYPE ICEBERG,
  ENDPOINT %s,
  SECRET cloudflare_catalog_secret
);
)SQL",
	    storage_secret, EnvSql(env, config, catalog_token_var), SqlQuote(warehouse), QuoteIdentifier(config.catalog),
	    SqlQuote(catalog_uri));
}

void ConfigureParquet(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"otlp"};
	config.catalog = "";
	config.schema = SchemaDefault(env, "otlp");
	config.parquet_export_path =
	    env.Get("PARQUET_EXPORT_PATH", env.Get("DUCKDB_OTLP_PARQUET_EXPORT_PATH",
	                                           env.Get("S3_EXPORT_PATH", env.Get("DUCKDB_OTLP_S3_EXPORT_PATH"))));
	if (config.parquet_export_path.empty()) {
		if (env.Has("S3_BUCKET") || env.Has("AWS_S3_BUCKET") || env.Has("DUCKDB_OTLP_S3_BUCKET")) {
			config.parquet_export_path = S3DataPath(env);
		} else {
			config.parquet_export_path = config.data_dir + "/parquet";
		}
	}

	config.data_location = config.parquet_export_path;
	if (!IsS3Path(config.parquet_export_path)) {
		CreateDirectory(config.parquet_export_path);
		config.mode_setup_sql = "";
		return;
	}

	config.mode_extensions = {"aws", "httpfs", "otlp"};
	auto region = env.Get("AWS_REGION", env.Get("AWS_DEFAULT_REGION"));
	if (region.empty()) {
		throw InvalidInputException("Missing AWS region for DUCKDB_MODE=%s with an s3:// export path. Set AWS_REGION "
		                            "or AWS_DEFAULT_REGION",
		                            config.mode);
	}
	auto profile = env.Get("AWS_PROFILE", env.Get("AWS_DEFAULT_PROFILE"));
	auto endpoint = env.Get("S3_ENDPOINT", env.Get("AWS_S3_ENDPOINT"));
	auto url_style = env.Get("S3_URL_STYLE", env.Get("AWS_S3_URL_STYLE"));

	auto secret_sql = BuildCredentialChainSecret("plain_s3_secret", region, profile, endpoint, url_style);

	config.mode_setup_sql = StringUtil::Format(R"SQL(
INSTALL aws;
INSTALL httpfs;
LOAD aws;
LOAD httpfs;
%s)SQL",
	                                           secret_sql);
}

void ConfigureR2LocalDuckLake(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"ducklake", "httpfs", "otlp"};
	config.catalog = CatalogDefault(env, env.Get("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault(env, "otlp");
	auto creds = ResolveR2Credentials(env, "R2");
	RequireAnyEnv(env, "R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	auto catalog_path = env.Get("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = env.Get("DUCKLAKE_DATA_PATH", R2DataPath(env));
	config.data_location = data_path;
	CreateParentDirectory(catalog_path);
	auto endpoint = R2EndpointDefault(env, config.mode);
	auto storage_secret = BuildR2StorageSecret(env, config, "r2_storage", creds, endpoint);

	config.mode_setup_sql =
	    StringUtil::Format(R"SQL(
INSTALL ducklake;
INSTALL httpfs;
LOAD ducklake;
LOAD httpfs;%sATTACH %s AS %s (
  DATA_PATH %s%s
);
)SQL",
	                       storage_secret, SqlQuote("ducklake:" + catalog_path), QuoteIdentifier(config.catalog),
	                       SqlQuote(data_path), DuckLakePathAttachOptions(env));
}

void ConfigureR2NeonDuckLake(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"ducklake", "postgres", "httpfs", "otlp"};
	config.catalog = CatalogDefault(env, env.Get("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault(env, "otlp");
	auto creds = ResolveR2Credentials(env, "R2");
	RequireAnyEnv(env, "R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	RequireEnv(env, "NEON_PGHOST", config.mode);
	RequireEnv(env, "NEON_PGDATABASE", config.mode);
	RequireEnv(env, "NEON_PGUSER", config.mode);
	RequireEnv(env, "NEON_PGPASSWORD", config.mode);
	auto data_path = env.Get("DUCKLAKE_DATA_PATH", R2DataPath(env));
	config.data_location = data_path;
	auto endpoint = R2EndpointDefault(env, config.mode);
	auto storage_secret = BuildR2StorageSecret(env, config, "r2_storage", creds, endpoint);

	config.mode_setup_sql = StringUtil::Format(
	    R"SQL(
INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
LOAD ducklake;
LOAD postgres;
LOAD httpfs;%sCREATE OR REPLACE SECRET postgres_secret (
  TYPE postgres,
  HOST %s,
  PORT %s,
  DATABASE %s,
  USER %s,
  PASSWORD %s,
  SSLMODE %s
);
CREATE OR REPLACE SECRET ducklake_secret (
  TYPE ducklake,
  METADATA_PATH '',
  DATA_PATH %s,
  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
);
ATTACH 'ducklake:ducklake_secret' AS %s%s;
)SQL",
	    storage_secret, EnvSql(env, config, "NEON_PGHOST"), EnvSql(env, config, "NEON_PGPORT", "5432"),
	    EnvSql(env, config, "NEON_PGDATABASE"), EnvSql(env, config, "NEON_PGUSER"),
	    EnvSql(env, config, "NEON_PGPASSWORD"), EnvSql(env, config, "NEON_PGSSLMODE", "require"), SqlQuote(data_path),
	    QuoteIdentifier(config.catalog), DuckLakeSecretAttachOptions(env));
}

void ConfigureGcpDuckLake(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"ducklake", "postgres", "gcs", "otlp"};
	config.catalog = CatalogDefault(env, env.Get("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault(env, "otlp");
	auto data_path = RequireEnv(env, "DUCKLAKE_DATA_PATH", config.mode);
	config.data_location = data_path;
	// Force the native GCS filesystem even if httpfs is loaded by another extension.
	if (!StringUtil::StartsWith(data_path, "gcss://") || data_path.size() <= 7 || data_path[7] == '/') {
		throw InvalidInputException("DUCKDB_MODE=gcp-ducklake requires DUCKLAKE_DATA_PATH=gcss://bucket/prefix");
	}
	for (auto name : {"PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"}) {
		RequireEnv(env, name, config.mode);
	}

	config.mode_setup_sql = StringUtil::Format(
	    R"SQL(
INSTALL ducklake;
INSTALL postgres;
INSTALL gcs FROM community;
LOAD ducklake;
LOAD postgres;
LOAD gcs;
CREATE OR REPLACE SECRET gcp_storage (
  TYPE gcp,
  PROVIDER credential_chain
);
CREATE OR REPLACE SECRET postgres_secret (
  TYPE postgres,
  HOST %s,
  PORT %s,
  DATABASE %s,
  USER %s,
  PASSWORD %s,
  SSLMODE %s
);
CREATE OR REPLACE SECRET ducklake_secret (
  TYPE ducklake,
  METADATA_PATH '',
  DATA_PATH %s,
  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
);
ATTACH 'ducklake:ducklake_secret' AS %s%s;
)SQL",
	    EnvSql(env, config, "PGHOST"), EnvSql(env, config, "PGPORT", "5432"), EnvSql(env, config, "PGDATABASE"),
	    EnvSql(env, config, "PGUSER"), EnvSql(env, config, "PGPASSWORD"), EnvSql(env, config, "PGSSLMODE", "require"),
	    SqlQuote(data_path), QuoteIdentifier(config.catalog), DuckLakeSecretAttachOptions(env));
}

void ConfigureS3Tables(const EnvSource &env, ServerConfig &config) {
	config.mode_extensions = {"iceberg", "aws", "httpfs", "otlp"};
	config.catalog = CatalogDefault(env, env.Get("S3_TABLES_CATALOG_NAME", "s3tables"));
	config.schema = SchemaDefault(env, "otlp");
	auto bucket_arn =
	    env.Get("S3_TABLES_BUCKET_ARN", env.Get("S3_TABLES_TABLE_BUCKET_ARN", env.Get("TABLE_BUCKET_ARN")));
	if (bucket_arn.empty()) {
		throw InvalidInputException(
		    "Missing S3 Tables bucket ARN. Set S3_TABLES_BUCKET_ARN, S3_TABLES_TABLE_BUCKET_ARN, or TABLE_BUCKET_ARN");
	}
	auto region = env.Get("AWS_REGION", env.Get("AWS_DEFAULT_REGION"));
	if (region.empty()) {
		auto marker = string(":s3tables:");
		auto start = bucket_arn.find(marker);
		if (start != string::npos) {
			start += marker.size();
			auto end = bucket_arn.find(':', start);
			if (end != string::npos) {
				region = bucket_arn.substr(start, end - start);
			}
		}
	}
	if (region.empty()) {
		throw InvalidInputException("Missing AWS region for DUCKDB_MODE=%s. Set AWS_REGION/AWS_DEFAULT_REGION or use "
		                            "an S3 Tables ARN that includes a region",
		                            config.mode);
	}
	auto profile = env.Get("AWS_PROFILE", env.Get("AWS_DEFAULT_PROFILE"));
	// s3-tables uses the same credential_chain secret as the parquet mode, minus the optional
	// endpoint/url_style (both empty here).
	auto secret_sql = BuildCredentialChainSecret("s3_tables_secret", region, profile, /*endpoint=*/"",
	                                             /*url_style=*/"");

	config.mode_setup_sql = StringUtil::Format(R"SQL(
INSTALL iceberg;
INSTALL aws;
INSTALL httpfs;
LOAD iceberg;
LOAD aws;
LOAD httpfs;
%sATTACH %s AS %s (
  TYPE iceberg,
  ENDPOINT_TYPE s3_tables
);
)SQL",
	                                           secret_sql, SqlQuote(bucket_arn), QuoteIdentifier(config.catalog));
}

void ConfigureMode(const EnvSource &env, ServerConfig &config) {
	if (config.mode == "local-ducklake") {
		ConfigureLocalDuckLake(env, config);
	} else if (config.mode == "gcp-ducklake") {
		ConfigureGcpDuckLake(env, config);
	} else if (config.mode == "aws-ducklake") {
		ConfigureAwsDuckLake(env, config);
	} else if (config.mode == "parquet") {
		ConfigureParquet(env, config);
	} else if (config.mode == "r2-data-catalog") {
		ConfigureR2DataCatalog(env, config);
	} else if (config.mode == "s3-tables") {
		ConfigureS3Tables(env, config);
	} else if (config.mode == "r2-neon-ducklake") {
		ConfigureR2NeonDuckLake(env, config);
	} else if (config.mode == "r2-local-ducklake") {
		ConfigureR2LocalDuckLake(env, config);
	} else {
		throw InvalidInputException(
		    "Unsupported DUCKDB_MODE \"%s\". Supported modes: local-ducklake, aws-ducklake, parquet, "
		    "r2-data-catalog, s3-tables, r2-neon-ducklake, r2-local-ducklake, gcp-ducklake",
		    config.mode);
	}
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
	config.mode = NormalizeMode(env.Get("DUCKDB_MODE", "local-ducklake"));
	config.data_dir = env.Get("DUCKDB_OTLP_DATA_DIR", DefaultDataDir(env));
	// Derived from data_dir rather than hard-coded to /data, so overriding the data directory
	// moves the control database with it. The container sets DUCKDB_OTLP_DATA_DIR=/data, which
	// reproduces the previous default path exactly.
	config.database = env.Get("DUCKDB_DATABASE", config.data_dir + "/duckdb-otlp-control.duckdb");
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
	config.startup_timeout_secs = ParsePositiveIntEnv(env, "DUCKDB_OTLP_STARTUP_TIMEOUT", 60);
	config.http_threads = ParsePositiveUInt64Env(env, "DUCKDB_OTLP_HTTP_THREADS", 0);
	config.max_body_bytes =
	    ParsePositiveUInt64Env(env, "DUCKDB_OTLP_MAX_BODY_BYTES", otlp_limits::DEFAULT_MAX_BODY_BYTES);
	config.max_buffered_bytes =
	    ParsePositiveUInt64Env(env, "DUCKDB_OTLP_MAX_BUFFERED_BYTES", otlp_limits::DEFAULT_MAX_BUFFERED_BYTES);
	config.seal_target_bytes =
	    ParsePositiveUInt64Env(env, "DUCKDB_OTLP_SEAL_TARGET_BYTES", otlp_limits::DEFAULT_SEAL_TARGET_BYTES);
	config.seal_max_age_ms =
	    ParsePositiveInt64Env(env, "DUCKDB_OTLP_SEAL_MAX_AGE_MS", otlp_limits::DEFAULT_SEAL_MAX_AGE_MS);
	config.target_file_size =
	    ParsePositiveUInt64Env(env, "DUCKDB_OTLP_TARGET_FILE_SIZE", otlp_limits::DEFAULT_TARGET_FILE_SIZE);
	config.maintenance_retention_ms = ParsePositiveInt64Env(env, "DUCKDB_OTLP_MAINTENANCE_RETENTION_MS",
	                                                        otlp_limits::DEFAULT_MAINTENANCE_RETENTION_MS);
	// Read to contents here (not at execution time) so `validate` fails on an unreadable
	// script and prints the SQL it would run. Empty/whitespace-only is a no-op, not an error:
	// an operator templating this file into a container should be able to render it empty.
	config.init_sql_path = env.Get("DUCKDB_OTLP_INIT_SQL", "");
	if (!config.init_sql_path.empty()) {
		config.init_sql = ReadSqlFile(config.init_sql_path, "--init-sql script");
	}
	config.promote_resource_attributes = env.Get("DUCKDB_OTLP_PROMOTE_RESOURCE_ATTRIBUTES", "");
	config.promote_scope_attributes = env.Get("DUCKDB_OTLP_PROMOTE_SCOPE_ATTRIBUTES", "");

	auto quack_token_var = FirstEnv(env, {"DUCKDB_QUACK_TOKEN", "QUACK_AUTH_TOKEN"});
	if (config.quack_enabled && quack_token_var.empty()) {
		throw InvalidInputException(
		    "DUCKDB_QUACK_ENABLED=1 requires a dedicated Quack token. Set DUCKDB_QUACK_TOKEN or QUACK_AUTH_TOKEN.");
	}
	if (!quack_token_var.empty()) {
		config.quack_token = env.Get(quack_token_var.c_str());
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
	if (config.data_location.empty() && !config.catalog.empty()) {
		// The catalog-managed modes (r2-data-catalog, s3-tables) have no local data path of
		// their own; the catalog decides where files go, so name the catalog instead.
		config.data_location = "catalog " + config.catalog;
	}
	ValidateCatalogDoesNotShadowDatabase(config);
	return config;
}

string ServerConfig::StartOtlpSql() const {
	bool any_http = false;
	for (const auto &listener : listeners) {
		any_http = any_http || listener.transport == "http";
	}
	auto thread_sql = http_threads == 0 || !any_http
	                      ? string("")
	                      : StringUtil::Format(",\n    http_threads := %llu", static_cast<uint64_t>(http_threads));
	// These ingest limits are declared in three places that must stay in lockstep: the ServerConfig
	// fields (server_config.hpp), the env parsing in FromEnv(), and this otlp_serve() param emission.
	// Emit them from a single {name, value} table so a new limit is added in one place and the
	// hand-aligned %llu/%lld format strings (whose mismatch against a field's type was a silent
	// corruption surface) are gone. Every value is a non-negative integer rendered as plain decimal,
	// so signedness does not affect the emitted text. Each entry contributes ",\n    <name> := <value>".
	const std::pair<const char *, uint64_t> limits[] = {
	    {"max_body_bytes", static_cast<uint64_t>(max_body_bytes)},
	    {"max_buffered_bytes", static_cast<uint64_t>(max_buffered_bytes)},
	    {"seal_target_bytes", static_cast<uint64_t>(seal_target_bytes)},
	    {"seal_max_age_ms", static_cast<uint64_t>(seal_max_age_ms)},
	    {"target_file_size", static_cast<uint64_t>(target_file_size)},
	    {"maintenance_retention_ms", static_cast<uint64_t>(maintenance_retention_ms)},
	};
	string limits_sql;
	for (const auto &limit : limits) {
		limits_sql += StringUtil::Format(",\n    %s := %llu", limit.first, limit.second);
	}
	auto export_sql = parquet_export_path.empty()
	                      ? string("")
	                      : StringUtil::Format(",\n    parquet_export_path := %s", SqlQuote(parquet_export_path));
	// Attribute promotion params, emitted only when set so the common path is unchanged.
	string promote_sql;
	if (!promote_resource_attributes.empty()) {
		promote_sql +=
		    StringUtil::Format(",\n    promote_resource_attributes := %s", SqlQuote(promote_resource_attributes));
	}
	if (!promote_scope_attributes.empty()) {
		promote_sql += StringUtil::Format(",\n    promote_scope_attributes := %s", SqlQuote(promote_scope_attributes));
	}
	auto schema_target =
	    catalog.empty() ? QuoteIdentifier(schema) : QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema);
	// Every listener feeds one server (one buffer set, one sealer, one maintenance schedule),
	// so the URIs and their transports are passed as parallel lists to a single call.
	const char *serve_fn = listeners.front().otap ? "otap_serve" : "otlp_serve";
	string uris_sql;
	string transports_sql;
	for (const auto &listener : listeners) {
		uris_sql += (uris_sql.empty() ? "" : ", ") + SqlQuote(listener.uri);
		transports_sql += (transports_sql.empty() ? "" : ", ") + SqlQuote(listener.transport);
	}
	// The token is read at execution time from a session variable (set via the C++ API in
	// main.cpp) rather than interpolated as a literal, so it never appears in the generated
	// SQL string (which DRY_RUN=1 prints to stdout and the engine can echo in error
	// messages). With authentication disabled there is no token at all, and passing one
	// alongside disable_auth would be misleading, so the parameters are mutually exclusive.
	auto auth_sql = disable_auth ? string("    disable_auth := true")
	                             : string("    token := getvariable('duckdb_otlp_effective_token')");
	return StringUtil::Format(R"SQL(
CREATE SCHEMA IF NOT EXISTS %s;
SELECT listen_url, catalog_name, schema_name
FROM %s(
    [%s],
    transport := [%s],
    catalog := %s,
    schema := %s,
%s,
    allow_other_hostname := true%s%s%s%s
);
)SQL",
	                          schema_target, serve_fn, uris_sql, transports_sql, SqlQuote(catalog), SqlQuote(schema),
	                          auth_sql, thread_sql, limits_sql, export_sql, promote_sql);
}

string ServerConfig::StartQuackSql() const {
	if (!quack_enabled) {
		return "";
	}
	return StringUtil::Format(R"SQL(
LOAD quack;
SELECT listen_uri
FROM quack_serve(
    %s,
    token := getvariable('duckdb_quack_effective_token'),
    allow_other_hostname := true
);
)SQL",
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
	auto init = init_sql.empty() ? string() : init_sql + "\n";
	return mode_setup_sql + "\n" + init + StartOtlpSql() + "\n" + StartQuackSql();
}

} // namespace duckdb_otlp_server
