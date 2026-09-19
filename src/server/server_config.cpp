#include "server_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_uri.hpp"

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
using duckdb::SqlEscape;
using duckdb::SqlQuote;
using duckdb::string;
using duckdb::StringUtil;

//! Default bind host. Loopback, not a wildcard: the CLI is used on laptops, where binding
//! every interface by default would expose an unauthenticated ingest port to the local
//! network. The container image opts back into 0.0.0.0 via DUCKDB_OTLP_HOST in its Dockerfile.
constexpr const char *DEFAULT_HOST = "127.0.0.1";
constexpr int DEFAULT_HTTP_PORT = 4318;
constexpr int DEFAULT_GRPC_PORT = 4317;

string Env(const char *name, const string &fallback = "") {
	auto value = std::getenv(name);
	return value && value[0] ? string(value) : fallback;
}

bool HasEnv(const char *name) {
	auto value = std::getenv(name);
	return value && value[0];
}

bool Truthy(const string &value) {
	return value == "1" || value == "true" || value == "TRUE" || value == "yes" || value == "YES" || value == "on" ||
	       value == "ON";
}

//! Default data directory. The container pins this to /data via its Dockerfile; everywhere
//! else it follows the XDG base-directory spec, which macOS tolerates and Linux expects.
string DefaultDataDir() {
	auto xdg = Env("XDG_DATA_HOME");
	if (!xdg.empty()) {
		return xdg + "/duckdb-otlp";
	}
	auto home = Env("HOME");
	if (!home.empty()) {
		return home + "/.local/share/duckdb-otlp";
	}
	// No HOME (some init/container contexts): fall back to the working directory rather than
	// writing to an unpredictable absolute path.
	return "./duckdb-otlp-data";
}

//! True for hosts that are only reachable from this machine. Used to decide whether an
//! unauthenticated server is acceptable. A wildcard bind (0.0.0.0 / ::) is NOT loopback:
//! it accepts traffic from the whole network and therefore always needs a token.
bool IsLoopbackHost(const string &host) {
	if (host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]") {
		return true;
	}
	// The whole 127.0.0.0/8 block is loopback.
	return StringUtil::StartsWith(host, "127.");
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
void RejectUnsupportedOtelEnv() {
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
		if (HasEnv(entry.name)) {
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

string FirstEnv(std::initializer_list<const char *> names) {
	for (auto name : names) {
		if (HasEnv(name)) {
			return name;
		}
	}
	return "";
}

string RequireEnv(const char *name, const string &mode) {
	auto value = Env(name);
	if (value.empty()) {
		throw InvalidInputException("Missing required environment variable %s for DUCKDB_MODE=%s", name, mode);
	}
	return value;
}

string RequireAnyEnv(const string &label, std::initializer_list<const char *> names) {
	auto name = FirstEnv(names);
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

string R2EndpointDefault(const string &mode) {
	if (HasEnv("CLOUDFLARE_R2_ENDPOINT")) {
		return EndpointHost(Env("CLOUDFLARE_R2_ENDPOINT"));
	}
	if (HasEnv("CLOUDFLARE_S3_API_HOST")) {
		return EndpointHost(Env("CLOUDFLARE_S3_API_HOST"));
	}
	if (HasEnv("R2_ENDPOINT")) {
		return EndpointHost(Env("R2_ENDPOINT"));
	}
	return RequireEnv("CLOUDFLARE_ACCOUNT_ID", mode) + ".r2.cloudflarestorage.com";
}

string R2BucketValue() {
	auto var = RequireAnyEnv("Cloudflare R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	return Env(var.c_str());
}

string R2PrefixValue() {
	auto prefix = Env("CLOUDFLARE_R2_PREFIX", Env("R2_PREFIX", "duckdb-otlp/"));
	while (!prefix.empty() && prefix[0] == '/') {
		prefix = prefix.substr(1);
	}
	return prefix;
}

string R2DataPath() {
	auto bucket = R2BucketValue();
	auto prefix = R2PrefixValue();
	return prefix.empty() ? "s3://" + bucket + "/" : "s3://" + bucket + "/" + prefix;
}

string S3BucketValue() {
	auto var = RequireAnyEnv("S3 bucket", {"S3_BUCKET", "AWS_S3_BUCKET", "DUCKDB_OTLP_S3_BUCKET"});
	return Env(var.c_str());
}

string S3PrefixValue() {
	auto prefix = Env("S3_PREFIX", Env("AWS_S3_PREFIX", Env("DUCKDB_OTLP_S3_PREFIX", "duckdb-otlp/")));
	while (!prefix.empty() && prefix[0] == '/') {
		prefix = prefix.substr(1);
	}
	while (!prefix.empty() && prefix[prefix.size() - 1] == '/') {
		prefix = prefix.substr(0, prefix.size() - 1);
	}
	return prefix;
}

string S3DataPath() {
	auto bucket = S3BucketValue();
	auto prefix = S3PrefixValue();
	return prefix.empty() ? "s3://" + bucket : "s3://" + bucket + "/" + prefix;
}

bool IsS3Path(const string &path) {
	return StringUtil::StartsWith(StringUtil::Lower(path), "s3://");
}

void CreateDirectory(const string &path) {
	if (path.empty()) {
		return;
	}
	std::error_code ec;
	std::filesystem::create_directories(path, ec);
	if (ec) {
		throw InvalidInputException("Failed to create directory \"%s\": %s (check the mounted volume and permissions)",
		                            path, ec.message());
	}
}

void CreateParentDirectory(const string &path) {
	auto parent = std::filesystem::path(path).parent_path();
	if (parent.empty()) {
		return;
	}
	std::error_code ec;
	std::filesystem::create_directories(parent, ec);
	if (ec) {
		throw InvalidInputException(
		    "Failed to create parent directory \"%s\" for \"%s\": %s (check the mounted volume and permissions)",
		    parent.string(), path, ec.message());
	}
}

string CatalogDefault(const string &fallback) {
	return Env("DUCKDB_CATALOG", fallback);
}

string SchemaDefault(const string &fallback) {
	return Env("DUCKDB_SCHEMA", fallback);
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

string EnvSql(ServerConfig &config, const string &name, const string &fallback = "") {
	if (!HasEnv(name.c_str())) {
		return SqlQuote(fallback);
	}
	// Read the value through a session variable the daemon binds from the environment, not
	// getenv(): getenv() is a CLI-only DuckDB function and is NOT registered in the embedded
	// library the daemon links, so it errors at mode setup. Recording only the NAME keeps the
	// secret value out of the generated SQL text (which DRY_RUN prints).
	config.env_variables.push_back(name);
	return "getvariable(" + SqlQuote("env_" + name) + ")";
}

int ParsePositiveIntEnv(const char *name, int fallback) {
	auto value = Env(name);
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

uint64_t ParsePositiveUInt64Env(const char *name, uint64_t fallback) {
	auto value = Env(name);
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

int64_t ParsePositiveInt64Env(const char *name, int64_t fallback) {
	auto value = Env(name);
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
string DuckLakeInliningOption() {
	auto name = "DUCKLAKE_DATA_INLINING_ROW_LIMIT";
	auto value = Env(name);
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
string DuckLakePathAttachOptions() {
	auto option = DuckLakeInliningOption();
	return option.empty() ? string() : ",\n  " + option;
}

// Option clause for `ATTACH 'ducklake:<secret>' AS <name>`.
string DuckLakeSecretAttachOptions() {
	auto option = DuckLakeInliningOption();
	return option.empty() ? string() : " (" + option + ")";
}

// Cloudflare R2 access/secret-key env-var resolution. The candidate lists are identical across
// every R2 mode (r2-data-catalog, r2-local-ducklake, r2-neon-ducklake), so they live here once.
struct R2Credentials {
	string access_key_var;
	string secret_key_var;
};

R2Credentials ResolveR2Credentials(const string &label) {
	R2Credentials creds;
	creds.access_key_var = RequireAnyEnv(label + " access key",
	                                     {"CLOUDFLARE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_ACCESS_KEY_ID",
	                                      "CLOUDFLARE_R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_KEY_ID"});
	creds.secret_key_var =
	    RequireAnyEnv(label + " secret key",
	                  {"CLOUDFLARE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_ACCESS_KEY",
	                   "CLOUDFLARE_R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_KEY"});
	return creds;
}

// The KEY_ID/SECRET R2 storage secret block, byte-identical across the three R2 modes. The key/secret
// are referenced through getvariable() (via EnvSql) so the values never appear in the generated SQL.
// Returns the secret statement with a leading newline and a trailing ");\n" so it can be injected via
// %s exactly where the modes previously inlined it.
string BuildR2StorageSecret(ServerConfig &config, const string &secret_name, const R2Credentials &creds,
                            const string &endpoint) {
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
	                          secret_name, EnvSql(config, creds.access_key_var), EnvSql(config, creds.secret_key_var),
	                          SqlQuote(endpoint));
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

void ConfigureLocalDuckLake(ServerConfig &config) {
	config.mode_extensions = {"ducklake", "otlp"};
	config.catalog = CatalogDefault(Env("DUCKLAKE_NAME", "otel"));
	config.schema = SchemaDefault("main");
	auto catalog_path = Env("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = Env("DUCKLAKE_DATA_PATH", config.data_dir + "/ducklake/storage");
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
	                                           SqlQuote(data_path), DuckLakePathAttachOptions());
}

void ConfigureAwsDuckLake(ServerConfig &config) {
	config.mode_extensions = {"ducklake", "aws", "httpfs", "otlp"};
	config.catalog = CatalogDefault(Env("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault("otlp");
	auto catalog_path = Env("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = Env("DUCKLAKE_DATA_PATH");
	if (!IsS3Path(data_path)) {
		throw InvalidInputException("DUCKDB_MODE=%s requires DUCKLAKE_DATA_PATH=s3://bucket/prefix", config.mode);
	}
	auto region = Env("AWS_REGION", Env("AWS_DEFAULT_REGION"));
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
	                       SqlQuote(data_path), DuckLakePathAttachOptions());
}

void ConfigureR2DataCatalog(ServerConfig &config) {
	config.mode_extensions = {"iceberg", "httpfs", "otlp"};
	config.catalog = CatalogDefault(Env("CLOUDFLARE_CATALOG_NAME", "r2catalog"));
	config.schema = SchemaDefault("otlp");
	auto catalog_token_var =
	    RequireAnyEnv("Cloudflare catalog token", {"CLOUDFLARE_CATALOG_TOKEN", "CLOUDFLARE_API_TOKEN"});
	auto creds = ResolveR2Credentials("Cloudflare R2");
	RequireAnyEnv("Cloudflare R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	RequireEnv("CLOUDFLARE_ACCOUNT_ID", config.mode);
	auto catalog_uri = RequireEnv("CLOUDFLARE_CATALOG_URI", config.mode);
	auto warehouse = Env("CLOUDFLARE_WAREHOUSE", Env("R2_WAREHOUSE"));
	if (warehouse.empty() && HasEnv("CLOUDFLARE_ACCOUNT_ID")) {
		warehouse = Env("CLOUDFLARE_ACCOUNT_ID") + "_" + R2BucketValue();
	}
	if (warehouse.empty()) {
		throw InvalidInputException("Missing Cloudflare warehouse. Set CLOUDFLARE_WAREHOUSE or provide "
		                            "CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_R2_BUCKET");
	}
	auto endpoint = R2EndpointDefault(config.mode);
	auto storage_secret = BuildR2StorageSecret(config, "cloudflare_r2_secret", creds, endpoint);

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
	    storage_secret, EnvSql(config, catalog_token_var), SqlQuote(warehouse), QuoteIdentifier(config.catalog),
	    SqlQuote(catalog_uri));
}

void ConfigureParquet(ServerConfig &config) {
	config.mode_extensions = {"otlp"};
	config.catalog = "";
	config.schema = SchemaDefault("otlp");
	config.parquet_export_path =
	    Env("PARQUET_EXPORT_PATH",
	        Env("DUCKDB_OTLP_PARQUET_EXPORT_PATH", Env("S3_EXPORT_PATH", Env("DUCKDB_OTLP_S3_EXPORT_PATH"))));
	if (config.parquet_export_path.empty()) {
		if (HasEnv("S3_BUCKET") || HasEnv("AWS_S3_BUCKET") || HasEnv("DUCKDB_OTLP_S3_BUCKET")) {
			config.parquet_export_path = S3DataPath();
		} else {
			config.parquet_export_path = config.data_dir + "/parquet";
		}
	}

	if (!IsS3Path(config.parquet_export_path)) {
		CreateDirectory(config.parquet_export_path);
		config.mode_setup_sql = "";
		return;
	}

	config.mode_extensions = {"aws", "httpfs", "otlp"};
	auto region = Env("AWS_REGION", Env("AWS_DEFAULT_REGION"));
	if (region.empty()) {
		throw InvalidInputException("Missing AWS region for DUCKDB_MODE=%s with an s3:// export path. Set AWS_REGION "
		                            "or AWS_DEFAULT_REGION",
		                            config.mode);
	}
	auto profile = Env("AWS_PROFILE", Env("AWS_DEFAULT_PROFILE"));
	auto endpoint = Env("S3_ENDPOINT", Env("AWS_S3_ENDPOINT"));
	auto url_style = Env("S3_URL_STYLE", Env("AWS_S3_URL_STYLE"));

	auto secret_sql = BuildCredentialChainSecret("plain_s3_secret", region, profile, endpoint, url_style);

	config.mode_setup_sql = StringUtil::Format(R"SQL(
INSTALL aws;
INSTALL httpfs;
LOAD aws;
LOAD httpfs;
%s)SQL",
	                                           secret_sql);
}

void ConfigureR2LocalDuckLake(ServerConfig &config) {
	config.mode_extensions = {"ducklake", "httpfs", "otlp"};
	config.catalog = CatalogDefault(Env("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault("otlp");
	auto creds = ResolveR2Credentials("R2");
	RequireAnyEnv("R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	auto catalog_path = Env("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = Env("DUCKLAKE_DATA_PATH", R2DataPath());
	CreateParentDirectory(catalog_path);
	auto endpoint = R2EndpointDefault(config.mode);
	auto storage_secret = BuildR2StorageSecret(config, "r2_storage", creds, endpoint);

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
	                       SqlQuote(data_path), DuckLakePathAttachOptions());
}

void ConfigureR2NeonDuckLake(ServerConfig &config) {
	config.mode_extensions = {"ducklake", "postgres", "httpfs", "otlp"};
	config.catalog = CatalogDefault(Env("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault("otlp");
	auto creds = ResolveR2Credentials("R2");
	RequireAnyEnv("R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	RequireEnv("NEON_PGHOST", config.mode);
	RequireEnv("NEON_PGDATABASE", config.mode);
	RequireEnv("NEON_PGUSER", config.mode);
	RequireEnv("NEON_PGPASSWORD", config.mode);
	auto data_path = Env("DUCKLAKE_DATA_PATH", R2DataPath());
	auto endpoint = R2EndpointDefault(config.mode);
	auto storage_secret = BuildR2StorageSecret(config, "r2_storage", creds, endpoint);

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
	    storage_secret, EnvSql(config, "NEON_PGHOST"), EnvSql(config, "NEON_PGPORT", "5432"),
	    EnvSql(config, "NEON_PGDATABASE"), EnvSql(config, "NEON_PGUSER"), EnvSql(config, "NEON_PGPASSWORD"),
	    EnvSql(config, "NEON_PGSSLMODE", "require"), SqlQuote(data_path), QuoteIdentifier(config.catalog),
	    DuckLakeSecretAttachOptions());
}

void ConfigureGcpDuckLake(ServerConfig &config) {
	config.mode_extensions = {"ducklake", "postgres", "gcs", "otlp"};
	config.catalog = CatalogDefault(Env("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault("otlp");
	auto data_path = RequireEnv("DUCKLAKE_DATA_PATH", config.mode);
	// Force the native GCS filesystem even if httpfs is loaded by another extension.
	if (!StringUtil::StartsWith(data_path, "gcss://") || data_path.size() <= 7 || data_path[7] == '/') {
		throw InvalidInputException("DUCKDB_MODE=gcp-ducklake requires DUCKLAKE_DATA_PATH=gcss://bucket/prefix");
	}
	for (auto name : {"PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"}) {
		RequireEnv(name, config.mode);
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
	    EnvSql(config, "PGHOST"), EnvSql(config, "PGPORT", "5432"), EnvSql(config, "PGDATABASE"),
	    EnvSql(config, "PGUSER"), EnvSql(config, "PGPASSWORD"), EnvSql(config, "PGSSLMODE", "require"),
	    SqlQuote(data_path), QuoteIdentifier(config.catalog), DuckLakeSecretAttachOptions());
}

void ConfigureS3Tables(ServerConfig &config) {
	config.mode_extensions = {"iceberg", "aws", "httpfs", "otlp"};
	config.catalog = CatalogDefault(Env("S3_TABLES_CATALOG_NAME", "s3tables"));
	config.schema = SchemaDefault("otlp");
	auto bucket_arn = Env("S3_TABLES_BUCKET_ARN", Env("S3_TABLES_TABLE_BUCKET_ARN", Env("TABLE_BUCKET_ARN")));
	if (bucket_arn.empty()) {
		throw InvalidInputException(
		    "Missing S3 Tables bucket ARN. Set S3_TABLES_BUCKET_ARN, S3_TABLES_TABLE_BUCKET_ARN, or TABLE_BUCKET_ARN");
	}
	auto region = Env("AWS_REGION", Env("AWS_DEFAULT_REGION"));
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
	auto profile = Env("AWS_PROFILE", Env("AWS_DEFAULT_PROFILE"));
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

void ConfigureMode(ServerConfig &config) {
	if (config.mode == "local-ducklake") {
		ConfigureLocalDuckLake(config);
	} else if (config.mode == "gcp-ducklake") {
		ConfigureGcpDuckLake(config);
	} else if (config.mode == "aws-ducklake") {
		ConfigureAwsDuckLake(config);
	} else if (config.mode == "parquet") {
		ConfigureParquet(config);
	} else if (config.mode == "r2-data-catalog") {
		ConfigureR2DataCatalog(config);
	} else if (config.mode == "s3-tables") {
		ConfigureS3Tables(config);
	} else if (config.mode == "r2-neon-ducklake") {
		ConfigureR2NeonDuckLake(config);
	} else if (config.mode == "r2-local-ducklake") {
		ConfigureR2LocalDuckLake(config);
	} else {
		throw InvalidInputException(
		    "Unsupported DUCKDB_MODE \"%s\". Supported modes: local-ducklake, aws-ducklake, parquet, "
		    "r2-data-catalog, s3-tables, r2-neon-ducklake, r2-local-ducklake, gcp-ducklake",
		    config.mode);
	}
}

} // namespace

bool EnvTruthy(const char *name) {
	return Truthy(Env(name));
}

namespace {

//! One transport's resolved bind address, or `enabled == false` when it is switched off.
struct ResolvedListener {
	bool enabled = false;
	string host;
	int port = 0;
};

//! Parse a port env var. Unlike the other numeric parsers, 0 is legal and means
//! "disable this listener", which is how --http 0 / --grpc 0 switch a transport off.
int ParsePortEnv(const char *name) {
	auto value = Env(name);
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

//! Host portion of a bare "host:port" / "[ipv6]:port" bind address. Empty input -> "".
string SplitAddrHost(const string &addr) {
	if (addr.empty()) {
		return "";
	}
	if (addr[0] == '[') {
		auto close = addr.find(']');
		return close == string::npos ? addr : addr.substr(1, close - 1);
	}
	auto colon = addr.rfind(':');
	return colon == string::npos ? addr : addr.substr(0, colon);
}

//! Port portion of a bare "host:port" bind address. A missing port falls back to
//! `default_port`; a port that is PRESENT but unparseable or out of range is an error, never
//! a silent fallback — quietly binding 4317 because someone typed 70000 hides the typo until
//! traffic goes missing.
int SplitAddrPort(const string &addr, int default_port, const char *addr_var) {
	auto colon = addr.rfind(':');
	if (colon == string::npos) {
		return default_port;
	}
	auto port_text = addr.substr(colon + 1);
	try {
		size_t pos = 0;
		auto parsed = std::stoll(port_text, &pos);
		if (pos == port_text.size() && parsed > 0 && parsed <= 65535) {
			return static_cast<int>(parsed);
		}
	} catch (...) {
	}
	throw InvalidInputException("%s has an invalid port \"%s\": expected a port between 1 and 65535",
	                            addr_var ? addr_var : "the bind address", port_text);
}

//! Resolve one transport's listener from, in order: its DUCKDB_OTLP_*_PORT variable, its
//! legacy OTEL_*_ADDR variable, then the built-in default. DUCKDB_OTLP_HOST always wins
//! over a host embedded in an OTEL_*_ADDR, so `--host` can move every listener at once.
ResolvedListener ResolveListener(const char *port_var, const char *addr_var, int default_port, bool enabled_default) {
	ResolvedListener resolved;
	auto explicit_host = Env("DUCKDB_OTLP_HOST");
	auto addr = addr_var ? Env(addr_var) : string();

	if (HasEnv(port_var)) {
		auto port = ParsePortEnv(port_var);
		resolved.enabled = port != 0;
		resolved.port = port;
	} else if (!addr.empty()) {
		resolved.enabled = enabled_default;
		resolved.port = SplitAddrPort(addr, default_port, addr_var);
	} else {
		resolved.enabled = enabled_default;
		resolved.port = default_port;
	}

	if (!explicit_host.empty()) {
		resolved.host = explicit_host;
	} else if (!addr.empty() && !SplitAddrHost(addr).empty()) {
		resolved.host = SplitAddrHost(addr);
	} else {
		resolved.host = DEFAULT_HOST;
	}
	return resolved;
}

//! Transport selection from OTEL_EXPORTER_OTLP_PROTOCOL, consulted ONLY when nothing more
//! specific selected the transports. The variable is exporter-side in the OTel spec, and a
//! developer's shell often already carries it for their application's exporter, so letting
//! it silently narrow an explicitly-configured server would be surprising.
void ApplyOtelProtocol(bool &http_enabled, bool &grpc_enabled, string &reason) {
	auto protocol = Env("OTEL_EXPORTER_OTLP_PROTOCOL");
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
std::vector<IngestListener> ListenersFromTransportList(const string &override_uri) {
	auto transports = Env("DUCKDB_OTLP_TRANSPORTS", "http");
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
		                    ? ResolveListener("DUCKDB_OTLP_HTTP_PORT", "OTEL_HTTP_ADDR", DEFAULT_HTTP_PORT, true)
		                    : ResolveListener("DUCKDB_OTLP_GRPC_PORT", "OTEL_GRPC_ADDR", DEFAULT_GRPC_PORT, true);
		duckdb::OtlpUri uri(override_uri.empty() ? "otlp:" + resolved.host + ":" + std::to_string(resolved.port)
		                                         : override_uri);
		listeners.push_back({uri.Uri(), transport, false});
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

std::vector<IngestListener> ListenersFromEnv(string *selection_reason) {
	RejectUnsupportedOtelEnv();
	string reason;
	auto override_uri = Env("DUCKDB_OTLP_LISTEN_URI");
	bool ports_set =
	    HasEnv("DUCKDB_OTLP_HTTP_PORT") || HasEnv("DUCKDB_OTLP_GRPC_PORT") || HasEnv("DUCKDB_OTLP_OTAP_PORT");

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
			if (HasEnv("DUCKDB_OTLP_TRANSPORTS")) {
				throw InvalidInputException("DUCKDB_OTLP_TRANSPORTS cannot be combined with an otap: listen URI");
			}
			return finish({{duckdb::OtlpUri(override_uri).Uri(), "grpc", true}});
		}
		return finish(ListenersFromTransportList(override_uri));
	}

	// (2) Legacy transport list, unless explicit ports supersede it. The container image sets
	// DUCKDB_OTLP_TRANSPORTS in its Dockerfile, so a container user passing --http/--grpc
	// still lands on the port path below.
	if (HasEnv("DUCKDB_OTLP_TRANSPORTS") && !ports_set) {
		reason = "DUCKDB_OTLP_TRANSPORTS=" + Env("DUCKDB_OTLP_TRANSPORTS");
		return finish(ListenersFromTransportList(""));
	}

	// (3) Port-based selection: the CLI path. Any explicit port switches to explicit mode,
	// where a transport is on only if its port is set and non-zero.
	bool http_enabled = !ports_set;
	bool grpc_enabled = !ports_set;
	bool otap_enabled = false;
	if (ports_set) {
		reason = "explicit ports";
		http_enabled = HasEnv("DUCKDB_OTLP_HTTP_PORT") && ParsePortEnv("DUCKDB_OTLP_HTTP_PORT") != 0;
		grpc_enabled = HasEnv("DUCKDB_OTLP_GRPC_PORT") && ParsePortEnv("DUCKDB_OTLP_GRPC_PORT") != 0;
		otap_enabled = HasEnv("DUCKDB_OTLP_OTAP_PORT") && ParsePortEnv("DUCKDB_OTLP_OTAP_PORT") != 0;
	} else {
		reason = "default (OTLP/HTTP and OTLP/gRPC)";
		// (4) Nothing explicit: let the standard exporter protocol variable narrow the default.
		ApplyOtelProtocol(http_enabled, grpc_enabled, reason);
	}

	if (otap_enabled && (http_enabled || grpc_enabled)) {
		// OTAP/Arrow is served by otap_serve and standard OTLP by otlp_serve; a single
		// process starts one or the other, never both, so catch it here with a clear message
		// rather than at the SQL layer.
		throw InvalidInputException("OTAP/Arrow cannot be combined with standard OTLP listeners. Start OTAP alone "
		                            "(--otap PORT --http 0 --grpc 0), or run a second process for it.");
	}
	if (!http_enabled && !grpc_enabled && !otap_enabled) {
		throw InvalidInputException("Every listener is disabled. Enable at least one of --http, --grpc, or --otap.");
	}

	std::vector<IngestListener> listeners;
	if (otap_enabled) {
		auto resolved = ResolveListener("DUCKDB_OTLP_OTAP_PORT", nullptr, DEFAULT_GRPC_PORT, true);
		listeners.push_back(
		    {duckdb::OtlpUri("otap:" + resolved.host + ":" + std::to_string(resolved.port)).Uri(), "grpc", true});
		return finish(std::move(listeners));
	}
	if (http_enabled) {
		auto resolved = ResolveListener("DUCKDB_OTLP_HTTP_PORT", "OTEL_HTTP_ADDR", DEFAULT_HTTP_PORT, true);
		listeners.push_back(
		    {duckdb::OtlpUri("otlp:" + resolved.host + ":" + std::to_string(resolved.port)).Uri(), "http", false});
	}
	if (grpc_enabled) {
		auto resolved = ResolveListener("DUCKDB_OTLP_GRPC_PORT", "OTEL_GRPC_ADDR", DEFAULT_GRPC_PORT, true);
		listeners.push_back(
		    {duckdb::OtlpUri("otlp:" + resolved.host + ":" + std::to_string(resolved.port)).Uri(), "grpc", false});
	}
	return finish(std::move(listeners));
}

ServerConfig ServerConfig::FromEnv() {
	ServerConfig config;
	// DUCKDB_MODE is no longer required: a bare `duckdb-otlp` on a laptop should start a
	// working local lakehouse with no configuration at all. Every other mode still has to be
	// named explicitly, so this default cannot silently redirect an intended remote target.
	config.mode = NormalizeMode(Env("DUCKDB_MODE", "local-ducklake"));
	config.data_dir = Env("DUCKDB_OTLP_DATA_DIR", DefaultDataDir());
	// Derived from data_dir rather than hard-coded to /data, so overriding the data directory
	// moves the control database with it. The container sets DUCKDB_OTLP_DATA_DIR=/data, which
	// reproduces the previous default path exactly.
	config.database = Env("DUCKDB_DATABASE", config.data_dir + "/duckdb-otlp-control.duckdb");
	config.listeners = ListenersFromEnv(&config.transport_selection);
	config.log_level = Env("OTEL_LOG_LEVEL");
	// Token resolution, most specific first. OTEL_EXPORTER_OTLP_HEADERS is the standard
	// exporter-side spelling ("Authorization=Bearer <token>"); accepting it lets one variable
	// configure both an exporter and this receiver.
	config.token = Env("OTEL_AUTH_TOKEN", Env("DUCKDB_OTLP_TOKEN"));
	if (config.token.empty() && HasEnv("OTEL_EXPORTER_OTLP_HEADERS")) {
		config.token = BearerTokenFromOtelHeaders(Env("OTEL_EXPORTER_OTLP_HEADERS"));
	}
	config.disable_auth = Truthy(Env("DUCKDB_OTLP_DISABLE_AUTH", "0"));
	config.quack_enabled = Truthy(Env("DUCKDB_QUACK_ENABLED", Env("QUACK_ENABLED", "0")));
	// --quack PORT sets DUCKDB_QUACK_PORT; the legacy DUCKDB_QUACK_ADDR / QUACK_HTTP_ADDR
	// host:port form still wins when no port was given explicitly.
	auto quack_host = Env("DUCKDB_OTLP_HOST", DEFAULT_HOST);
	config.quack_http_addr = HasEnv("DUCKDB_QUACK_PORT")
	                             ? quack_host + ":" + std::to_string(ParsePortEnv("DUCKDB_QUACK_PORT"))
	                             : Env("DUCKDB_QUACK_ADDR", Env("QUACK_HTTP_ADDR", string(DEFAULT_HOST) + ":9494"));
	config.quack_listen_uri = Env("DUCKDB_QUACK_LISTEN_URI", "quack:" + config.quack_http_addr);
	config.dry_run = Truthy(Env("DRY_RUN", "0"));
	config.startup_timeout_secs = ParsePositiveIntEnv("DUCKDB_OTLP_STARTUP_TIMEOUT", 60);
	config.http_threads = ParsePositiveUInt64Env("DUCKDB_OTLP_HTTP_THREADS", 0);
	config.max_body_bytes = ParsePositiveUInt64Env("DUCKDB_OTLP_MAX_BODY_BYTES", otlp_limits::DEFAULT_MAX_BODY_BYTES);
	config.max_buffered_bytes =
	    ParsePositiveUInt64Env("DUCKDB_OTLP_MAX_BUFFERED_BYTES", otlp_limits::DEFAULT_MAX_BUFFERED_BYTES);
	config.seal_target_bytes =
	    ParsePositiveUInt64Env("DUCKDB_OTLP_SEAL_TARGET_BYTES", otlp_limits::DEFAULT_SEAL_TARGET_BYTES);
	config.seal_max_age_ms = ParsePositiveInt64Env("DUCKDB_OTLP_SEAL_MAX_AGE_MS", otlp_limits::DEFAULT_SEAL_MAX_AGE_MS);
	config.target_file_size =
	    ParsePositiveUInt64Env("DUCKDB_OTLP_TARGET_FILE_SIZE", otlp_limits::DEFAULT_TARGET_FILE_SIZE);
	config.maintenance_retention_ms =
	    ParsePositiveInt64Env("DUCKDB_OTLP_MAINTENANCE_RETENTION_MS", otlp_limits::DEFAULT_MAINTENANCE_RETENTION_MS);
	config.promote_resource_attributes = Env("DUCKDB_OTLP_PROMOTE_RESOURCE_ATTRIBUTES", "");
	config.promote_scope_attributes = Env("DUCKDB_OTLP_PROMOTE_SCOPE_ATTRIBUTES", "");

	auto quack_token_var = FirstEnv({"DUCKDB_QUACK_TOKEN", "QUACK_AUTH_TOKEN"});
	if (config.quack_enabled && quack_token_var.empty()) {
		throw InvalidInputException(
		    "DUCKDB_QUACK_ENABLED=1 requires a dedicated Quack token. Set DUCKDB_QUACK_TOKEN or QUACK_AUTH_TOKEN.");
	}
	if (!quack_token_var.empty()) {
		config.quack_token = Env(quack_token_var.c_str());
	}

	// Authentication. There is deliberately no built-in default token: a token published in
	// this repository authenticates nothing, and silently falling back to one gave servers the
	// appearance of being protected. Instead, an unauthenticated server is allowed only where
	// it cannot be reached from off the machine.
	bool all_loopback = true;
	for (const auto &listener : config.listeners) {
		all_loopback = all_loopback && IsLoopbackHost(duckdb::OtlpUri(listener.uri).Host());
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
	ConfigureMode(config);
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
	// main.cpp reads it to exit non-zero on a data-dropping shutdown (review finding M4). Any
	// listener URI names the whole server, so one stop closes every listener and drains once.
	return StringUtil::Format("SELECT status, dropped_rows FROM otlp_stop(%s);", SqlQuote(listeners.front().uri));
}

string ServerConfig::StopQuackSql() const {
	return StringUtil::Format("CALL quack_stop(%s);", SqlQuote(quack_listen_uri));
}

string ServerConfig::BootSql() const {
	return mode_setup_sql + "\n" + StartOtlpSql() + "\n" + StartQuackSql();
}

} // namespace duckdb_otlp_server
