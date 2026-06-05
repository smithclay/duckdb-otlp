#include "server_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <sstream>
#include <system_error>

namespace duckdb_otlp_server {
namespace {

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

constexpr const char *DEFAULT_TOKEN = "dev-otlp-token-123456";

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

string SqlEscape(const string &value) {
	return StringUtil::Replace(value, "'", "''");
}

string SqlQuote(const string &value) {
	return "'" + SqlEscape(value) + "'";
}

string QuoteIdent(const string &value) {
	return "\"" + StringUtil::Replace(value, "\"", "\"\"") + "\"";
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

void ConfigureLocalDuckLake(ServerConfig &config) {
	config.mode_extensions = {"ducklake", "otlp"};
	config.catalog = CatalogDefault(Env("DUCKLAKE_NAME", "otel"));
	config.schema = SchemaDefault("main");
	auto catalog_path = Env("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = Env("DUCKLAKE_DATA_PATH", config.data_dir + "/ducklake/storage");
	CreateParentDirectory(catalog_path);
	CreateDirectory(data_path);

	config.mode_setup_sql =
	    StringUtil::Format(R"SQL(
INSTALL ducklake;
LOAD ducklake;
ATTACH %s AS %s (
  DATA_PATH %s
);
)SQL",
	                       SqlQuote("ducklake:" + catalog_path), QuoteIdent(config.catalog), SqlQuote(data_path));
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
	config.mode_setup_sql = StringUtil::Format(R"SQL(
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
  DATA_PATH %s
);
)SQL",
	                                           SqlQuote(region), SqlQuote("ducklake:" + catalog_path),
	                                           QuoteIdent(config.catalog), SqlQuote(data_path));
}

void ConfigureR2DataCatalog(ServerConfig &config) {
	config.mode_extensions = {"iceberg", "httpfs", "otlp"};
	config.catalog = CatalogDefault(Env("CLOUDFLARE_CATALOG_NAME", "r2catalog"));
	config.schema = SchemaDefault("otlp");
	auto catalog_token_var =
	    RequireAnyEnv("Cloudflare catalog token", {"CLOUDFLARE_CATALOG_TOKEN", "CLOUDFLARE_API_TOKEN"});
	auto access_key_var = RequireAnyEnv("Cloudflare R2 access key",
	                                    {"CLOUDFLARE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_ACCESS_KEY_ID",
	                                     "CLOUDFLARE_R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_KEY_ID"});
	auto secret_key_var =
	    RequireAnyEnv("Cloudflare R2 secret key",
	                  {"CLOUDFLARE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_ACCESS_KEY",
	                   "CLOUDFLARE_R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_KEY"});
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

	config.mode_setup_sql = StringUtil::Format(R"SQL(
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;
CREATE OR REPLACE SECRET cloudflare_r2_secret (
  TYPE s3,
  KEY_ID %s,
  SECRET %s,
  REGION 'auto',
  ENDPOINT %s,
  URL_STYLE 'path'
);
CREATE OR REPLACE SECRET cloudflare_catalog_secret (
  TYPE ICEBERG,
  TOKEN %s
);
ATTACH %s AS %s (
  TYPE ICEBERG,
  ENDPOINT %s,
  SECRET cloudflare_catalog_secret
);
)SQL",
	                                           EnvSql(config, access_key_var), EnvSql(config, secret_key_var),
	                                           SqlQuote(endpoint), EnvSql(config, catalog_token_var),
	                                           SqlQuote(warehouse), QuoteIdent(config.catalog), SqlQuote(catalog_uri));
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

	auto secret_sql = StringUtil::Format(R"SQL(
CREATE OR REPLACE SECRET plain_s3_secret (
  TYPE s3,
  PROVIDER credential_chain,
)SQL");
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
	auto access_key_var =
	    RequireAnyEnv("R2 access key", {"CLOUDFLARE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_ACCESS_KEY_ID",
	                                    "CLOUDFLARE_R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_KEY_ID"});
	auto secret_key_var = RequireAnyEnv(
	    "R2 secret key", {"CLOUDFLARE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_ACCESS_KEY",
	                      "CLOUDFLARE_R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_KEY"});
	RequireAnyEnv("R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	auto catalog_path = Env("DUCKLAKE_CATALOG_PATH", config.data_dir + "/ducklake/catalog.duckdb");
	auto data_path = Env("DUCKLAKE_DATA_PATH", R2DataPath());
	CreateParentDirectory(catalog_path);
	auto endpoint = R2EndpointDefault(config.mode);

	config.mode_setup_sql =
	    StringUtil::Format(R"SQL(
INSTALL ducklake;
INSTALL httpfs;
LOAD ducklake;
LOAD httpfs;
CREATE OR REPLACE SECRET r2_storage (
  TYPE s3,
  KEY_ID %s,
  SECRET %s,
  REGION 'auto',
  ENDPOINT %s,
  URL_STYLE 'path'
);
ATTACH %s AS %s (
  DATA_PATH %s
);
)SQL",
	                       EnvSql(config, access_key_var), EnvSql(config, secret_key_var), SqlQuote(endpoint),
	                       SqlQuote("ducklake:" + catalog_path), QuoteIdent(config.catalog), SqlQuote(data_path));
}

void ConfigureR2NeonDuckLake(ServerConfig &config) {
	config.mode_extensions = {"ducklake", "postgres", "httpfs", "otlp"};
	config.catalog = CatalogDefault(Env("DUCKLAKE_NAME", "lake"));
	config.schema = SchemaDefault("otlp");
	auto access_key_var =
	    RequireAnyEnv("R2 access key", {"CLOUDFLARE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_ACCESS_KEY_ID",
	                                    "CLOUDFLARE_R2_ACCESS_KEY_ID", "CLOUDFLARE_S3_KEY_ID"});
	auto secret_key_var = RequireAnyEnv(
	    "R2 secret key", {"CLOUDFLARE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_ACCESS_KEY",
	                      "CLOUDFLARE_R2_SECRET_ACCESS_KEY", "CLOUDFLARE_S3_SECRET_KEY"});
	RequireAnyEnv("R2 bucket", {"CLOUDFLARE_R2_BUCKET", "R2_BUCKET_NAME", "R2_BUCKET"});
	RequireEnv("NEON_PGHOST", config.mode);
	RequireEnv("NEON_PGDATABASE", config.mode);
	RequireEnv("NEON_PGUSER", config.mode);
	RequireEnv("NEON_PGPASSWORD", config.mode);
	auto data_path = Env("DUCKLAKE_DATA_PATH", R2DataPath());
	auto endpoint = R2EndpointDefault(config.mode);

	config.mode_setup_sql = StringUtil::Format(
	    R"SQL(
INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
LOAD ducklake;
LOAD postgres;
LOAD httpfs;
CREATE OR REPLACE SECRET r2_storage (
  TYPE s3,
  KEY_ID %s,
  SECRET %s,
  REGION 'auto',
  ENDPOINT %s,
  URL_STYLE 'path'
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
ATTACH 'ducklake:ducklake_secret' AS %s;
)SQL",
	    EnvSql(config, access_key_var), EnvSql(config, secret_key_var), SqlQuote(endpoint),
	    EnvSql(config, "NEON_PGHOST"), EnvSql(config, "NEON_PGPORT", "5432"), EnvSql(config, "NEON_PGDATABASE"),
	    EnvSql(config, "NEON_PGUSER"), EnvSql(config, "NEON_PGPASSWORD"), EnvSql(config, "NEON_PGSSLMODE", "require"),
	    SqlQuote(data_path), QuoteIdent(config.catalog));
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
	auto secret_sql = StringUtil::Format(R"SQL(
CREATE OR REPLACE SECRET s3_tables_secret (
  TYPE s3,
  PROVIDER credential_chain,
)SQL");
	if (!profile.empty()) {
		secret_sql += StringUtil::Format("  CHAIN config,\n  PROFILE %s,\n", SqlQuote(profile));
	} else {
		secret_sql += "  CHAIN env,\n";
	}
	secret_sql += StringUtil::Format("  REGION %s\n);\n", SqlQuote(region));

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
	                                           secret_sql, SqlQuote(bucket_arn), QuoteIdent(config.catalog));
}

void ConfigureMode(ServerConfig &config) {
	if (config.mode == "local-ducklake") {
		ConfigureLocalDuckLake(config);
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
		    "r2-data-catalog, s3-tables, r2-neon-ducklake, r2-local-ducklake",
		    config.mode);
	}
}

} // namespace

ServerConfig ServerConfig::FromEnv() {
	auto raw_mode = Env("DUCKDB_MODE");
	if (raw_mode.empty()) {
		throw InvalidInputException("Missing required environment variable DUCKDB_MODE");
	}

	ServerConfig config;
	config.mode = NormalizeMode(raw_mode);
	config.database = Env("DUCKDB_DATABASE", "/data/duckdb-otlp-control.duckdb");
	config.data_dir = Env("DUCKDB_OTLP_DATA_DIR", "/data");
	config.otel_http_addr = Env("OTEL_HTTP_ADDR", "0.0.0.0:4318");
	config.listen_uri = Env("DUCKDB_OTLP_LISTEN_URI", "otlp:" + config.otel_http_addr);
	config.token = Env("OTEL_AUTH_TOKEN", Env("DUCKDB_OTLP_TOKEN", DEFAULT_TOKEN));
	config.using_default_token = config.token == DEFAULT_TOKEN;
	config.quack_enabled = Truthy(Env("DUCKDB_QUACK_ENABLED", Env("QUACK_ENABLED", "0")));
	config.quack_http_addr = Env("DUCKDB_QUACK_ADDR", Env("QUACK_HTTP_ADDR", "0.0.0.0:9494"));
	config.quack_listen_uri = Env("DUCKDB_QUACK_LISTEN_URI", "quack:" + config.quack_http_addr);
	config.dry_run = Truthy(Env("DRY_RUN", "0"));
	config.startup_timeout_secs = ParsePositiveIntEnv("DUCKDB_OTLP_STARTUP_TIMEOUT", 60);
	config.http_threads = ParsePositiveUInt64Env("DUCKDB_OTLP_HTTP_THREADS", 0);
	config.max_body_bytes = ParsePositiveUInt64Env("DUCKDB_OTLP_MAX_BODY_BYTES", 16ULL * 1024ULL * 1024ULL);
	config.max_buffered_bytes = ParsePositiveUInt64Env("DUCKDB_OTLP_MAX_BUFFERED_BYTES", 512ULL * 1024ULL * 1024ULL);

	auto quack_token_var = FirstEnv({"DUCKDB_QUACK_TOKEN", "QUACK_AUTH_TOKEN"});
	if (config.quack_enabled && quack_token_var.empty()) {
		throw InvalidInputException(
		    "DUCKDB_QUACK_ENABLED=1 requires a dedicated Quack token. Set DUCKDB_QUACK_TOKEN or QUACK_AUTH_TOKEN.");
	}
	if (!quack_token_var.empty()) {
		config.quack_token = Env(quack_token_var.c_str());
	}

	CreateDirectory(config.data_dir);
	ConfigureMode(config);
	ValidateCatalogDoesNotShadowDatabase(config);
	return config;
}

string ServerConfig::StartOtlpSql() const {
	auto thread_sql = http_threads == 0
	                      ? string("")
	                      : StringUtil::Format(",\n    http_threads := %llu", static_cast<uint64_t>(http_threads));
	auto limits_sql =
	    StringUtil::Format(",\n    max_body_bytes := %llu,\n    max_buffered_bytes := %llu",
	                       static_cast<uint64_t>(max_body_bytes), static_cast<uint64_t>(max_buffered_bytes));
	auto export_sql = parquet_export_path.empty()
	                      ? string("")
	                      : StringUtil::Format(",\n    parquet_export_path := %s", SqlQuote(parquet_export_path));
	auto schema_target = catalog.empty() ? QuoteIdent(schema) : QuoteIdent(catalog) + "." + QuoteIdent(schema);
	// The token is read at execution time from a session variable (set via the C++ API in
	// main.cpp) rather than interpolated as a literal, so it never appears in the generated
	// SQL string (which DRY_RUN=1 prints to stdout and the engine can echo in error
	// messages).
	return StringUtil::Format(R"SQL(
CREATE SCHEMA IF NOT EXISTS %s;
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
    %s,
    catalog := %s,
    schema := %s,
    token := getvariable('duckdb_otlp_effective_token'),
    allow_other_hostname := true%s%s%s
);
)SQL",
	                          schema_target, SqlQuote(listen_uri), SqlQuote(catalog), SqlQuote(schema), thread_sql,
	                          limits_sql, export_sql);
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
	return StringUtil::Format("SELECT status FROM otlp_stop(%s);", SqlQuote(listen_uri));
}

string ServerConfig::StopQuackSql() const {
	return StringUtil::Format("CALL quack_stop(%s);", SqlQuote(quack_listen_uri));
}

string ServerConfig::BootSql() const {
	return mode_setup_sql + "\n" + StartOtlpSql() + "\n" + StartQuackSql();
}

} // namespace duckdb_otlp_server
