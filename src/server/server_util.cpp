#include "server_util.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

namespace duckdb_otlp_server {

using duckdb::InvalidInputException;
using duckdb::string;

void CheckResult(duckdb::QueryResult &result, const string &label) {
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

void Execute(duckdb::Connection &con, const string &sql, const string &label, bool print_result) {
	if (sql.empty()) {
		return;
	}
	auto result = con.Query(sql);
	CheckResult(*result, label);
	if (print_result) {
		result->Print();
	}
}

void BindConfigEnvVariables(duckdb::Connection &con, const ServerConfig &config, const EnvSource &env) {
	// getenv() is a CLI-only DuckDB function and is not registered in the embedded library the
	// daemon links, so the generated SQL reads these through getvariable() instead. Recording
	// only the variable NAME in the config keeps secret values out of the generated SQL text
	// (which `validate` prints and the engine can echo back in error messages).
	for (const auto &name : config.env_variables) {
		con.context->config.SetUserVariable("env_" + name, duckdb::Value(env.Get(name.c_str())));
	}
}

string CliErrorMessage(const std::exception &ex) {
	auto message = duckdb::ErrorData(ex).RawMessage();
	auto echo = message.find("\nLINE ");
	if (echo != string::npos) {
		message = message.substr(0, echo);
	}
	duckdb::StringUtil::RTrim(message);
	return message;
}

string ReadSqlFile(const string &path, const string &what) {
	// Rejecting a directory explicitly: an ifstream opened on one succeeds on some platforms
	// and then reads nothing, which would silently turn a mistyped path into "no SQL to run".
	std::error_code ec;
	if (std::filesystem::is_directory(path, ec)) {
		throw InvalidInputException("%s \"%s\" is a directory, not a SQL file", what, path);
	}
	std::ifstream file(path);
	if (!file) {
		throw InvalidInputException("Could not open %s \"%s\"", what, path);
	}
	std::ostringstream buffer;
	buffer << file.rdbuf();
	// An ifstream that opened but failed mid-read (an unreadable mount, an I/O error) would
	// otherwise hand back a silently truncated script.
	if (file.bad()) {
		throw InvalidInputException("Failed to read %s \"%s\"", what, path);
	}
	auto sql = buffer.str();
	duckdb::StringUtil::Trim(sql);
	return sql;
}

bool PathExists(const string &path) {
	std::error_code ec;
	return std::filesystem::exists(path, ec);
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

} // namespace duckdb_otlp_server
