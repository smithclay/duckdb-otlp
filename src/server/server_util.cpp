#include "server_util.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

#include <cctype>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

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
	// An ifstream on a directory can succeed and read nothing, silently running no SQL.
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
	// Otherwise a failed mid-read would hand back a silently truncated script.
	if (file.bad()) {
		throw InvalidInputException("Failed to read %s \"%s\"", what, path);
	}
	auto sql = buffer.str();
	duckdb::StringUtil::Trim(sql);
	return sql;
}

bool IsRemotePath(const string &path) {
	// Any scheme, not a list of known ones: no URI has local directories to create.
	auto sep = path.find("://");
	if (sep == string::npos || sep == 0) {
		return false;
	}
	if (std::isalpha(static_cast<unsigned char>(path[0])) == 0) {
		return false;
	}
	for (duckdb::idx_t i = 1; i < sep; i++) {
		auto c = static_cast<unsigned char>(path[i]);
		if (std::isalnum(c) == 0 && c != '+' && c != '-' && c != '.') {
			return false;
		}
	}
	return true;
}

void CreateDirectory(const string &path) {
	if (path.empty() || IsRemotePath(path)) {
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
	if (IsRemotePath(path)) {
		return;
	}
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

namespace {

constexpr int PROBE_TIMEOUT_MS = 2000;

bool WaitFor(int fd, decltype(pollfd::events) events) {
	struct pollfd pfd {};
	pfd.fd = fd;
	pfd.events = events;
	return ::poll(&pfd, 1, PROBE_TIMEOUT_MS) > 0 && (pfd.revents & events) != 0;
}

bool Connect(int fd, const struct addrinfo &ai) {
	::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
	if (::connect(fd, ai.ai_addr, ai.ai_addrlen) == 0) {
		return true;
	}
	int err = 0;
	socklen_t len = sizeof(err);
	return errno == EINPROGRESS && WaitFor(fd, POLLOUT) && ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 &&
	       err == 0;
}

//! Send GET `path` on a connected socket and read the status line ("HTTP/1.1 200 OK").
bool HttpStatusOk(int fd, const duckdb::string &path) {
	auto request = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
	if (!WaitFor(fd, POLLOUT) || ::send(fd, request.data(), request.size(), 0) != (ssize_t)request.size()) {
		return false;
	}
	duckdb::string response;
	char buffer[256];
	while (response.size() < 12 && WaitFor(fd, POLLIN)) {
		auto n = ::recv(fd, buffer, sizeof(buffer), 0);
		if (n <= 0) {
			break;
		}
		response.append(buffer, static_cast<size_t>(n));
	}
	return response.size() >= 12 && response.compare(0, 5, "HTTP/") == 0 && (response[9] == '2' || response[9] == '3');
}

} // namespace

bool ProbeListener(const duckdb::string &host, int port, const duckdb::string &path) {
	struct addrinfo hints {};
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	struct addrinfo *res = nullptr;
	if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) {
		return false;
	}
	bool ok = false;
	for (auto *ai = res; ai && !ok; ai = ai->ai_next) {
		int fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
		if (fd < 0) {
			continue;
		}
		ok = Connect(fd, *ai) && (path.empty() || HttpStatusOk(fd, path));
		::close(fd);
	}
	::freeaddrinfo(res);
	return ok;
}

} // namespace duckdb_otlp_server
