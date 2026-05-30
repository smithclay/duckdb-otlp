#include "otlp_uri.hpp"

namespace duckdb {

OtlpUri::OtlpUri(string uri_p) : uri(std::move(uri_p)) {
	StringUtil::Trim(uri);
	string remainder;
	if (StringUtil::StartsWith(uri, "otlp://")) {
		remainder = uri.substr(strlen("otlp://"));
	} else if (StringUtil::StartsWith(uri, "otlp:")) {
		remainder = uri.substr(strlen("otlp:"));
	} else {
		throw InvalidInputException("Invalid OTLP listen URI, needs to start with 'otlp:'");
	}
	if (remainder.empty()) {
		remainder = "localhost";
	}

	if (StringUtil::StartsWith(remainder, "[")) {
		if (!StringUtil::Contains(remainder, ']')) {
			throw InvalidInputException("Invalid IPv6 OTLP URI, missing ']'");
		}
		ipv6 = true;
		auto pos = remainder.find(']');
		host = remainder.substr(1, pos - 1);
		if (host.empty()) {
			throw InvalidInputException("Missing IPv6 address");
		}
		remainder = remainder.substr(pos + 1);
		if (StringUtil::StartsWith(remainder, ":")) {
			remainder = remainder.substr(1);
		}
		if (!remainder.empty()) {
			int raw_port;
			try {
				raw_port = stoi(remainder);
			} catch (std::exception &) {
				throw InvalidInputException("Invalid OTLP listen port");
			}
			if (raw_port < 1 || raw_port > 65535) {
				throw InvalidInputException("Invalid OTLP listen port");
			}
			port = raw_port;
		}
	} else {
		if (StringUtil::Contains(remainder, ':')) {
			auto pos = remainder.find(':');
			auto port_str = remainder.substr(pos + 1);
			if (port_str.empty()) {
				throw InvalidInputException("Invalid OTLP listen port");
			}
			int raw_port;
			try {
				raw_port = stoi(port_str);
			} catch (std::exception &) {
				throw InvalidInputException("Invalid OTLP listen port");
			}
			if (raw_port < 1 || raw_port > 65535) {
				throw InvalidInputException("Invalid OTLP listen port");
			}
			port = raw_port;
			remainder = remainder.substr(0, pos);
		}
		host = remainder;
		if (host.empty()) {
			throw InvalidInputException("Missing OTLP listen hostname");
		}
	}

	http = StringUtil::Format("http://%s:%d", ipv6 ? "[" + host + "]" : host, port);
	uri = CanonicalUri();
}

} // namespace duckdb
