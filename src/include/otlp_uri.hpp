#pragma once

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

class OtlpUri {
public:
	OtlpUri() : OtlpUri("otlp:localhost:4318") {
	}
	explicit OtlpUri(string uri_p);

	string Http() const {
		return http;
	}
	string Uri() const {
		return uri;
	}
	string CanonicalUri() const {
		return "otlp:" + (ipv6 ? "[" + host + "]" : host) + ":" + std::to_string(port);
	}
	string Host() const {
		return host;
	}
	uint16_t Port() const {
		return port;
	}
	bool IPv6() const {
		return ipv6;
	}
	bool IsLocal() const {
		return StringUtil::Lower(host) == "localhost" || host == "127.0.0.1" || host == "::1";
	}

private:
	bool ipv6 = false;
	string host;
	uint16_t port = 4318;
	string http;
	string uri;
};

//! Exposes OtlpUri parsing as a scalar function so the parser can be unit-tested from SQL.
class OtlpUriParserFunction {
public:
	static ScalarFunction GetFunction();
};

} // namespace duckdb
