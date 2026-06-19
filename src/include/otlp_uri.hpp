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
		// Scheme-aware so otap:host:4317 and otlp:host:4318 are distinct registry
		// keys (and otlp_stop/otlp_flush target the right server).
		return scheme + ":" + (ipv6 ? "[" + host + "]" : host) + ":" + std::to_string(port);
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
	//! Scheme: "otlp" (HTTP transport) or "otap" (gRPC transport).
	string Scheme() const {
		return scheme;
	}
	//! True when this URI selects the gRPC transport (otap: scheme).
	bool IsGrpc() const {
		return scheme == "otap";
	}
	bool IsLocal() const {
		return StringUtil::Lower(host) == "localhost" || host == "127.0.0.1" || host == "::1";
	}

private:
	bool ipv6 = false;
	string scheme = "otlp";
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
