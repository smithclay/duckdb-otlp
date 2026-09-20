#pragma once

#include "duckdb.hpp"
#include "otlp_function_docs.hpp"

namespace duckdb {

//! `host` as it must appear inside a URI or a host:port string: an IPv6 literal bracketed,
//! anything else unchanged. One definition, because two callers build such strings from a
//! bare host — the listener URI and the Quack bind address — and only one of them used to
//! bracket, so `--host ::1 --quack 9494` produced "::1:9494", which nothing can parse back.
inline string UriHost(const string &host) {
	if (host.find(':') == string::npos || (!host.empty() && host[0] == '[')) {
		return host;
	}
	return "[" + host + "]";
}

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
		return scheme + ":" + UriHost(host) + ":" + std::to_string(port);
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
	//! True when this URI can only be reached from this machine. Two decisions depend on it:
	//! the `allow_other_hostname` gate in otlp_serve, and the daemon's rule that an
	//! unauthenticated server is acceptable only on loopback. They must agree about a given
	//! URI, so there is one definition. The whole 127.0.0.0/8 block is loopback per RFC 1122,
	//! not just 127.0.0.1.
	bool IsLocal() const {
		auto lower = StringUtil::Lower(host);
		return lower == "localhost" || lower == "::1" || StringUtil::StartsWith(host, "127.");
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
	static CreateScalarFunctionInfo GetFunction();
};

} // namespace duckdb
