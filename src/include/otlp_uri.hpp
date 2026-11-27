#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! `host` as it must appear in a URI or host:port string: an IPv6 literal bracketed. Use it for
//! every such string, or "::1" + port becomes unparseable.
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

	const string &Http() const {
		return http;
	}
	//! Canonical form: `otlp:` + HostPort(). Registry lookups compare it.
	const string &Uri() const {
		return uri;
	}
	//! The bind address: `host:port`, an IPv6 host bracketed.
	string HostPort() const {
		return UriHost(host) + ":" + std::to_string(port);
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
	//! Loopback only (all of 127.0.0.0/8). Shared by the allow_other_hostname gate and the daemon's
	//! no-auth-on-loopback rule, which must agree.
	bool IsLocal() const {
		auto lower = StringUtil::Lower(host);
		return lower == "localhost" || lower == "::1" || StringUtil::StartsWith(host, "127.");
	}

private:
	bool ipv6 = false;
	string host;
	uint16_t port = 4318;
	string http;
	string uri;
};

void RegisterOtlpUriFunction(ExtensionLoader &loader);

} // namespace duckdb
