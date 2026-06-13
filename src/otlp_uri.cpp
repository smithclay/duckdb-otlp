#include "otlp_uri.hpp"

#include <charconv>

namespace duckdb {

namespace {

static void ValidateHost(const string &host, bool is_ipv6) {
	if (is_ipv6) {
		// IPv6 literal content (between the brackets): hex digits, colons, dots (for
		// v4-mapped addresses like ::ffff:192.0.2.1), and '%' for zone IDs.
		for (char c : host) {
			if (!isxdigit((unsigned char)c) && c != ':' && c != '.' && c != '%') {
				throw InvalidInputException("Invalid character in IPv6 address");
			}
		}
	} else {
		// Regular hostname: alphanumerics, hyphens, dots.
		for (char c : host) {
			if (!isalnum((unsigned char)c) && c != '-' && c != '.') {
				throw InvalidInputException("Invalid character in OTLP hostname");
			}
		}
	}
}

static uint16_t ParsePort(const string &port_str) {
	if (port_str.empty()) {
		throw InvalidInputException("Invalid OTLP listen port");
	}
	int raw_port = 0;
	auto begin = port_str.data();
	auto end = begin + port_str.size();
	auto result = std::from_chars(begin, end, raw_port);
	if (result.ec != std::errc() || result.ptr != end || raw_port < 1 || raw_port > 65535) {
		throw InvalidInputException("Invalid OTLP listen port");
	}
	return static_cast<uint16_t>(raw_port);
}

} // namespace

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
		ValidateHost(host, true);
		remainder = remainder.substr(pos + 1);
		if (StringUtil::StartsWith(remainder, ":")) {
			remainder = remainder.substr(1);
		}
		if (!remainder.empty()) {
			port = ParsePort(remainder);
		}
	} else {
		if (StringUtil::Contains(remainder, ':')) {
			auto pos = remainder.find(':');
			auto port_str = remainder.substr(pos + 1);
			port = ParsePort(port_str);
			remainder = remainder.substr(0, pos);
		}
		host = remainder;
		if (host.empty()) {
			throw InvalidInputException("Missing OTLP listen hostname");
		}
		ValidateHost(host, false);
	}

	http = StringUtil::Format("http://%s:%d", ipv6 ? "[" + host + "]" : host, port);
	uri = CanonicalUri();
}

static void OtlpUriParser(const DataChunk &args, ExpressionState &, Vector &result) {
	if (!args.AllConstant()) {
		throw InvalidInputException("otlp_uri_parser expects all arguments to be constant");
	}
	OtlpUri parsed(args.GetValue(0, 0).GetValue<string>());
	result.SetValue(0, Value::STRUCT({{"host", Value(parsed.Host())},
	                                  {"port", Value::USMALLINT(parsed.Port())},
	                                  {"ipv6", Value::BOOLEAN(parsed.IPv6())},
	                                  {"url", Value(parsed.Http())}}));
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

ScalarFunction OtlpUriParserFunction::GetFunction() {
	// Materialize the LogicalType constants into locals (read by value) before they
	// reach perfect-forwarding APIs (pair/initializer_list). Binding a reference to
	// the static constexpr LogicalType::VARCHAR/USMALLINT/BOOLEAN members ODR-uses
	// them and forces a duplicate symbol definition, which the GNU linker rejects as
	// a multiple definition of e.g. duckdb::LogicalType::VARCHAR.
	LogicalType varchar_type(LogicalType::VARCHAR);
	LogicalType usmallint_type(LogicalType::USMALLINT);
	LogicalType boolean_type(LogicalType::BOOLEAN);

	child_list_t<LogicalType> struct_children;
	struct_children.emplace_back("host", varchar_type);
	struct_children.emplace_back("port", usmallint_type);
	struct_children.emplace_back("ipv6", boolean_type);
	struct_children.emplace_back("url", varchar_type);

	return ScalarFunction("otlp_uri_parser", {varchar_type}, LogicalType::STRUCT(std::move(struct_children)),
	                      OtlpUriParser);
}

} // namespace duckdb
