#include "otlp_uri.hpp"

#include <charconv>

namespace duckdb {

namespace {

static void ValidateHost(const string &host, bool is_ipv6) {
	if (is_ipv6) {
		// Dots for v4-mapped addresses, '%' for zone IDs.
		for (char c : host) {
			if (!isxdigit((unsigned char)c) && c != ':' && c != '.' && c != '%') {
				throw InvalidInputException("Invalid character in IPv6 address");
			}
		}
	} else {
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

CreateScalarFunctionInfo OtlpUriParserFunction::GetFunction() {
	// Copies, not references: binding a reference to the static constexpr LogicalType members
	// ODR-uses them and the GNU linker then reports a multiple definition.
	LogicalType varchar_type(LogicalType::VARCHAR);
	LogicalType usmallint_type(LogicalType::USMALLINT);
	LogicalType boolean_type(LogicalType::BOOLEAN);

	child_list_t<LogicalType> struct_children;
	struct_children.emplace_back("host", varchar_type);
	struct_children.emplace_back("port", usmallint_type);
	struct_children.emplace_back("ipv6", boolean_type);
	struct_children.emplace_back("url", varchar_type);

	// duckdb_functions() takes parameter names from the signature (a bare LogicalType would be col0).
	auto function = ScalarFunction("otlp_uri_parser", {FunctionParameter(Identifier("listen_uri"), varchar_type)},
	                               LogicalType::STRUCT(std::move(struct_children)), OtlpUriParser);
	function.SetFallible();
	return OtlpDocumented(
	    std::move(function),
	    {OtlpDoc({varchar_type}, {"listen_uri"},
	             "Parse an OTLP/OTAP listen URI into a struct of host, port, ipv6 and the http:// URL a "
	             "listener would bind. Use the otlp: scheme. The host defaults to localhost and the port to 4318. "
	             "The argument must be constant.",
	             {"otlp_uri_parser('otlp:localhost:4318')"}, {"opentelemetry", "utility"})});
}

} // namespace duckdb
