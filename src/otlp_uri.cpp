#include "otlp_uri.hpp"

#include "otlp_function_docs.hpp"

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
	if (!StringUtil::StartsWith(uri, "otlp:")) {
		throw InvalidInputException("Invalid OTLP listen URI, needs to start with 'otlp:'");
	}
	auto remainder = uri.substr(sizeof("otlp:") - 1);
	if (StringUtil::StartsWith(remainder, "//")) {
		remainder.erase(0, 2);
	}
	if (remainder.empty()) {
		remainder = "localhost";
	}

	ipv6 = remainder.front() == '[';
	auto host_end = remainder.find(ipv6 ? ']' : ':');
	if (ipv6) {
		if (host_end == string::npos) {
			throw InvalidInputException("Invalid IPv6 OTLP URI, missing ']'");
		}
		host = remainder.substr(1, host_end - 1);
		if (host.empty()) {
			throw InvalidInputException("Missing IPv6 address");
		}
		ValidateHost(host, true);
	} else {
		host = remainder.substr(0, host_end);
	}
	if (host_end != string::npos) {
		auto port_start = host_end + 1;
		if (ipv6 && port_start < remainder.size() && remainder[port_start] == ':') {
			port_start++;
		}
		if (!ipv6 || port_start < remainder.size()) {
			port = ParsePort(remainder.substr(port_start));
		}
	}
	if (!ipv6) {
		if (host.empty()) {
			throw InvalidInputException("Missing OTLP listen hostname");
		}
		ValidateHost(host, false);
	}

	http = "http://" + HostPort();
	uri = "otlp:" + HostPort();
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

void RegisterOtlpUriFunction(ExtensionLoader &loader) {
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
	loader.RegisterFunction(OtlpDocumented(
	    std::move(function),
	    {OtlpDoc({varchar_type}, {"listen_uri"},
	             "Parse an otlp: listen URI into a struct of host, port, ipv6 and the derived http:// URL. "
	             "The host defaults to localhost and the port to 4318. "
	             "The argument must be constant.",
	             {"otlp_uri_parser('otlp:localhost:4318')"}, {"opentelemetry", "utility"})}));
}

} // namespace duckdb
