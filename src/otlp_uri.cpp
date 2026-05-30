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
