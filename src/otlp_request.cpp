#include "otlp_request.hpp"

namespace duckdb {

const char *OtlpRequestKindName(OtlpRequestKind kind) {
	switch (kind) {
	case OtlpRequestKind::LOGS:
		return "logs";
	case OtlpRequestKind::TRACES:
		return "traces";
	case OtlpRequestKind::METRICS:
		return "metrics";
	default:
		throw InternalException("Unknown OTLP request kind");
	}
}

bool OtlpRequestKindFromName(const string &name, OtlpRequestKind &kind) {
	if (name == "logs") {
		kind = OtlpRequestKind::LOGS;
		return true;
	}
	if (name == "traces") {
		kind = OtlpRequestKind::TRACES;
		return true;
	}
	if (name == "metrics") {
		kind = OtlpRequestKind::METRICS;
		return true;
	}
	return false;
}

} // namespace duckdb
