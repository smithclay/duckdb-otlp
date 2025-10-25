#define DUCKDB_EXTENSION_MAIN

#include "storage/otlp_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"

// OTLP functionality
#include "function/read_otlp.hpp"

namespace duckdb {

inline void OtlpScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "OTLP " + name.GetString() + " 🐥");
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register OTLP table function (available in all builds including WASM)
	// Register read_otlp_* table functions
	loader.RegisterFunction(ReadOTLPTableFunction::GetTracesFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetLogsFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetMetricsFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetMetricsGaugeFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetMetricsSumFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetMetricsHistogramFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetMetricsExpHistogramFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetMetricsSummaryFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetStatsFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetOptionsFunction());

	// Register a scalar function (from template, keeping for now)
	auto otlp_scalar_function = ScalarFunction("otlp", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OtlpScalarFun);
	loader.RegisterFunction(otlp_scalar_function);
}

void OtlpExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string OtlpExtension::Name() {
	return "otlp";
}

std::string OtlpExtension::Version() const {
#ifdef EXT_VERSION_OTLP
	return EXT_VERSION_OTLP;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(otlp, loader) {
	duckdb::LoadInternal(loader);
}
}
