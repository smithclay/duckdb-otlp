#define DUCKDB_EXTENSION_MAIN

#include "storage/otlp_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

// OTLP functionality
#include "function/read_otlp.hpp"

// Rust backend (when enabled)
#ifdef OTLP_RUST_BACKEND
namespace duckdb {
void RegisterReadOTLPRustFunctions(ExtensionLoader &loader);
}
#endif

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
#ifdef OTLP_RUST_BACKEND
	// Register Rust-backed table functions (replaces C++ implementations)
	RegisterReadOTLPRustFunctions(loader);

	// Keep utility functions from C++ (stats and options)
	loader.RegisterFunction(ReadOTLPTableFunction::GetStatsFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetOptionsFunction());
#else
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
#endif
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
	return "dev";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(otlp, loader) {
	duckdb::LoadInternal(loader);
}
}
