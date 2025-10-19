#define DUCKDB_EXTENSION_MAIN

#include "storage/duckspan_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OTLP functionality
#include "function/read_otlp.hpp"
#include "function/otlp_metrics_union.hpp"
#ifndef DUCKSPAN_DISABLE_GRPC
#include "storage/otlp_storage_extension.hpp"
// OpenSSL linked through vcpkg (only for gRPC builds)
#include <openssl/opensslv.h>
#endif

namespace duckdb {

inline void DuckspanScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Duckspan " + name.GetString() + " 🐥");
	});
}

#ifndef DUCKSPAN_DISABLE_GRPC
inline void DuckspanOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Duckspan " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}
#endif

static void LoadInternal(ExtensionLoader &loader) {
#ifndef DUCKSPAN_DISABLE_GRPC
	// Register OTLP storage extension for ATTACH support (not available in WASM)
	auto &db_instance = loader.GetDatabaseInstance();
	auto &db_config = DBConfig::GetConfig(db_instance);
	db_config.storage_extensions["otlp"] = OTLPStorageExtension::Create();
#endif

	// Register OTLP table function (available in all builds including WASM)
	// Register read_otlp_* table functions
	loader.RegisterFunction(ReadOTLPTableFunction::GetTracesFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetLogsFunction());
	loader.RegisterFunction(ReadOTLPTableFunction::GetMetricsFunction());
#ifndef DUCKSPAN_DISABLE_GRPC
	loader.RegisterFunction(GetOTLPMetricsUnionFunction());
#endif

	// Register a scalar function (from template, keeping for now)
	auto duckspan_scalar_function =
	    ScalarFunction("duckspan", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckspanScalarFun);
	loader.RegisterFunction(duckspan_scalar_function);

#ifndef DUCKSPAN_DISABLE_GRPC
	// Register OpenSSL version function (only in non-WASM builds)
	auto duckspan_openssl_version_scalar_function = ScalarFunction(
	    "duckspan_openssl_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckspanOpenSSLVersionScalarFun);
	loader.RegisterFunction(duckspan_openssl_version_scalar_function);
#endif
}

void DuckspanExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DuckspanExtension::Name() {
	return "duckspan";
}

std::string DuckspanExtension::Version() const {
#ifdef EXT_VERSION_DUCKSPAN
	return EXT_VERSION_DUCKSPAN;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckspan, loader) {
	duckdb::LoadInternal(loader);
}
}
