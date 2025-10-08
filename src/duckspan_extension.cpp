#define DUCKDB_EXTENSION_MAIN

#include "duckspan_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void DuckspanScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Duckspan " + name.GetString() + " 🐥");
	});
}

inline void DuckspanOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Duckspan " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto duckspan_scalar_function = ScalarFunction("duckspan", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckspanScalarFun);
	loader.RegisterFunction(duckspan_scalar_function);

	// Register another scalar function
	auto duckspan_openssl_version_scalar_function = ScalarFunction("duckspan_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, DuckspanOpenSSLVersionScalarFun);
	loader.RegisterFunction(duckspan_openssl_version_scalar_function);
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
