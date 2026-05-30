#define DUCKDB_EXTENSION_MAIN

#include "storage/otlp_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "otlp_start_stop.hpp"
#include "otlp_storage.hpp"

// Rust backend provides all OTLP functionality
namespace duckdb {
void RegisterReadOTLPRustFunctions(ExtensionLoader &loader);
}

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register Rust-backed table functions
	RegisterReadOTLPRustFunctions(loader);
	loader.RegisterFunction(OtlpServeFunction::GetFunction());
	loader.RegisterFunction(OtlpStopFunction::GetFunction());
	loader.RegisterFunction(OtlpServerListFunction::GetFunction());

	auto ext = duckdb::make_shared_ptr<OtlpStorageExtension>();
	ext->storage_info = duckdb::make_uniq<OtlpStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, OtlpStorageExtensionInfo::STORAGE_EXTENSION_KEY,
	                           ext);
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
