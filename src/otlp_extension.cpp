#define DUCKDB_EXTENSION_MAIN

#include "otlp_extension.hpp"

#include "otlp_control.hpp"
#include "otlp_read.hpp"
#include "otlp_uri.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	RegisterOtlpReadFunctions(loader);
	RegisterOtlpUriFunction(loader);
#ifndef __EMSCRIPTEN__
	RegisterOtlpControlFunctions(loader);
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
