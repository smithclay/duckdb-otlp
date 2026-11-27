#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterOtlpReadFunctions(ExtensionLoader &loader);

} // namespace duckdb
