#include "otlp_storage_extension.hpp"

namespace duckdb {

#ifdef DUCKSPAN_DISABLE_GRPC

// WASM stub implementation: Returns nullptr since ATTACH is not supported in WASM
// The gRPC receiver and network functionality cannot work in browser environments
unique_ptr<StorageExtension> OTLPStorageExtension::Create() {
	// Return nullptr to indicate storage extension is not available
	// This prevents ATTACH 'otlp:host:port' from working in WASM builds
	return nullptr;
}

#endif // DUCKSPAN_DISABLE_GRPC

} // namespace duckdb
