#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "otlp_server.hpp"

namespace duckdb {

//! Per-DatabaseInstance server registry owned by the StorageExtension hook.
class OtlpServerRegistry : public StorageExtensionInfo {
public:
	static constexpr const char *STORAGE_EXTENSION_KEY = "otlp_server";
	~OtlpServerRegistry() override;

	static OtlpServerRegistry &Get(const DatabaseInstance &instance);

	//! Start one server for the listeners. Each URI and ingest target can have only one server.
	shared_ptr<OtlpServer> Start(ClientContext &context, const vector<OtlpListenerSpec> &listeners,
	                             const OtlpServerConfig &config);

	//! Any listener URI identifies its server. Find and Remove throw if it is not registered.
	shared_ptr<OtlpServer> Find(const OtlpUri &uri);
	shared_ptr<OtlpServer> Remove(const OtlpUri &uri);
	vector<shared_ptr<OtlpServer>> RemoveAll();
	vector<shared_ptr<OtlpServer>> All();

private:
	//! Caller holds servers_mutex.
	vector<shared_ptr<OtlpServer>>::iterator FindServer(const string &canonical_uri);

	//! Serializes starts across the conflict checks and listener bind.
	std::mutex start_mutex;
	//! Protects the server list only. Construction and seals can block on a remote catalog.
	std::mutex servers_mutex;
	vector<shared_ptr<OtlpServer>> servers;
};

} // namespace duckdb
