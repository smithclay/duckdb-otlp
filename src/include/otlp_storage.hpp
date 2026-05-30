#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "otlp_server.hpp"

namespace duckdb {

class OtlpStorageExtension : public StorageExtension {
public:
	OtlpStorageExtension();
};

class OtlpStorageExtensionInfo : public StorageExtensionInfo {
public:
	static constexpr const char *STORAGE_EXTENSION_KEY = "otlp_server";
	~OtlpStorageExtensionInfo() override;

	static OtlpStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	OtlpServer &CreateServer(ClientContext &context, const OtlpUri &listen_uri, OtlpServerConfig config);
	bool StopServer(ClientContext &context, const OtlpUri &listen_uri);

	struct ServerSnapshot {
		string listen_uri;
		string listen_url;
		string host;
		uint16_t port;
		string schema_name;
		idx_t active_requests;
		idx_t total_requests;
		idx_t total_rows;
		bool is_listening;
		string last_error;
	};

	vector<ServerSnapshot> ListServers();

private:
	void StopAllServers();

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<OtlpServer>> servers;
};

} // namespace duckdb
