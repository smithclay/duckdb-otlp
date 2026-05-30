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

	OtlpServer &CreateServer(ClientContext &context, const OtlpUri &listen_uri, const OtlpServerConfig &config);
	bool StopServer(ClientContext &context, const OtlpUri &listen_uri);

	struct ServerSnapshot {
		string listen_uri;
		string listen_url;
		string host;
		uint16_t port;
		string catalog_name;
		string schema_name;
		idx_t active_requests;
		idx_t total_requests;
		idx_t total_rows;
		bool is_listening;
		string last_error;
		idx_t buffered_rows;
		idx_t buffered_bytes;
		int64_t last_seal_age_ms;
		idx_t seals_total;
		string seal_last_error;
	};

	vector<ServerSnapshot> ListServers();

	struct FlushResult {
		bool found = false;
		idx_t sealed_rows = 0;
		idx_t seals_total = 0;
		string error;
	};
	//! Force a synchronous seal of a registered server's buffer. Holds servers_mutex
	//! for the duration so the server cannot be destroyed mid-flush.
	FlushResult FlushServer(const OtlpUri &listen_uri, bool run_checkpoint);

private:
	void StopAllServers();

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<OtlpServer>> servers;
};

} // namespace duckdb
