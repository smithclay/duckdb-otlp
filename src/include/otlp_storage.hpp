#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "otlp_server.hpp"

namespace duckdb {

//! NOT a real storage/ATTACH backend: this StorageExtension implements no attach,
//! transaction, or catalog semantics. It is only the standard DuckDB hook for hanging
//! per-DatabaseInstance global state (the OtlpServer registry in OtlpStorageExtensionInfo)
//! off DatabaseInstance.config, keyed by STORAGE_EXTENSION_KEY. Mirrors duckdb-quack; do
//! not repurpose as an actual storage backend without sign-off.
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
		idx_t admitted_bytes;
		idx_t seal_target_bytes;
		int64_t seal_max_age_ms;
		int64_t oldest_buffered_age_ms;
		int64_t last_seal_age_ms;
		idx_t seals_total;
		idx_t committed_rows_total;
		idx_t seal_failures_total;
		string seal_last_error;
		idx_t maintenance_runs_total;
		idx_t maintenance_failures_total;
		int64_t last_maintenance_age_ms;
		string maintenance_last_error;
	};

	vector<ServerSnapshot> ListServers();

	struct SealSnapshot {
		string listen_uri;
		OtlpSealEvent event;
	};

	vector<SealSnapshot> ListSeals();

	struct FlushResult {
		bool found = false;
		idx_t sealed_rows = 0;
		idx_t seals_total = 0;
		string error;
	};
	//! Force a synchronous seal of a registered server's buffer. Takes a shared_ptr
	//! under servers_mutex, then releases the registry lock while the seal runs.
	FlushResult FlushServer(const OtlpUri &listen_uri);

private:
	void StopAllServers();

private:
	std::mutex servers_mutex;
	//! shared_ptr (not unique_ptr) so FlushServer can hold a ref across a slow seal
	//! with servers_mutex released; the server can't be freed mid-flush, and a
	//! concurrent otlp_stop/db-close isn't blocked by the seal.
	unordered_map<string, shared_ptr<OtlpServer>> servers;
};

} // namespace duckdb
