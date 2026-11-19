#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "otlp_server.hpp"

namespace duckdb {

//! Not a storage/ATTACH backend: only the DuckDB hook for hanging per-DatabaseInstance state
//! (the OtlpServer registry) off DatabaseInstance.config, as duckdb-quack does.
class OtlpStorageExtension : public StorageExtension {
public:
	OtlpStorageExtension();
};

class OtlpStorageExtensionInfo : public StorageExtensionInfo {
public:
	static constexpr const char *STORAGE_EXTENSION_KEY = "otlp_server";
	~OtlpStorageExtensionInfo() override;

	static OtlpStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	//! Start one server fed by every listener in `listeners`. Rejects a listen URI that is
	//! already registered, and a second server for an ingest target (catalog + schema, or
	//! Parquet export root) that already has one: a target has exactly one writer.
	OtlpServer &CreateServer(ClientContext &context, const vector<OtlpListenerSpec> &listeners,
	                         const OtlpServerConfig &config);

	struct StopResult {
		bool found = false;
		//! Every listener the stop closed (the whole server, whichever URI named it).
		vector<string> listen_uris;
		//! Rows dropped because the final drain failed; 0 on a clean stop.
		idx_t dropped_rows = 0;
		//! Set only by StopAllServersGraceful, which keeps going after a failed server so the rest still seal.
		string error;
	};
	StopResult StopServer(ClientContext &context, const OtlpUri &listen_uri);

	//! Stop and seal every registered server. Must run while the DatabaseInstance is alive: at
	//! ~OtlpStorageExtensionInfo the db_ptr has expired, the final seal is a no-op and rows are dropped.
	//! Does not mark the registry shut down, so a later otlp_serve still works.
	vector<StopResult> StopAllServersGraceful();

	//! One otlp_server_list() row per listener, in OtlpServer::ListColumns() order, sorted by URI.
	vector<vector<Value>> ListServers();

	//! Seal events are server-wide; listen_uri is the server's first (primary) listener.
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
	//! Synchronous seal of the server owning `listen_uri`; the registry lock is not held during the seal.
	FlushResult FlushServer(const OtlpUri &listen_uri);

private:
	void StopAllServers();
	//! Distinct servers in the registry (several URIs can map to one). Caller holds servers_mutex.
	vector<shared_ptr<OtlpServer>> DistinctServers() const;
	//! Throws once teardown has begun, so a registry call racing a DB close fails cleanly. Caller holds servers_mutex.
	void EnsureNotShutDown() const;

private:
	std::mutex servers_mutex;
	//! Set by StopAllServers (destructor teardown); guarded by servers_mutex.
	bool shutting_down = false;
	//! Canonical listen URI -> owning server; every listener of a server has an entry. shared_ptr so
	//! FlushServer can keep a server alive across a slow seal with servers_mutex released.
	unordered_map<string, shared_ptr<OtlpServer>> servers;
};

} // namespace duckdb
