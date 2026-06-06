#include "otlp_storage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>

namespace duckdb {

OtlpStorageExtension::OtlpStorageExtension() {
}

OtlpStorageExtensionInfo::~OtlpStorageExtensionInfo() {
	StopAllServers();
}

OtlpStorageExtensionInfo &OtlpStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
	auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw InternalException("Fatal error: couldn't find OTLP server extension state");
	}
	return *static_cast<OtlpStorageExtensionInfo *>(ext->storage_info.get());
}

OtlpServer &OtlpStorageExtensionInfo::CreateServer(ClientContext &context, const OtlpUri &listen_uri,
                                                   const OtlpServerConfig &config) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
#else
	auto key = listen_uri.CanonicalUri();
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(key);
	if (it != servers.end()) {
		throw InvalidInputException("OTLP server already exists for %s", key);
	}
	auto server = make_shared_ptr<OtlpServer>(context, listen_uri, config);
	auto &server_ref = *server;
	servers.emplace(key, std::move(server));
	return server_ref;
#endif
}

bool OtlpStorageExtensionInfo::StopServer(ClientContext &context, const OtlpUri &listen_uri) {
	shared_ptr<OtlpServer> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		const auto it = servers.find(listen_uri.CanonicalUri());
		if (it == servers.end()) {
			return false;
		}
		to_destroy = std::move(it->second);
		servers.erase(it);
	}
	// Synchronously stop listeners/workers and drain the final seal before returning.
	// A concurrent otlp_flush may still hold another shared_ptr; Close() is idempotent
	// and serializes with it through the server's writer mutex.
	to_destroy->Close();
	to_destroy.reset();
	return true;
}

OtlpStorageExtensionInfo::FlushResult OtlpStorageExtensionInfo::FlushServer(const OtlpUri &listen_uri) {
	// Take a shared_ptr ref under the lock, then release the lock before the seal. The
	// ref keeps the server alive even if a concurrent otlp_stop erases it, and dropping
	// the lock means a long seal doesn't block otlp_serve/otlp_stop/otlp_server_list.
	shared_ptr<OtlpServer> server;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		auto it = servers.find(listen_uri.CanonicalUri());
		if (it != servers.end()) {
			server = it->second;
		}
	}
	FlushResult result;
	if (!server) {
		return result;
	}
	result.found = true;
	try {
		result.sealed_rows = server->FlushNow().rows;
	} catch (std::exception &ex) {
		result.error = ex.what();
	}
	result.seals_total = server->SealsTotal();
	return result;
}

void OtlpStorageExtensionInfo::StopAllServers() {
	vector<shared_ptr<OtlpServer>> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		to_destroy.reserve(servers.size());
		for (auto &kv : servers) {
			to_destroy.push_back(std::move(kv.second));
		}
		servers.clear();
	}
	for (auto &server : to_destroy) {
		server->Close();
	}
	to_destroy.clear();
}

vector<OtlpStorageExtensionInfo::ServerSnapshot> OtlpStorageExtensionInfo::ListServers() {
	vector<ServerSnapshot> result;
	std::lock_guard<std::mutex> lock(servers_mutex);
	result.reserve(servers.size());
	for (auto &kv : servers) {
		auto &server = *kv.second;
		auto &uri = server.ListenUri();
		ServerSnapshot snap;
		snap.listen_uri = uri.Uri();
		snap.listen_url = uri.Http();
		snap.host = uri.Host();
		snap.port = uri.Port();
		snap.catalog_name = server.CatalogName();
		snap.schema_name = server.SchemaName();
		snap.active_requests = server.ActiveRequests();
		snap.total_requests = server.TotalRequests();
		snap.total_rows = server.TotalRows();
		snap.is_listening = server.IsListening();
		snap.last_error = server.LastError();
		snap.buffered_rows = server.BufferedRows();
		snap.admitted_bytes = server.AdmittedBytes();
		snap.seal_target_bytes = server.SealTargetBytes();
		snap.seal_max_age_ms = server.SealMaxAgeMs();
		snap.oldest_buffered_age_ms = server.OldestBufferedAgeMs();
		snap.last_seal_age_ms = server.LastSealAgeMs();
		snap.seals_total = server.SealsTotal();
		snap.committed_rows_total = server.CommittedRowsTotal();
		snap.seal_failures_total = server.SealFailuresTotal();
		snap.seal_last_error = server.SealLastError();
		result.push_back(std::move(snap));
	}
	// servers is an unordered_map; sort for deterministic output order.
	std::sort(result.begin(), result.end(),
	          [](const ServerSnapshot &a, const ServerSnapshot &b) { return a.listen_uri < b.listen_uri; });
	return result;
}

vector<OtlpStorageExtensionInfo::SealSnapshot> OtlpStorageExtensionInfo::ListSeals() {
	vector<SealSnapshot> result;
	std::lock_guard<std::mutex> lock(servers_mutex);
	for (auto &kv : servers) {
		auto events = kv.second->SealHistory();
		for (auto &event : events) {
			result.push_back({kv.first, std::move(event)});
		}
	}
	std::sort(result.begin(), result.end(), [](const SealSnapshot &a, const SealSnapshot &b) {
		if (a.listen_uri != b.listen_uri) {
			return a.listen_uri < b.listen_uri;
		}
		return a.event.seal_sequence < b.event.seal_sequence;
	});
	return result;
}

} // namespace duckdb
