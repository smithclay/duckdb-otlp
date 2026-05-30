#include "otlp_storage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

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
                                                   OtlpServerConfig config) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
#else
	auto key = listen_uri.CanonicalUri();
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(key);
	if (it != servers.end()) {
		throw InvalidInputException("OTLP server already exists for %s", key);
	}
	auto server = make_uniq<HttpOtlpServer>(context, listen_uri, std::move(config));
	auto &server_ref = *server;
	servers.emplace(key, std::move(server));
	return server_ref;
#endif
}

bool OtlpStorageExtensionInfo::StopServer(ClientContext &context, const OtlpUri &listen_uri) {
	unique_ptr<OtlpServer> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		const auto it = servers.find(listen_uri.CanonicalUri());
		if (it == servers.end()) {
			return false;
		}
		to_destroy = std::move(it->second);
		servers.erase(it);
	}
	// Synchronously free the listening port so the URI can be reused immediately.
	to_destroy->StopAccepting();
	to_destroy.reset();
	return true;
}

void OtlpStorageExtensionInfo::StopAllServers() {
	vector<unique_ptr<OtlpServer>> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		to_destroy.reserve(servers.size());
		for (auto &kv : servers) {
			to_destroy.push_back(std::move(kv.second));
		}
		servers.clear();
	}
	for (auto &server : to_destroy) {
		server->StopAccepting();
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
		snap.schema_name = server.SchemaName();
		snap.active_requests = server.ActiveRequests();
		snap.total_requests = server.TotalRequests();
		snap.total_rows = server.TotalRows();
		result.push_back(std::move(snap));
	}
	return result;
}

} // namespace duckdb
