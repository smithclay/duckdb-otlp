#include "otlp_registry.hpp"

#include "duckdb/main/database.hpp"

#include <utility>

namespace duckdb {

OtlpServerRegistry::~OtlpServerRegistry() {
	for (auto &server : RemoveAll()) {
		server->Close();
	}
}

OtlpServerRegistry &OtlpServerRegistry::Get(const DatabaseInstance &instance) {
	auto ext = StorageExtension::Find(instance.config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw InternalException("Fatal error: couldn't find OTLP server extension state");
	}
	return *static_cast<OtlpServerRegistry *>(ext->storage_info.get());
}

//! Identity of what a server writes to; one server per target keeps a single sealer per catalog.
static string IngestTargetKey(const OtlpServerConfig &config) {
	if (!config.parquet_export_path.empty()) {
		return "parquet export path " + config.parquet_export_path;
	}
	return "catalog '" + config.catalog_name + "' schema '" + config.schema_name + "'";
}

shared_ptr<OtlpServer> OtlpServerRegistry::Start(ClientContext &context, const vector<OtlpListenerSpec> &listeners,
                                                 const OtlpServerConfig &config) {
	std::lock_guard<std::mutex> start_lock(start_mutex);
	auto target = IngestTargetKey(config);
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		for (auto &spec : listeners) {
			if (FindServer(spec.uri.Uri()) != servers.end()) {
				throw InvalidInputException("OTLP server already exists for %s", spec.uri.Uri());
			}
		}
		for (auto &existing : servers) {
			if (IngestTargetKey(existing->Config()) == target) {
				throw InvalidInputException("OTLP server already exists for %s (listening on %s); pass every listen "
				                            "URI to one otlp_serve call instead of starting a second server",
				                            target, existing->PrimaryUri().Uri());
			}
		}
	}
	auto server = make_shared_ptr<OtlpServer>(context, listeners, config);
	std::lock_guard<std::mutex> lock(servers_mutex);
	servers.push_back(server);
	return server;
}

vector<shared_ptr<OtlpServer>>::iterator OtlpServerRegistry::FindServer(const string &uri) {
	for (auto entry = servers.begin(); entry != servers.end(); ++entry) {
		for (auto &listener : (*entry)->Listeners()) {
			if (listener->Uri().Uri() == uri) {
				return entry;
			}
		}
	}
	return servers.end();
}

vector<shared_ptr<OtlpServer>>::iterator OtlpServerRegistry::FindServerOrThrow(const OtlpUri &uri) {
	auto entry = FindServer(uri.Uri());
	if (entry == servers.end()) {
		throw InvalidInputException("No server found listening on %s", uri.Uri());
	}
	return entry;
}

shared_ptr<OtlpServer> OtlpServerRegistry::Find(const OtlpUri &uri) {
	std::lock_guard<std::mutex> lock(servers_mutex);
	return *FindServerOrThrow(uri);
}

shared_ptr<OtlpServer> OtlpServerRegistry::Remove(const OtlpUri &uri) {
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto entry = FindServerOrThrow(uri);
	auto server = std::move(*entry);
	servers.erase(entry);
	return server;
}

vector<shared_ptr<OtlpServer>> OtlpServerRegistry::RemoveAll() {
	std::lock_guard<std::mutex> lock(servers_mutex);
	return std::exchange(servers, {});
}

vector<shared_ptr<OtlpServer>> OtlpServerRegistry::All() {
	std::lock_guard<std::mutex> lock(servers_mutex);
	return servers;
}

} // namespace duckdb
