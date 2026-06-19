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

void OtlpStorageExtensionInfo::EnsureNotShutDown() const {
	// Caller holds servers_mutex.
	if (shutting_down) {
		throw InvalidInputException("OTLP server state is shutting down");
	}
}

OtlpServer &OtlpStorageExtensionInfo::CreateServer(ClientContext &context, const OtlpUri &listen_uri,
                                                   const OtlpServerConfig &config) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
#else
	auto key = listen_uri.CanonicalUri();
	// Fast pre-check under the lock. The OtlpServer constructor binds the listener and (for a
	// remote catalog) runs EnsureTargetTables, which can block for a long time on a slow
	// DuckLake/S3/Postgres attach. Holding servers_mutex across that head-of-line-blocks every
	// concurrent otlp_stop/otlp_flush/otlp_server_list/StopAllServers, so we only take the lock
	// for the (cheap) duplicate check and the (cheap) insert — never across construction
	// (review finding M1).
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		EnsureNotShutDown();
		if (servers.find(key) != servers.end()) {
			throw InvalidInputException("OTLP server already exists for %s", key);
		}
	}
	// Construct (bind + EnsureTargetTables) with the lock released.
	auto server = make_shared_ptr<OtlpServer>(context, listen_uri, config);
	// Re-acquire and insert with a recheck: a concurrent CreateServer for the same key may have
	// inserted while we were unlocked, or the registry may have started tearing down. Decide
	// under the lock, but tear the just-built server down OUTSIDE the lock (Close() stops its
	// listener + sealer and drains its empty buffer, and must not run while head-of-line-blocking
	// other registry operations).
	bool duplicate = false;
	bool shutting_down_now = false;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		if (shutting_down) {
			shutting_down_now = true;
		} else if (servers.find(key) != servers.end()) {
			duplicate = true;
		} else {
			auto &server_ref = *server;
			servers.emplace(key, std::move(server));
			return server_ref;
		}
	}
	// Lost the race (duplicate) or the registry is shutting down: clean up the just-built server.
	server->Close(); // idempotent; safe here (controlling thread, not an httplib worker)
	server.reset();
	if (shutting_down_now) {
		throw InvalidInputException("OTLP server state is shutting down");
	}
	throw InvalidInputException("OTLP server already exists for %s", key);
#endif
}

OtlpStorageExtensionInfo::StopResult OtlpStorageExtensionInfo::StopServer(ClientContext &context,
                                                                          const OtlpUri &listen_uri) {
	shared_ptr<OtlpServer> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		EnsureNotShutDown();
		const auto it = servers.find(listen_uri.CanonicalUri());
		if (it == servers.end()) {
			return {};
		}
		to_destroy = std::move(it->second);
		servers.erase(it);
	}
	// Synchronously stop listeners/workers and drain the final seal before returning.
	// A concurrent otlp_flush may still hold another shared_ptr; Close() is idempotent
	// and serializes with it through the server's writer mutex.
	to_destroy->Close();
	StopResult result;
	result.found = true;
	// Close() ran ShutdownIngest()'s final drain; read how many rows it had to drop so the
	// caller can surface a data-dropping shutdown (review finding M4). Read the recorded value
	// rather than ShutdownIngest()'s return so we are not coupled to which of the (idempotent)
	// teardown calls actually performed the drain.
	result.dropped_rows = to_destroy->ShutdownDroppedRows();
	to_destroy.reset();
	return result;
}

OtlpStorageExtensionInfo::FlushResult OtlpStorageExtensionInfo::FlushServer(const OtlpUri &listen_uri) {
	// Take a shared_ptr ref under the lock, then release the lock before the seal. The
	// ref keeps the server alive even if a concurrent otlp_stop erases it, and dropping
	// the lock means a long seal doesn't block otlp_serve/otlp_stop/otlp_server_list.
	shared_ptr<OtlpServer> server;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		EnsureNotShutDown();
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
	} catch (...) {
		try {
			throw;
		} catch (std::exception &ex) {
			result.error = ex.what();
		} catch (...) {
			result.error = "unknown (non-std) exception during flush";
		}
	}
	result.seals_total = server->SealsTotal();
	return result;
}

void OtlpStorageExtensionInfo::StopAllServers() {
	vector<shared_ptr<OtlpServer>> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		// Mark teardown under the lock so a racing registry operation from another connection
		// fails cleanly (EnsureNotShutDown) instead of touching servers we are about to free
		// (review finding L4). Idempotent: a second teardown just finds an empty registry.
		shutting_down = true;
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
	EnsureNotShutDown();
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
		snap.buffered_bytes = server.BufferedBytes();
		snap.admitted_bytes = server.AdmittedBytes();
		snap.seal_target_bytes = server.SealTargetBytes();
		snap.seal_max_age_ms = server.SealMaxAgeMs();
		snap.oldest_buffered_age_ms = server.OldestBufferedAgeMs();
		snap.last_seal_age_ms = server.LastSealAgeMs();
		snap.seals_total = server.SealsTotal();
		snap.committed_rows_total = server.CommittedRowsTotal();
		snap.seal_failures_total = server.SealFailuresTotal();
		snap.seal_last_error = server.SealLastError();
		snap.maintenance_runs_total = server.MaintenanceRunsTotal();
		snap.maintenance_failures_total = server.MaintenanceFailuresTotal();
		snap.last_maintenance_age_ms = server.LastMaintenanceAgeMs();
		snap.maintenance_last_error = server.MaintenanceLastError();
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
	EnsureNotShutDown();
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
