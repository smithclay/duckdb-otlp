#include "otlp_storage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/error_data.hpp"
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

//! Identity of what a server writes to. Two servers with the same target would run two sealers
//! and two maintenance schedules against one catalog, which is the conflict one-server-per-target
//! exists to prevent.
static string IngestTargetKey(const OtlpServerConfig &config) {
	if (!config.parquet_export_path.empty()) {
		return "parquet export path " + config.parquet_export_path;
	}
	return "catalog '" + config.catalog_name + "' schema '" + config.schema_name + "'";
}

vector<shared_ptr<OtlpServer>> OtlpStorageExtensionInfo::DistinctServers() const {
	vector<shared_ptr<OtlpServer>> result;
	for (auto &kv : servers) {
		if (std::find(result.begin(), result.end(), kv.second) == result.end()) {
			result.push_back(kv.second);
		}
	}
	return result;
}

OtlpServer &OtlpStorageExtensionInfo::CreateServer(ClientContext &context, const vector<OtlpListenerSpec> &listeners,
                                                   const OtlpServerConfig &config) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
#else
	auto target = IngestTargetKey(config);
	// Throws if any listen URI is taken or the target already has a server. Caller holds the lock.
	auto check_conflicts = [&] {
		for (auto &spec : listeners) {
			auto key = spec.uri.CanonicalUri();
			if (servers.find(key) != servers.end()) {
				throw InvalidInputException("OTLP server already exists for %s", key);
			}
		}
		for (auto &existing : DistinctServers()) {
			if (IngestTargetKey(existing->Config()) == target) {
				throw InvalidInputException("OTLP server already exists for %s (listening on %s); pass every listen "
				                            "URI to one otlp_serve call instead of starting a second server",
				                            target, existing->PrimaryUri().Uri());
			}
		}
	};
	// Fast pre-check under the lock. The OtlpServer constructor binds the listeners and (for a
	// remote catalog) runs EnsureTargetTables, which can block for a long time on a slow
	// DuckLake/S3/Postgres attach. Holding servers_mutex across that head-of-line-blocks every
	// concurrent otlp_stop/otlp_flush/otlp_server_list/StopAllServers, so we only take the lock
	// for the (cheap) conflict check and the (cheap) insert — never across construction
	// (review finding M1).
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		EnsureNotShutDown();
		check_conflicts();
	}
	// Construct (bind + EnsureTargetTables) with the lock released.
	auto server = make_shared_ptr<OtlpServer>(context, listeners, config);
	// Re-acquire and insert with a recheck: a concurrent CreateServer for the same URI or target
	// may have inserted while we were unlocked, or the registry may have started tearing down.
	// Decide under the lock, but tear the just-built server down OUTSIDE the lock (Close() stops
	// its listeners + sealer and drains its empty buffer, and must not run while head-of-line-
	// blocking other registry operations).
	string rejection;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		if (shutting_down) {
			rejection = "OTLP server state is shutting down";
		} else {
			try {
				check_conflicts();
				for (auto &spec : listeners) {
					servers.emplace(spec.uri.CanonicalUri(), server);
				}
				return *server;
			} catch (InvalidInputException &ex) {
				rejection = ErrorData(ex).RawMessage();
			}
		}
	}
	// Lost the race or the registry is shutting down: clean up the just-built server.
	server->Close(); // idempotent; safe here (controlling thread, not an httplib worker)
	server.reset();
	throw InvalidInputException(rejection);
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
		to_destroy = it->second;
		// Any listener URI names the whole server: unregister all of its listeners.
		for (auto entry = servers.begin(); entry != servers.end();) {
			entry = entry->second == to_destroy ? servers.erase(entry) : std::next(entry);
		}
	}
	// Synchronously stop every listener, then drain the final seal once before returning.
	// A concurrent otlp_flush may still hold another shared_ptr; Close() is idempotent
	// and serializes with it through the server's writer mutex.
	to_destroy->Close();
	StopResult result;
	result.found = true;
	for (auto &listener : to_destroy->Listeners()) {
		result.listen_uris.push_back(listener->Uri().Uri());
	}
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
		to_destroy = DistinctServers();
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
	for (auto &server_ptr : DistinctServers()) {
		auto &server = *server_ptr;
		// Server-wide state, read once and shared by every listener row.
		ServerSnapshot shared;
		shared.catalog_name = server.CatalogName();
		shared.schema_name = server.SchemaName();
		shared.active_requests = server.ActiveRequests();
		shared.total_requests = server.TotalRequests();
		shared.total_rows = server.TotalRows();
		shared.buffered_rows = server.BufferedRows();
		shared.buffered_bytes = server.BufferedBytes();
		shared.admitted_bytes = server.AdmittedBytes();
		shared.seal_target_bytes = server.SealTargetBytes();
		shared.seal_max_age_ms = server.SealMaxAgeMs();
		shared.oldest_buffered_age_ms = server.OldestBufferedAgeMs();
		shared.last_seal_age_ms = server.LastSealAgeMs();
		shared.seals_total = server.SealsTotal();
		shared.committed_rows_total = server.CommittedRowsTotal();
		shared.seal_failures_total = server.SealFailuresTotal();
		shared.seal_last_error = server.SealLastError();
		shared.maintenance_runs_total = server.MaintenanceRunsTotal();
		shared.maintenance_failures_total = server.MaintenanceFailuresTotal();
		shared.last_maintenance_age_ms = server.LastMaintenanceAgeMs();
		shared.maintenance_last_error = server.MaintenanceLastError();
		shared.promoted_columns_total = server.PromotedColumnsTotal();
		for (auto &listener : server.Listeners()) {
			auto &uri = listener->Uri();
			auto snap = shared;
			snap.listen_uri = uri.Uri();
			snap.transport = OtlpTransportName(listener->Transport());
			snap.listen_url = uri.Http();
			snap.host = uri.Host();
			snap.port = uri.Port();
			snap.is_listening = listener->IsListening();
			snap.last_error = listener->LastError();
			result.push_back(std::move(snap));
		}
	}
	// Sort for deterministic output order.
	std::sort(result.begin(), result.end(),
	          [](const ServerSnapshot &a, const ServerSnapshot &b) { return a.listen_uri < b.listen_uri; });
	return result;
}

vector<OtlpStorageExtensionInfo::SealSnapshot> OtlpStorageExtensionInfo::ListSeals() {
	vector<SealSnapshot> result;
	std::lock_guard<std::mutex> lock(servers_mutex);
	EnsureNotShutDown();
	for (auto &server : DistinctServers()) {
		auto primary = server->PrimaryUri().Uri();
		for (auto &event : server->SealHistory()) {
			result.push_back({primary, std::move(event)});
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
