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
	if (shutting_down) {
		throw InvalidInputException("OTLP server state is shutting down");
	}
}

//! Identity of what a server writes to; one server per target keeps a single sealer per catalog.
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
	// Caller holds the lock.
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
	// The lock is never held across construction, which binds listeners and may block on a slow
	// remote catalog; so check, construct unlocked, then recheck and insert (or Close() unlocked).
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		EnsureNotShutDown();
		check_conflicts();
	}
	auto server = make_shared_ptr<OtlpServer>(context, listeners, config);
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
	server->Close(); // safe here: controlling thread, not an httplib worker
	server.reset();
	throw InvalidInputException(rejection);
#endif
}

//! Close an already-unregistered server (final seal included) and report what it dropped.
//! Stop-all records a failure and continues, so later servers still seal.
static OtlpStorageExtensionInfo::StopResult CloseAndDescribe(OtlpServer &server, bool catch_errors) {
	OtlpStorageExtensionInfo::StopResult result;
	for (auto &listener : server.Listeners()) {
		result.listen_uris.push_back(listener->Uri().Uri());
	}
	try {
		server.Close();
	} catch (std::exception &ex) {
		if (!catch_errors) {
			throw;
		}
		result.error = ErrorData(ex).RawMessage();
	}
	// The recorded value, whichever (idempotent) teardown call performed the drain.
	result.dropped_rows = server.ShutdownDroppedRows();
	return result;
}

vector<shared_ptr<OtlpServer>> OtlpStorageExtensionInfo::Select(optional_ptr<const OtlpUri> listen_uri) const {
	EnsureNotShutDown();
	if (!listen_uri) {
		return DistinctServers();
	}
	auto entry = servers.find(listen_uri->CanonicalUri());
	if (entry == servers.end()) {
		throw InvalidInputException("No server found listening on %s", listen_uri->CanonicalUri());
	}
	return {entry->second};
}

vector<OtlpStorageExtensionInfo::StopResult>
OtlpStorageExtensionInfo::StopServers(optional_ptr<const OtlpUri> listen_uri) {
	vector<shared_ptr<OtlpServer>> selected;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		selected = Select(listen_uri);
		// Unregister before closing so a concurrent stop cannot stop a server twice.
		if (listen_uri) {
			for (auto entry = servers.begin(); entry != servers.end();) {
				entry = entry->second == selected.front() ? servers.erase(entry) : std::next(entry);
			}
		} else {
			servers.clear();
		}
	}
	vector<StopResult> results;
	results.reserve(selected.size());
	for (auto &server : selected) {
		results.push_back(CloseAndDescribe(*server, /*catch_errors=*/!listen_uri));
	}
	return results;
}

vector<OtlpStorageExtensionInfo::FlushResult>
OtlpStorageExtensionInfo::FlushServers(optional_ptr<const OtlpUri> listen_uri) {
	vector<shared_ptr<OtlpServer>> selected;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		selected = Select(listen_uri);
	}
	vector<FlushResult> results;
	results.reserve(selected.size());
	for (auto &server : selected) {
		FlushResult result;
		try {
			result.sealed_rows = server->FlushNow().rows;
		} catch (std::exception &ex) {
			result.error = ex.what();
		} catch (...) {
			result.error = "unknown (non-std) exception during flush";
		}
		result.seals_total = server->SealsTotal();
		results.push_back(std::move(result));
	}
	return results;
}

void OtlpStorageExtensionInfo::StopAllServers() {
	vector<shared_ptr<OtlpServer>> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		shutting_down = true;
		to_destroy = DistinctServers();
		servers.clear();
	}
	for (auto &server : to_destroy) {
		server->Close();
	}
	to_destroy.clear();
}

vector<vector<Value>> OtlpStorageExtensionInfo::ListServers() {
	vector<vector<Value>> rows;
	std::lock_guard<std::mutex> lock(servers_mutex);
	EnsureNotShutDown();
	for (auto &server : DistinctServers()) {
		for (auto &listener : server->Listeners()) {
			vector<Value> row;
			for (auto &column : OtlpServer::ListColumns()) {
				row.push_back(column.get(*server, *listener));
			}
			rows.push_back(std::move(row));
		}
	}
	// Column 0 is listen_uri.
	std::sort(rows.begin(), rows.end(), [](const vector<Value> &a, const vector<Value> &b) {
		return StringValue::Get(a[0]) < StringValue::Get(b[0]);
	});
	return rows;
}

} // namespace duckdb
