#include "storage/otlp_storage_extension.hpp"
#include "storage/otlp_storage_info.hpp"
#include "receiver/otlp_receiver.hpp"
#include "catalog/otlp_schema.hpp"
#include "catalog/otlp_catalog.hpp"
#include "storage/otlp_transaction.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Destructor implementation (declared in otlp_storage_info.hpp)
OTLPStorageInfo::~OTLPStorageInfo() {
	// Stop receiver if running
	if (receiver) {
		receiver->Stop();
	}
}

unique_ptr<StorageExtension> OTLPStorageExtension::Create() {
	auto storage = make_uniq<StorageExtension>();
	storage->attach = Attach;
	storage->create_transaction_manager = CreateTransactionManager;
	return storage;
}

unique_ptr<Catalog> OTLPStorageExtension::Attach(optional_ptr<StorageExtensionInfo> storage_info,
                                                 ClientContext &context, AttachedDatabase &db, const string &name,
                                                 AttachInfo &info, AttachOptions &options) {

	// Parse the connection string: otlp:host:port or host:port
	// If TYPE is specified, DuckDB doesn't strip the prefix, so we need to handle both
	auto &path = info.path;
	string connection_str = path;

	// Remove "otlp:" prefix if present
	if (StringUtil::StartsWith(connection_str, "otlp:")) {
		connection_str = connection_str.substr(5); // Remove "otlp:"
	}

	// Parse host:port
	string host = "localhost";
	uint16_t port = 4317; // Default OTLP port

	auto colon_pos = connection_str.find(':');
	if (colon_pos != string::npos) {
		host = connection_str.substr(0, colon_pos);
		try {
			port = (uint16_t)std::stoi(connection_str.substr(colon_pos + 1));
		} catch (...) {
			throw BinderException("Invalid port number in OTLP connection string: " + path);
		}
	} else if (!connection_str.empty()) {
		host = connection_str;
	}

	// Parse buffer capacity from attach options (default: 10000)
	idx_t buffer_capacity = 10000;
	auto buffer_size_iter = options.options.find("buffer_size");
	if (buffer_size_iter != options.options.end()) {
		try {
			buffer_capacity = (idx_t)std::stoull(buffer_size_iter->second.ToString());
			if (buffer_capacity == 0) {
				throw BinderException("buffer_size must be greater than 0");
			}
		} catch (const std::exception &e) {
			throw BinderException("Invalid buffer_size value: " + buffer_size_iter->second.ToString());
		}
	}

	// Create OTLP storage info with ring buffers
	auto otlp_info = make_shared_ptr<OTLPStorageInfo>(host, port, buffer_capacity);
	otlp_info->schema_name = name;

	// Create custom OTLP catalog with virtual tables backed by ring buffers
	auto catalog = make_uniq<OTLPCatalog>(db, otlp_info);

	catalog->Initialize(false); // Don't load builtins

	// Create and start gRPC receiver (inserts into ring buffers)
	otlp_info->receiver = make_uniq<OTLPReceiver>(host, port, otlp_info);
	try {
		otlp_info->receiver->Start();
	} catch (std::exception &ex) {
		throw IOException("Failed to start OTLP gRPC receiver on " + host + ":" + std::to_string(port) + ": " +
		                  string(ex.what()));
	}

	return catalog;
}

unique_ptr<TransactionManager>
OTLPStorageExtension::CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                               Catalog &catalog) {

	// Use our custom OTLP transaction manager for read-only access
	return make_uniq<OTLPTransactionManager>(db);
}

} // namespace duckdb
