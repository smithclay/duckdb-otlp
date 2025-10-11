#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

//! OTLPStorageExtension implements DuckDB's StorageExtension interface
//! This enables ATTACH 'otlp://host:port' AS name (TYPE otlp)
class OTLPStorageExtension {
public:
	//! Create and configure the storage extension
	static unique_ptr<StorageExtension> Create();

	//! Attach function - called when user executes ATTACH with TYPE otlp
	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);

	//! Create transaction manager for the attached database
	static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
	                                                               AttachedDatabase &db, Catalog &catalog);
};

} // namespace duckdb
