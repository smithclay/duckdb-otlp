#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

class OTLPCatalog;

//! Schema entry for OTLP catalog that knows about virtual tables
class OTLPSchemaEntry : public SchemaCatalogEntry {
public:
	OTLPSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);

	//! Scan all catalog entries in this schema (tables, views, etc.)
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	//! Scan all catalog entries of a specific type matching a specific name
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	//! Get a specific catalog entry by name
	optional_ptr<CatalogEntry> GetEntry(CatalogType type, const string &name);

	//! Lookup an entry
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	//! Create methods (all not supported - read-only)
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                        TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                                CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                 CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	//! Alter an entry (not supported - read-only)
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

	//! Drop an entry (not supported - read-only)
	void DropEntry(ClientContext &context, DropInfo &info) override;
};

} // namespace duckdb
