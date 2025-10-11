#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "ring_buffer.hpp"
#include <unordered_map>

namespace duckdb {

class OTLPStorageInfo;
class OTLPTableEntry;

//! Custom catalog for OTLP storage extension
//! Provides virtual tables backed by ring buffers
class OTLPCatalog : public Catalog {
public:
	explicit OTLPCatalog(AttachedDatabase &db, shared_ptr<OTLPStorageInfo> storage_info);
	~OTLPCatalog() override;

	string GetCatalogType() override {
		return "otlp";
	}

	void Initialize(bool load_builtin) override;

	//! Get a catalog entry by name (returns virtual table entries)
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &schema, const string &name);

	//! Scan all schemas in this catalog
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	//! Look up a specific schema by name
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	//! Create a schema (not supported for OTLP - read-only structure)
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	//! Drop schema (not supported)
	void DropSchema(ClientContext &context, DropInfo &info) override;

	//! Get database size (in-memory, returns ring buffer usage)
	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//! Plan methods (not supported for read-only OTLP catalog)
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	//! Catalog properties
	bool InMemory() override;
	string GetDBPath() override;

private:
	shared_ptr<OTLPStorageInfo> storage_info_;
	unique_ptr<SchemaCatalogEntry> main_schema_;
	unordered_map<string, unique_ptr<OTLPTableEntry>> table_entries_; // Cache of table entries
};

} // namespace duckdb
