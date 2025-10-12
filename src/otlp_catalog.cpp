#include "otlp_catalog.hpp"
#include "otlp_storage_info.hpp"
#include "otlp_table_entry.hpp"
#include "otlp_schema_entry.hpp"
#include "otlp_receiver.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

OTLPCatalog::OTLPCatalog(AttachedDatabase &db, shared_ptr<OTLPStorageInfo> storage_info)
    : Catalog(db), storage_info_(storage_info) {
}

OTLPCatalog::~OTLPCatalog() = default;

void OTLPCatalog::Initialize(bool load_builtin) {
	// Create the main schema for this OTLP catalog
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;

	main_schema_ = make_uniq<OTLPSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> OTLPCatalog::GetEntry(ClientContext &context, const string &schema, const string &name) {
	// Only support default schema
	if (schema != DEFAULT_SCHEMA) {
		return nullptr;
	}

	// Get ring buffer for this table name
	auto buffer = storage_info_->GetBuffer(name);
	if (!buffer) {
		return nullptr; // Not one of our virtual tables
	}

	// Check if we already created this entry (cache lookup)
	auto entry_key = schema + "." + name;
	auto it = table_entries_.find(entry_key);
	if (it != table_entries_.end()) {
		return it->second.get(); // Return cached entry
	}

	// Create table info with schema
	auto table_info = make_uniq<CreateTableInfo>();
	table_info->schema = schema;
	table_info->table = name;
	table_info->columns.AddColumn(ColumnDefinition("timestamp", LogicalType::TIMESTAMP));
	table_info->columns.AddColumn(ColumnDefinition("resource", LogicalType::JSON()));
	table_info->columns.AddColumn(ColumnDefinition("data", LogicalType::JSON()));

	// Create and cache the table entry
	auto entry = make_uniq<OTLPTableEntry>(*this, *main_schema_, *table_info, buffer);
	auto entry_ptr = entry.get();
	table_entries_[entry_key] = std::move(entry);

	return entry_ptr;
}

optional_ptr<CatalogEntry> OTLPCatalog::GetEntryCached(const string &name) {
	// Check cache first
	auto entry_key = DEFAULT_SCHEMA + string(".") + name;
	auto it = table_entries_.find(entry_key);
	if (it != table_entries_.end()) {
		return it->second.get();
	}

	// Not cached - create it
	auto buffer = storage_info_->GetBuffer(name);
	if (!buffer) {
		return nullptr;
	}

	// Create table info
	auto table_info = make_uniq<CreateTableInfo>();
	table_info->schema = DEFAULT_SCHEMA;
	table_info->table = name;
	table_info->columns.AddColumn(ColumnDefinition("timestamp", LogicalType::TIMESTAMP));
	table_info->columns.AddColumn(ColumnDefinition("resource", LogicalType::JSON()));
	table_info->columns.AddColumn(ColumnDefinition("data", LogicalType::JSON()));

	// Create and cache
	auto entry = make_uniq<OTLPTableEntry>(*this, *main_schema_, *table_info, buffer);
	auto entry_ptr = entry.get();
	table_entries_[entry_key] = std::move(entry);

	return entry_ptr;
}

void OTLPCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	// Only have one schema - the main schema
	if (main_schema_) {
		callback(*main_schema_);
	}
}

optional_ptr<SchemaCatalogEntry> OTLPCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	// Only support the default schema (main)
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA) {
		return main_schema_.get();
	}

	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema with name \"%s\" does not exist", schema_lookup.GetEntryName());
}

optional_ptr<CatalogEntry> OTLPCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("OTLP catalogs do not support schema creation");
}

void OTLPCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("OTLP catalogs do not support schema deletion");
}

DatabaseSize OTLPCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize result;
	// Approximate size based on ring buffer contents
	idx_t total_size = 0;
	if (storage_info_) {
		total_size += storage_info_->traces_buffer->Size();
		total_size += storage_info_->metrics_buffer->Size();
		total_size += storage_info_->logs_buffer->Size();
	}
	// Rough estimate: 1KB per row
	result.bytes = total_size * 1024;
	result.block_size = 0;
	result.total_blocks = 0;
	result.used_blocks = 0;
	result.free_blocks = 0;
	result.wal_size = 0;
	return result;
}

PhysicalOperator &OTLPCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
	throw BinderException("OTLP catalogs are read-only");
}

PhysicalOperator &OTLPCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	throw BinderException("OTLP catalogs are read-only");
}

PhysicalOperator &OTLPCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	throw BinderException("OTLP catalogs are read-only");
}

PhysicalOperator &OTLPCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	throw BinderException("OTLP catalogs are read-only");
}

bool OTLPCatalog::InMemory() {
	return true; // OTLP catalogs store data in-memory ring buffers
}

string OTLPCatalog::GetDBPath() {
	return ""; // In-memory, no database path
}

} // namespace duckdb
