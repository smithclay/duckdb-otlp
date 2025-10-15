#include "catalog/otlp_schema_entry.hpp"
#include "catalog/otlp_catalog.hpp"
#include "schema/otlp_types.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

OTLPSchemaEntry::OTLPSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

void OTLPSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	auto &otlp_catalog = catalog.Cast<OTLPCatalog>();
	if (type == CatalogType::TABLE_ENTRY) {
		// List all 7 OTLP virtual tables (1 traces, 1 logs, 5 metric types)
		const OTLPTableType table_types[] = {OTLPTableType::TRACES,
		                                     OTLPTableType::LOGS,
		                                     OTLPTableType::METRICS_GAUGE,
		                                     OTLPTableType::METRICS_SUM,
		                                     OTLPTableType::METRICS_HISTOGRAM,
		                                     OTLPTableType::METRICS_EXP_HISTOGRAM,
		                                     OTLPTableType::METRICS_SUMMARY};
		for (auto table_type : table_types) {
			string table_name = TableTypeToString(table_type);
			auto entry = otlp_catalog.GetEntry(context, DEFAULT_SCHEMA, table_name);
			if (entry) {
				callback(*entry);
			}
		}
	}
}

void OTLPSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	auto &otlp_catalog = catalog.Cast<OTLPCatalog>();
	if (type == CatalogType::TABLE_ENTRY) {
		// List all 7 OTLP virtual tables (without context)
		const OTLPTableType table_types[] = {OTLPTableType::TRACES,
		                                     OTLPTableType::LOGS,
		                                     OTLPTableType::METRICS_GAUGE,
		                                     OTLPTableType::METRICS_SUM,
		                                     OTLPTableType::METRICS_HISTOGRAM,
		                                     OTLPTableType::METRICS_EXP_HISTOGRAM,
		                                     OTLPTableType::METRICS_SUMMARY};
		for (auto table_type : table_types) {
			string table_name = TableTypeToString(table_type);
			auto entry = otlp_catalog.GetEntryCached(table_name);
			if (entry) {
				callback(*entry);
			}
		}
	}
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::GetEntry(CatalogType type, const string &entry_name) {
	if (type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}

	// This method is called without a transaction context, so we need to access the catalog directly
	// Cast to OTLPCatalog and look up tables by name
	// Note: This is a simplified lookup that doesn't use transactions
	auto &otlp_catalog = catalog.Cast<OTLPCatalog>();

	// Check if it's one of our known tables using table type lookup
	auto table_type = StringToTableType(entry_name);
	if (table_type) {
		// We'd need a ClientContext to call GetEntry properly
		// For now, just return nullptr - this method is not commonly used
		return nullptr;
	}

	return nullptr;
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                        const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr; // Only support tables
	}

	// Delegate to catalog's GetEntry to get the table
	// Cast the catalog to OTLPCatalog to access our custom GetEntry method
	auto &otlp_catalog = catalog.Cast<OTLPCatalog>();
	return otlp_catalog.GetEntry(transaction.GetContext(), DEFAULT_SCHEMA, lookup_info.GetEntryName());
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                        TableCatalogEntry &table) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                CreateTableFunctionInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                               CreateCopyFunctionInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                 CreatePragmaFunctionInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

void OTLPSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

void OTLPSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw BinderException("OTLP schemas are read-only");
}

} // namespace duckdb
