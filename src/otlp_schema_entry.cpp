#include "otlp_schema_entry.hpp"
#include "otlp_catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

OTLPSchemaEntry::OTLPSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

void OTLPSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	printf("DEBUG OTLPSchemaEntry::Scan() called: type=%d (TABLE_ENTRY=%d)\n", (int)type,
	       (int)CatalogType::TABLE_ENTRY);

	// Only enumerate tables
	if (type != CatalogType::TABLE_ENTRY) {
		printf("DEBUG: Type mismatch, skipping\n");
		return;
	}

	printf("DEBUG: Enumerating OTLP tables...\n");

	// List the three OTLP virtual tables
	vector<string> table_names = {"traces", "metrics", "logs"};
	for (auto &table_name : table_names) {
		printf("DEBUG: Looking up table '%s'\n", table_name.c_str());
		// Get the table entry from the catalog using our custom GetEntry
		auto &otlp_catalog = catalog.Cast<OTLPCatalog>();
		auto entry = otlp_catalog.GetEntry(context, DEFAULT_SCHEMA, table_name);
		if (entry) {
			printf("DEBUG: Found entry for '%s', calling callback\n", table_name.c_str());
			callback(*entry);
		} else {
			printf("DEBUG: No entry found for '%s'\n", table_name.c_str());
		}
	}
	printf("DEBUG: OTLPSchemaEntry::Scan() complete\n");
}

void OTLPSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// This version is called without context - not commonly used
	// For now, we can't enumerate without context
}

optional_ptr<CatalogEntry> OTLPSchemaEntry::GetEntry(CatalogType type, const string &entry_name) {
	if (type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}

	// This method is called without a transaction context, so we need to access the catalog directly
	// Cast to OTLPCatalog and look up tables by name
	// Note: This is a simplified lookup that doesn't use transactions
	auto &otlp_catalog = catalog.Cast<OTLPCatalog>();

	// Check if it's one of our known tables
	if (entry_name == "traces" || entry_name == "metrics" || entry_name == "logs") {
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
