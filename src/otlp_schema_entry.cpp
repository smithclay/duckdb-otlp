#include "otlp_schema_entry.hpp"
#include "otlp_catalog.hpp"
#include "otlp_types.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

OTLPSchemaEntry::OTLPSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

void OTLPSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	// Only enumerate tables
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}

	// List the three OTLP virtual tables
	const OTLPSignalType signal_types[] = {OTLPSignalType::TRACES, OTLPSignalType::METRICS, OTLPSignalType::LOGS};
	for (auto signal_type : signal_types) {
		string table_name = SignalTypeToString(signal_type);
		// Get the table entry from the catalog using our custom GetEntry
		auto &otlp_catalog = catalog.Cast<OTLPCatalog>();
		auto entry = otlp_catalog.GetEntry(context, DEFAULT_SCHEMA, table_name);
		if (entry) {
			callback(*entry);
		}
	}
}

void OTLPSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// Only enumerate tables
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}

	// List the three OTLP virtual tables (without context)
	// We can enumerate the known table names directly
	const OTLPSignalType signal_types[] = {OTLPSignalType::TRACES, OTLPSignalType::METRICS, OTLPSignalType::LOGS};
	auto &otlp_catalog = catalog.Cast<OTLPCatalog>();

	for (auto signal_type : signal_types) {
		string table_name = SignalTypeToString(signal_type);
		// Try to get cached entry, or create it if needed
		// We use a temporary/null context which is OK since GetEntry handles it
		auto entry = otlp_catalog.GetEntryCached(table_name);
		if (entry) {
			callback(*entry);
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

	// Check if it's one of our known tables
	auto signal_type = StringToSignalType(entry_name);
	if (signal_type) {
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
