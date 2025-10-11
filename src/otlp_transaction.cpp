#include "otlp_transaction.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

OTLPTransactionManager::OTLPTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
}

Transaction &OTLPTransactionManager::StartTransaction(ClientContext &context) {
	// Create a simple read-only transaction
	auto transaction = make_uniq<Transaction>(static_cast<TransactionManager&>(*this), context);
	transaction->active_query = MAXIMUM_QUERY_ID;
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[&result] = std::move(transaction);
	return result;
}

ErrorData OTLPTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	// Read-only catalog, nothing to commit
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(&transaction);
	return ErrorData();
}

void OTLPTransactionManager::RollbackTransaction(Transaction &transaction) {
	// Read-only catalog, nothing to rollback
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(&transaction);
}

void OTLPTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// OTLP catalogs are in-memory, no checkpoint needed
}

} // namespace duckdb
