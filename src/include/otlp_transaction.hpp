#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include <mutex>
#include <unordered_map>

namespace duckdb {

//! Simple read-only transaction manager for OTLP storage
class OTLPTransactionManager : public TransactionManager {
public:
	OTLPTransactionManager(AttachedDatabase &db);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	mutex transaction_lock;
	std::unordered_map<Transaction*, unique_ptr<Transaction>> transactions;
};

} // namespace duckdb
