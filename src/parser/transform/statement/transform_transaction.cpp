#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include <iostream>

namespace duckdb {

unique_ptr<TransactionStatement> Transformer::TransformTransaction(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGTransactionStmt *>(node);
	std::cout << "TransformTransaction stmt->kind : " << int(stmt->kind) << std::endl;
	D_ASSERT(stmt);
	switch (stmt->kind) {
	case duckdb_libpgquery::PG_TRANS_STMT_BEGIN: // 0
	case duckdb_libpgquery::PG_TRANS_STMT_START: // 1
		return make_uniq<TransactionStatement>(TransactionType::BEGIN_TRANSACTION);
	case duckdb_libpgquery::PG_TRANS_STMT_COMMIT: // 2
		return make_uniq<TransactionStatement>(TransactionType::COMMIT);
	case duckdb_libpgquery::PG_TRANS_STMT_ROLLBACK: // 3
		return make_uniq<TransactionStatement>(TransactionType::ROLLBACK);
	default:
		throw NotImplementedException("Transaction type %d not implemented yet", stmt->kind);
	}
}

} // namespace duckdb
