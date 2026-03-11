// seq_scan_executor.cpp

#include "execution/executors/seq_scan_executor.h"
#include "execution/execution_context.h"
#include <stdexcept>

namespace tetodb {

SeqScanExecutor::SeqScanExecutor(ExecutionContext *exec_ctx,
                                 const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  metadata_ =
      exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()); // O(1) Lookup!
  iter_ = std::make_unique<TableIterator>(metadata_->table_->Begin());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // We loop so that if a tuple fails the WHERE clause,
  // we keep searching until we find one that passes.
  while (*iter_ != metadata_->table_->End()) {

    // 1. Extract the RID from the iterator
    *rid = (*iter_).GetRid();

    // ==========================================================
    // 2. ACQUIRE SHARED LOCK (S-LOCK)
    // ==========================================================
    Transaction *txn = exec_ctx_->GetTransaction();
    LockManager *lock_mgr = exec_ctx_->GetLockManager();

    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      bool locked = lock_mgr->LockShared(txn, *rid);
      if (!locked) {
        txn->SetState(TransactionState::ABORTED);
        throw std::runtime_error(
            "Transaction Aborted: Failed to acquire Shared Lock.");
      }
    }

    // ==========================================================
    // 3. FETCH THE TUPLE (Safely protected by the lock)
    // ==========================================================
    bool fetch_success = metadata_->table_->GetTuple(*rid, tuple, txn);

    // 4. Advance the iterator NOW so `continue` statements don't cause infinite
    // loops!
    ++(*iter_);

    // ==========================================================
    // 4.5 RELEASE SHARED LOCK IF READ_COMMITTED
    // ==========================================================
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->Unlock(txn, *rid);
    }

    if (!fetch_success) {
      // Tuple was physically deleted before we got the lock. Skip it.
      continue;
    }

    // ==========================================================
    // 5. EVALUATE THE WHERE CLAUSE (PREDICATE)
    // ==========================================================
    const AbstractExpression *predicate = plan_->GetPredicate();

    if (predicate != nullptr) {
      // Evaluate the expression tree against the tuple we just fetched
      Value result = predicate->Evaluate(tuple, &metadata_->schema_);

      // If the WHERE clause evaluates to false, this row doesn't match.
      if (!result.GetAsBoolean()) {
        continue; // Loop around and try the next tuple
      }
    }

    // 6. If we made it here, the tuple passed the filter!
    return true;
  }

  // We hit the end of the table without finding any more matching tuples.
  return false;
}

const Schema *SeqScanExecutor::GetOutputSchema() {
  return plan_->OutputSchema();
}

} // namespace tetodb