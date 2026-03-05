// index_scan_executor.cpp

#include "execution/executors/index_scan_executor.h"
#include "execution/execution_context.h"
#include <stdexcept>

namespace tetodb {

IndexScanExecutor::IndexScanExecutor(ExecutionContext *exec_ctx,
                                     const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  IndexMetadata *index_meta =
      exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

  // Cache search metadata to check boundaries while iterating
  search_key_ = plan_->GetSearchKey();
  index_key_schema_ = index_meta->index_->GetKeySchema();

  // Initialize the Iterator from the Index wrapper
  // This physically locks the first matching Leaf Page in the BufferPool
  // via a ReadLatch, preventing concurrent ghost inserts (Phantom Reads).
  iterator_ = index_meta->index_->GetBeginIterator(search_key_);
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (!iterator_ || iterator_->IsEnd()) {
    return false;
  }

  while (!iterator_->IsEnd()) {
    // Boundary condition: Since the B+Tree is sorted, if we hit a key greater
    // than our search key, we are done.
    if (iterator_->IsPastSearchBound()) {
      break;
    }

    *rid = iterator_->GetCurrentRid();

    Transaction *txn = exec_ctx_->GetTransaction();
    LockManager *lock_mgr = exec_ctx_->GetLockManager();

    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      bool locked = lock_mgr->LockShared(txn, *rid);
      if (!locked) {
        txn->SetState(TransactionState::ABORTED);
        throw std::runtime_error("Transaction Aborted: Failed to acquire "
                                 "Shared Lock in Index Scan.");
      }
    }

    bool success = table_metadata_->table_->GetTuple(*rid, tuple, txn);

    // Advance the latch-crabbing underlying Iterator
    iterator_->Advance();

    if (!success) {
      continue;
    }

    return true;
  }

  return false;
}

const Schema *IndexScanExecutor::GetOutputSchema() {
  return plan_->OutputSchema();
}

} // namespace tetodb