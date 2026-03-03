// index_scan_executor.cpp

#include "execution/executors/index_scan_executor.h"
#include "execution/execution_context.h" 
#include <stdexcept>                     

namespace tetodb {

    IndexScanExecutor::IndexScanExecutor(ExecutionContext* exec_ctx, const IndexScanPlanNode* plan)
        : AbstractExecutor(exec_ctx), plan_(plan) {
    }

    void IndexScanExecutor::Init() {
        table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
        IndexMetadata* index_meta = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

        result_rids_.clear();
        cursor_ = 0;

        // Upgraded to ScanKey! (Assumes plan_->GetSearchKey() returns a Tuple)
        index_meta->index_->ScanKey(plan_->GetSearchKey(), &result_rids_, exec_ctx_->GetTransaction());
    }

    bool IndexScanExecutor::Next(Tuple* tuple, RID* rid) {
        while (cursor_ < result_rids_.size()) {

            *rid = result_rids_[cursor_];

            Transaction* txn = exec_ctx_->GetTransaction();
            LockManager* lock_mgr = exec_ctx_->GetLockManager();

            if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
                bool locked = lock_mgr->LockShared(txn, *rid);
                if (!locked) {
                    txn->SetState(TransactionState::ABORTED);
                    throw std::runtime_error("Transaction Aborted: Failed to acquire Shared Lock in Index Scan.");
                }
            }

            bool success = table_metadata_->table_->GetTuple(*rid, tuple, txn);

            cursor_++;

            if (!success) {
                continue;
            }

            return true;
        }

        return false;
    }

    const Schema* IndexScanExecutor::GetOutputSchema() {
        return plan_->OutputSchema();
    }

}  // namespace tetodb