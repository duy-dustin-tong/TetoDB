// delete_executor.cpp

#include "execution/executors/delete_executor.h"
#include "execution/execution_context.h"
#include "execution/fk_constraint_handler.h"
#include <stdexcept>

namespace tetodb {

// Clean Constructor!
DeleteExecutor::DeleteExecutor(ExecutionContext *exec_ctx,
                               const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // Read OID directly from the plan!
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_indexes_ =
      exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple to_delete;
  RID delete_rid;

  // 1. Get the next tuple from the child executor (e.g., SeqScan)
  if (!child_executor_->Next(&to_delete, &delete_rid)) {
    return false;
  }

  Transaction *txn = exec_ctx_->GetTransaction();
  Catalog *catalog = exec_ctx_->GetCatalog();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();

  // --- Smart Lock Router ---
  auto acquire_write_lock = [&](const RID &target_rid) -> bool {
    if (txn->GetExclusiveLockSet()->find(target_rid) !=
        txn->GetExclusiveLockSet()->end()) {
      return true;
    }
    if (txn->GetSharedLockSet()->find(target_rid) !=
        txn->GetSharedLockSet()->end()) {
      return lock_mgr->LockUpgrade(txn, target_rid);
    }
    return lock_mgr->LockExclusive(txn, target_rid);
  };

  // ==========================================
  // 2. ENFORCE FK ON DELETE CONSTRAINTS (CASCADE/RESTRICT/SET_NULL)
  // ==========================================
  FKConstraintHandler::EnforceOnDelete(to_delete, table_info_, catalog, txn,
                                       acquire_write_lock);

  // ==========================================
  // 3. PHYSICAL DELETION OF TARGET ROW
  // ==========================================
  if (!acquire_write_lock(delete_rid)) {

    throw std::runtime_error(
        "Transaction Aborted: Failed to acquire Exclusive Lock on Delete.");
  }

  if (table_info_->table_->MarkDelete(delete_rid, txn)) {
    TableWriteRecord write_record(delete_rid, WType::DELETE, to_delete,
                                  table_info_->table_.get());
    txn->AppendTableWriteRecord(write_record);

    for (IndexMetadata *index_info : table_indexes_) {
      std::vector<Column> key_cols;
      std::vector<Value> key_values;

      for (uint32_t col_idx : index_info->key_attrs_) {
        key_cols.push_back(table_info_->schema_.GetColumn(col_idx));
        key_values.push_back(
            to_delete.GetValue(&table_info_->schema_, col_idx));
      }

      Schema key_schema(key_cols);
      Tuple key_tuple(key_values, &key_schema);

      index_info->index_->DeleteEntry(key_tuple, delete_rid, txn);

      IndexWriteRecord idx_record(delete_rid, WType::DELETE, key_tuple,
                                  index_info->index_.get());
      txn->AppendIndexWriteRecord(idx_record);
    }

    *tuple = to_delete;
    *rid = delete_rid;
    return true;
  }

  return false;
}

const Schema *DeleteExecutor::GetOutputSchema() {
  return &table_info_->schema_;
}

} // namespace tetodb