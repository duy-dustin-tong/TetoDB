// update_executor.cpp

#include "execution/executors/update_executor.h"
#include "execution/execution_context.h"
#include "execution/fk_constraint_handler.h"
#include <stdexcept>
#include <vector>

namespace tetodb {

UpdateExecutor::UpdateExecutor(ExecutionContext *exec_ctx,
                               const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_indexes_ =
      exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableOid());
  child_executor_->Init();

  target_rids_.clear();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid))
    target_rids_.push_back(child_rid);
  cursor_ = 0;
}

bool UpdateExecutor::Next(Tuple *tuple, RID *rid) {
  while (cursor_ < target_rids_.size()) {
    RID child_rid = target_rids_[cursor_++];

    Transaction *txn = exec_ctx_->GetTransaction();
    LockManager *lock_mgr = exec_ctx_->GetLockManager();
    Catalog *catalog = exec_ctx_->GetCatalog();
    const Schema *schema = &table_info_->schema_;

    auto acquire_write_lock = [&](const RID &target_rid) -> bool {
      if (txn->GetExclusiveLockSet()->find(target_rid) !=
          txn->GetExclusiveLockSet()->end())
        return true;
      if (txn->GetSharedLockSet()->find(target_rid) !=
          txn->GetSharedLockSet()->end())
        return lock_mgr->LockUpgrade(txn, target_rid);
      return lock_mgr->LockExclusive(txn, target_rid);
    };

    if (!acquire_write_lock(child_rid))
      throw std::runtime_error(
          "Transaction Aborted: Failed to acquire Exclusive Lock for Update.");

    Tuple current_old_tuple;
    if (!table_info_->table_->GetTuple(child_rid, &current_old_tuple, txn))
      continue;

    std::vector<Value> new_values;
    uint32_t col_count = schema->GetColumnCount();
    new_values.reserve(col_count);

    for (uint32_t i = 0; i < col_count; ++i) {
      if (plan_->GetUpdateExpressions().count(i) > 0) {
        const AbstractExpression *expr = plan_->GetUpdateExpressions().at(i);
        new_values.push_back(
            expr->Evaluate(&current_old_tuple, schema, exec_ctx_->GetParams()));
      } else {
        new_values.push_back(current_old_tuple.GetValue(schema, i));
      }
    }

    Tuple new_tuple(new_values, schema);
    RID new_rid = child_rid;

    // ==========================================
    // 1. UNIQUE CONSTRAINT / DUPLICATE CHECK
    // ==========================================
    for (IndexMetadata *index_info : table_indexes_) {
      std::vector<Column> key_cols;
      std::vector<Value> new_key_values, old_key_values;

      for (uint32_t col_idx : index_info->key_attrs_) {
        key_cols.push_back(schema->GetColumn(col_idx));
        new_key_values.push_back(new_tuple.GetValue(schema, col_idx));
        old_key_values.push_back(current_old_tuple.GetValue(schema, col_idx));
      }

      bool key_changed = false;
      for (size_t i = 0; i < new_key_values.size(); i++) {
        if (new_key_values[i].CompareNotEquals(old_key_values[i])) {
          key_changed = true;
          break;
        }
      }

      if (key_changed) {
        Schema key_schema(key_cols);
        Tuple new_key_tuple(new_key_values, &key_schema);
        std::vector<RID> result_rids;
        index_info->index_->ScanKey(new_key_tuple, &result_rids, txn);

        if (index_info->is_unique_ && !result_rids.empty()) {
          throw std::runtime_error("Constraint Violation: Duplicate key value "
                                   "violates Primary Key / Unique index '" +
                                   index_info->name_ + "'");
        }
      }
    }

    // ==========================================
    // 2. CHILD-SIDE FOREIGN KEY VALIDATION
    // ==========================================
    for (const auto &fk : table_info_->foreign_keys_) {
      Value new_child_val = new_tuple.GetValue(schema, fk.child_key_attrs_[0]);
      Value old_child_val =
          current_old_tuple.GetValue(schema, fk.child_key_attrs_[0]);

      if (new_child_val.CompareEquals(old_child_val))
        continue; // Unchanged FK column, skip lookup

      TableMetadata *parent_meta = catalog->GetTable(fk.parent_table_oid_);
      if (!parent_meta) {
        throw std::runtime_error("Foreign Key Error: Parent table lost.");
      }

      bool found_parent = false;
      auto parent_indexes = catalog->GetTableIndexes(parent_meta->oid_);
      IndexMetadata *pk_index = nullptr;
      for (auto *idx : parent_indexes) {
        if (idx->key_attrs_ == fk.parent_key_attrs_) {
          pk_index = idx;
          break;
        }
      }

      if (pk_index) {
        std::vector<Column> p_key_cols = {
            parent_meta->schema_.GetColumn(fk.parent_key_attrs_[0])};
        Schema p_key_schema(p_key_cols);
        Tuple search_key(std::vector<Value>{new_child_val}, &p_key_schema);
        std::vector<RID> p_rids;
        pk_index->index_->ScanKey(search_key, &p_rids, txn);
        found_parent = !p_rids.empty();
      } else {
        auto p_iter = parent_meta->table_->Begin(txn);
        while (p_iter != parent_meta->table_->End()) {
          Tuple p_tuple;
          if (parent_meta->table_->GetTuple(p_iter.GetRid(), &p_tuple, txn)) {
            Value p_val = p_tuple.GetValue(&parent_meta->schema_,
                                           fk.parent_key_attrs_[0]);
            if (p_val.CompareEquals(new_child_val)) {
              found_parent = true;
              break;
            }
          }
          ++p_iter;
        }
      }

      if (!found_parent) {
        throw std::runtime_error(
            "Constraint Violation: Foreign key '" + fk.fk_name_ +
            "' referenced value does not exist in parent table '" +
            parent_meta->name_ + "'.");
      }
    }

    // ==========================================
    // 3. PARENT-SIDE CASCADE/RESTRICT LOGIC
    // ==========================================
    FKConstraintHandler::EnforceOnUpdate(current_old_tuple, new_tuple,
                                         table_info_, catalog, txn,
                                         acquire_write_lock);

    // ==========================================
    // 4. PHYSICAL UPDATE & LOGGING OF TARGET ROW
    // ==========================================
    if (table_info_->table_->UpdateTuple(new_tuple, &new_rid, txn, lock_mgr)) {
      // UpdateTuple internally logs DELETE + INSERT write records,
      // so we do NOT add another TableWriteRecord here.

      for (IndexMetadata *index_info : table_indexes_) {
        std::vector<Column> key_cols;
        std::vector<Value> old_key_values, new_key_values;

        for (uint32_t col_idx : index_info->key_attrs_) {
          key_cols.push_back(schema->GetColumn(col_idx));
          old_key_values.push_back(current_old_tuple.GetValue(schema, col_idx));
          new_key_values.push_back(new_tuple.GetValue(schema, col_idx));
        }

        Schema key_schema(key_cols);
        Tuple old_key_tuple(old_key_values, &key_schema);
        Tuple new_key_tuple(new_key_values, &key_schema);

        index_info->index_->DeleteEntry(old_key_tuple, child_rid, txn);
        IndexWriteRecord idx_record_del(child_rid, WType::DELETE, old_key_tuple,
                                        index_info->index_.get());
        txn->AppendIndexWriteRecord(idx_record_del);

        index_info->index_->InsertEntry(new_key_tuple, new_rid, txn);
        IndexWriteRecord idx_record_ins(new_rid, WType::INSERT, new_key_tuple,
                                        index_info->index_.get());
        txn->AppendIndexWriteRecord(idx_record_ins);
      }

      *tuple = new_tuple;
      *rid = new_rid;
      return true;
    }
  }
  return false;
}

const Schema *UpdateExecutor::GetOutputSchema() {
  return &table_info_->schema_;
}

} // namespace tetodb