// insert_executor.cpp

#include "execution/executors/insert_executor.h"
#include "execution/execution_context.h"
#include <stdexcept>

namespace tetodb {

InsertExecutor::InsertExecutor(ExecutionContext *exec_ctx,
                               const InsertPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_indexes_ =
      exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableOid());
  cursor_ = 0;
}

bool InsertExecutor::Next(Tuple *tuple, RID *rid) {
  const auto &raw_exprs = plan_->GetRawExpressions();
  if (cursor_ >= raw_exprs.size())
    return false;

  Transaction *txn = exec_ctx_->GetTransaction();
  Catalog *catalog = exec_ctx_->GetCatalog();
  const Schema *schema = &table_info_->schema_;

  // --- NEW: Evaluate expressions at runtime to form the tuple ---
  std::vector<Value> raw_values;
  for (const auto *expr : raw_exprs[cursor_]) {
    raw_values.push_back(
        expr->Evaluate(nullptr, schema, exec_ctx_->GetParams()));
  }
  Tuple to_insert(raw_values, schema);

  // ==========================================
  // 0. NOT NULL CONSTRAINT CHECK
  // ==========================================
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    const Column &col = schema->GetColumn(i);
    if (!col.IsNullable() && raw_values[i].IsNull()) {
      throw std::runtime_error("Constraint Violation: Column '" +
                               col.GetName() + "' cannot be NULL.");
    }
  }

  // ==========================================
  // 1. PRIMARY KEY / UNIQUE CONSTRAINT CHECK
  // ==========================================
  for (IndexMetadata *index_info : table_indexes_) {
    std::vector<Column> key_cols;
    std::vector<Value> key_values;

    for (uint32_t col_idx : index_info->key_attrs_) {
      key_cols.push_back(schema->GetColumn(col_idx));
      key_values.push_back(to_insert.GetValue(schema, col_idx));
    }

    Schema key_schema(key_cols);
    Tuple key_tuple(key_values, &key_schema);
    std::vector<RID> result_rids;

    index_info->index_->ScanKey(key_tuple, &result_rids, txn);

    if (index_info->is_unique_ && !result_rids.empty()) {
      throw std::runtime_error("Constraint Violation: Duplicate key value "
                               "violates Primary Key / Unique index '" +
                               index_info->name_ + "'");
    }
  }

  // ==========================================
  // 2. FOREIGN KEY CONSTRAINT CHECK
  // ==========================================
  LockManager *lock_mgr = exec_ctx_->GetLockManager();

  for (const auto &fk : table_info_->foreign_keys_) {
    Value child_val = to_insert.GetValue(schema, fk.child_key_attrs_[0]);
    TableMetadata *parent_meta = catalog->GetTable(fk.parent_table_oid_);
    if (!parent_meta)
      throw std::runtime_error(
          "Foreign Key Error: Parent table no longer exists.");

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
      Tuple search_key(std::vector<Value>{child_val}, &p_key_schema);
      std::vector<RID> p_rids;
      pk_index->index_->ScanKey(search_key, &p_rids, txn);
      if (!p_rids.empty()) {
        found_parent = true;
        if (!lock_mgr->LockShared(txn, p_rids[0])) {
          throw std::runtime_error("Transaction Aborted: Failed to acquire "
                                   "Shared Lock on Parent Tuple.");
        }
      }
    } else {
      auto p_iter = parent_meta->table_->Begin(txn);
      while (p_iter != parent_meta->table_->End()) {
        Tuple p_tuple;
        if (parent_meta->table_->GetTuple(p_iter.GetRid(), &p_tuple, txn)) {
          Value p_val =
              p_tuple.GetValue(&parent_meta->schema_, fk.parent_key_attrs_[0]);
          if (p_val.CompareEquals(child_val)) {
            found_parent = true;
            if (!lock_mgr->LockShared(txn, p_iter.GetRid())) {
              throw std::runtime_error("Transaction Aborted: Failed to acquire "
                                       "Shared Lock on Parent Tuple.");
            }
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
  // 3. PHYSICAL INSERTION & LOGGING
  // ==========================================
  RID new_rid;

  if (table_info_->table_->InsertTuple(to_insert, &new_rid, txn, lock_mgr)) {

    TableWriteRecord write_record(new_rid, WType::INSERT,
                                  table_info_->table_.get());
    txn->AppendTableWriteRecord(write_record);

    // ==========================================
    // 4. INDEX MAINTENANCE (WRITE TO B+ TREE)
    // ==========================================
    for (IndexMetadata *index_info : table_indexes_) {
      std::vector<Column> key_cols;
      std::vector<Value> key_values;

      for (uint32_t col_idx : index_info->key_attrs_) {
        key_cols.push_back(schema->GetColumn(col_idx));
        key_values.push_back(to_insert.GetValue(schema, col_idx));
      }

      Schema key_schema(key_cols);
      Tuple key_tuple(key_values, &key_schema);

      index_info->index_->InsertEntry(key_tuple, new_rid, txn);

      IndexWriteRecord idx_record(new_rid, WType::INSERT, key_tuple,
                                  index_info->index_.get());
      txn->AppendIndexWriteRecord(idx_record);
    }

    *tuple = to_insert;
    *rid = new_rid;
    cursor_++;
    return true;
  }
  return false;
}

const Schema *InsertExecutor::GetOutputSchema() {
  return &table_info_->schema_;
}

} // namespace tetodb