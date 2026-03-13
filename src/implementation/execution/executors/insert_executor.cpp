// insert_executor.cpp

#include "execution/executors/insert_executor.h"
#include "execution/execution_context.h"
#include "execution/fk_constraint_handler.h"
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

  // --- Type coercion: VARCHAR → TIMESTAMP ---
  for (uint32_t i = 0; i < schema->GetColumnCount() && i < raw_values.size(); i++) {
    if (schema->GetColumn(i).GetTypeId() == TypeId::TIMESTAMP &&
        (raw_values[i].GetTypeId() == TypeId::VARCHAR ||
         raw_values[i].GetTypeId() == TypeId::CHAR) &&
        !raw_values[i].IsNull()) {
      int64_t epoch = Value::ParseTimestamp(raw_values[i].GetAsString());
      raw_values[i] = Value(TypeId::TIMESTAMP, epoch);
    }
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
    std::vector<Value> child_vals;
    for (uint32_t c_attr : fk.child_key_attrs_) {
      child_vals.push_back(to_insert.GetValue(schema, c_attr));
    }

    FKConstraintHandler::ValidateForeignKey(fk, child_vals, catalog, txn, lock_mgr);
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