// fk_constraint_handler.cpp

#include "execution/fk_constraint_handler.h"
#include <stdexcept>

namespace tetodb {

void FKConstraintHandler::EnforceOnDelete(
    const Tuple &deleted_tuple, TableMetadata *parent_table, Catalog *catalog,
    Transaction *txn, const WriteLockFn &acquire_write_lock,
    LockManager *lock_mgr) {

  const Schema *parent_schema = &parent_table->schema_;

  auto all_tables = catalog->GetAllTables();
  for (TableMetadata *child_meta : all_tables) {
    for (const auto &fk : child_meta->foreign_keys_) {
      if (fk.parent_table_oid_ != parent_table->oid_)
        continue;

      // --- Find dependent rows ---
      std::vector<RID> dependent_rids;
      auto child_indexes = catalog->GetTableIndexes(child_meta->oid_);

      // Look for an index on the FK columns
      IndexMetadata *fk_index = nullptr;
      for (auto *idx : child_indexes) {
        if (idx->key_attrs_ == fk.child_key_attrs_) {
          fk_index = idx;
          break;
        }
      }

      if (fk_index) {
        // FAST PATH: O(log N) Index Scan
        std::vector<Column> key_cols;
        std::vector<Value> key_vals;
        for (size_t i = 0; i < fk.parent_key_attrs_.size(); i++) {
          key_cols.push_back(
              child_meta->schema_.GetColumn(fk.child_key_attrs_[i]));
          key_vals.push_back(
              deleted_tuple.GetValue(parent_schema, fk.parent_key_attrs_[i]));
        }
        Schema key_schema(key_cols);
        Tuple search_key(key_vals, &key_schema);
        fk_index->index_->ScanKey(search_key, &dependent_rids, txn);
      } else {
        // SLOW PATH: O(N) Sequential Heap Scan
        auto child_iter = child_meta->table_->Begin(txn);
        while (child_iter != child_meta->table_->End()) {
          RID child_rid = child_iter.GetRid();
          Tuple child_tuple;
          if (child_meta->table_->GetTuple(child_rid, &child_tuple, txn)) {
            bool match = true;
            for (size_t i = 0; i < fk.parent_key_attrs_.size(); i++) {
              Value parent_val = deleted_tuple.GetValue(
                  parent_schema, fk.parent_key_attrs_[i]);
              Value child_val = child_tuple.GetValue(&child_meta->schema_,
                                                     fk.child_key_attrs_[i]);
              if (!parent_val.CompareEquals(child_val)) {
                match = false;
                break;
              }
            }
            if (match) {
              dependent_rids.push_back(child_rid);
            }
          }
          ++child_iter;
        }
      }

      if (dependent_rids.empty())
        continue;

      // --- Process dependent rows based on referential action ---
      if (fk.on_delete_ == ReferentialAction::RESTRICT) {
        throw std::runtime_error(
            "Constraint Violation: Cannot delete row from '" +
            parent_table->name_ + "'. Referenced by child table '" +
            child_meta->name_ + "'.");
      }

      if (fk.on_delete_ == ReferentialAction::CASCADE) {
        for (const RID &child_rid : dependent_rids) {
          if (!acquire_write_lock(child_rid)) {
            throw std::runtime_error("Transaction Aborted: Failed to acquire "
                                     "Exclusive Lock on Cascade Delete.");
          }

          Tuple child_tuple;
          if (!child_meta->table_->GetTuple(child_rid, &child_tuple, txn))
            continue;

          child_meta->table_->MarkDelete(child_rid, txn);
          TableWriteRecord child_write(child_rid, WType::DELETE, child_tuple,
                                       child_meta->table_.get());
          txn->AppendTableWriteRecord(child_write);

          for (auto *child_idx : child_indexes) {
            std::vector<Column> child_key_cols;
            std::vector<Value> child_key_vals;
            for (uint32_t c_idx : child_idx->key_attrs_) {
              child_key_cols.push_back(child_meta->schema_.GetColumn(c_idx));
              child_key_vals.push_back(
                  child_tuple.GetValue(&child_meta->schema_, c_idx));
            }
            Schema child_key_schema(child_key_cols);
            Tuple child_key_tuple(child_key_vals, &child_key_schema);

            child_idx->index_->DeleteEntry(child_key_tuple, child_rid, txn);
            IndexWriteRecord idx_rec(child_rid, WType::DELETE, child_key_tuple,
                                     child_idx->index_.get());
            txn->AppendIndexWriteRecord(idx_rec);
          }
          // Recursive Cascade: propagate delete to grandchild tables
          EnforceOnDelete(child_tuple, child_meta, catalog, txn,
                          acquire_write_lock, lock_mgr);
        }
      }

      if (fk.on_delete_ == ReferentialAction::SET_NULL) {
        for (const RID &child_rid : dependent_rids) {
          if (!acquire_write_lock(child_rid)) {
            throw std::runtime_error("Transaction Aborted: Failed to acquire "
                                     "Exclusive Lock on SET NULL.");
          }

          Tuple c_old_tuple;
          if (!child_meta->table_->GetTuple(child_rid, &c_old_tuple, txn))
            continue;

          std::vector<Value> c_new_values;
          for (uint32_t i = 0; i < child_meta->schema_.GetColumnCount(); ++i) {
            bool is_fk_col = false;
            for (uint32_t fk_c_attr : fk.child_key_attrs_) {
              if (i == fk_c_attr) {
                is_fk_col = true;
                break;
              }
            }
            if (is_fk_col) {
              TypeId col_type = child_meta->schema_.GetColumn(i).GetTypeId();
              c_new_values.push_back(Value::GetNullValue(col_type));
            } else {
              c_new_values.push_back(
                  c_old_tuple.GetValue(&child_meta->schema_, i));
            }
          }
          Tuple c_new_tuple(c_new_values, &child_meta->schema_);
          RID c_new_rid = child_rid;

          if (child_meta->table_->UpdateTuple(c_new_tuple, &c_new_rid, txn, lock_mgr)) {
            for (auto *c_idx : child_indexes) {
              std::vector<Column> c_key_cols;
              std::vector<Value> c_old_kvals, c_new_kvals;
              for (uint32_t ci : c_idx->key_attrs_) {
                c_key_cols.push_back(child_meta->schema_.GetColumn(ci));
                c_old_kvals.push_back(
                    c_old_tuple.GetValue(&child_meta->schema_, ci));
                c_new_kvals.push_back(
                    c_new_tuple.GetValue(&child_meta->schema_, ci));
              }
              Schema c_key_schema(c_key_cols);
              Tuple c_old_key_tuple(c_old_kvals, &c_key_schema);
              Tuple c_new_key_tuple(c_new_kvals, &c_key_schema);

              c_idx->index_->DeleteEntry(c_old_key_tuple, child_rid, txn);
              IndexWriteRecord c_idx_del(child_rid, WType::DELETE,
                                         c_old_key_tuple, c_idx->index_.get());
              txn->AppendIndexWriteRecord(c_idx_del);

              c_idx->index_->InsertEntry(c_new_key_tuple, c_new_rid, txn);
              IndexWriteRecord c_idx_ins(c_new_rid, WType::INSERT,
                                         c_new_key_tuple, c_idx->index_.get());
              txn->AppendIndexWriteRecord(c_idx_ins);
            }
          }
          // Recursive Cascade: propagate update down to grandchild tables
          EnforceOnUpdate(c_old_tuple, c_new_tuple, child_meta, catalog, txn,
                          acquire_write_lock, lock_mgr);
        }
      }
    }
  }
}

void FKConstraintHandler::EnforceOnUpdate(
    const Tuple &old_tuple, const Tuple &new_tuple, TableMetadata *parent_table,
    Catalog *catalog, Transaction *txn, const WriteLockFn &acquire_write_lock,
    LockManager *lock_mgr) {

  const Schema *parent_schema = &parent_table->schema_;
  auto all_tables = catalog->GetAllTables();

  for (TableMetadata *child_meta : all_tables) {
    for (const auto &fk : child_meta->foreign_keys_) {
      if (fk.parent_table_oid_ != parent_table->oid_)
        continue;

      // Check if the referenced parent key actually changed
      bool key_changed = false;
      for (size_t i = 0; i < fk.parent_key_attrs_.size(); i++) {
        Value old_parent_val =
            old_tuple.GetValue(parent_schema, fk.parent_key_attrs_[i]);
        Value new_parent_val =
            new_tuple.GetValue(parent_schema, fk.parent_key_attrs_[i]);
        if (!old_parent_val.CompareEquals(new_parent_val)) {
          key_changed = true;
          break;
        }
      }

      if (!key_changed)
        continue; // Primary key didn't change, no cascade needed.

      // --- Find dependent rows ---
      std::vector<RID> dependent_rids;
      auto child_indexes = catalog->GetTableIndexes(child_meta->oid_);

      IndexMetadata *fk_index = nullptr;
      for (auto *idx : child_indexes) {
        if (idx->key_attrs_ == fk.child_key_attrs_) {
          fk_index = idx;
          break;
        }
      }

      if (fk_index) {
        std::vector<Column> key_cols;
        std::vector<Value> key_vals;
        for (size_t i = 0; i < fk.parent_key_attrs_.size(); i++) {
          key_cols.push_back(
              child_meta->schema_.GetColumn(fk.child_key_attrs_[i]));
          key_vals.push_back(
              old_tuple.GetValue(parent_schema, fk.parent_key_attrs_[i]));
        }
        Schema key_schema(key_cols);
        Tuple search_key(key_vals, &key_schema);
        fk_index->index_->ScanKey(search_key, &dependent_rids, txn);
      } else {
        auto child_iter = child_meta->table_->Begin(txn);
        while (child_iter != child_meta->table_->End()) {
          RID child_rid = child_iter.GetRid();
          Tuple child_tuple;
          if (child_meta->table_->GetTuple(child_rid, &child_tuple, txn)) {
            bool match = true;
            for (size_t i = 0; i < fk.parent_key_attrs_.size(); i++) {
              Value old_parent_val =
                  old_tuple.GetValue(parent_schema, fk.parent_key_attrs_[i]);
              Value child_val = child_tuple.GetValue(&child_meta->schema_,
                                                     fk.child_key_attrs_[i]);
              if (!old_parent_val.CompareEquals(child_val)) {
                match = false;
                break;
              }
            }
            if (match) {
              dependent_rids.push_back(child_rid);
            }
          }
          ++child_iter;
        }
      }

      if (dependent_rids.empty())
        continue;

      // --- Process dependent rows based on referential action ---
      if (fk.on_update_ == ReferentialAction::RESTRICT) {
        throw std::runtime_error(
            "Constraint Violation: Cannot update primary key in '" +
            parent_table->name_ + "'. Referenced by child table '" +
            child_meta->name_ + "'.");
      }

      if (fk.on_update_ == ReferentialAction::CASCADE ||
          fk.on_update_ == ReferentialAction::SET_NULL) {

        for (const RID &child_rid : dependent_rids) {
          if (!acquire_write_lock(child_rid)) {
            throw std::runtime_error("Transaction Aborted: Failed to acquire "
                                     "Exclusive Lock on Cascade Update.");
          }

          Tuple c_old_tuple;
          if (!child_meta->table_->GetTuple(child_rid, &c_old_tuple, txn))
            continue;

          std::vector<Value> c_new_values;
          for (uint32_t i = 0; i < child_meta->schema_.GetColumnCount(); ++i) {
            int tuple_fk_idx = -1;
            for (size_t j = 0; j < fk.child_key_attrs_.size(); j++) {
              if (fk.child_key_attrs_[j] == i) {
                tuple_fk_idx = static_cast<int>(j);
                break;
              }
            }

            if (tuple_fk_idx != -1) {
              if (fk.on_update_ == ReferentialAction::SET_NULL) {
                TypeId col_type = child_meta->schema_.GetColumn(i).GetTypeId();
                c_new_values.push_back(Value::GetNullValue(col_type));
              } else {
                // CASCADE
                Value new_parent_val = new_tuple.GetValue(
                    parent_schema, fk.parent_key_attrs_[tuple_fk_idx]);
                c_new_values.push_back(new_parent_val);
              }
            } else {
              c_new_values.push_back(
                  c_old_tuple.GetValue(&child_meta->schema_, i));
            }
          }

          Tuple c_new_tuple(c_new_values, &child_meta->schema_);
          RID c_new_rid = child_rid;

          if (child_meta->table_->UpdateTuple(c_new_tuple, &c_new_rid, txn, lock_mgr)) {
            if (c_new_rid != child_rid && !acquire_write_lock(c_new_rid)) {
              throw std::runtime_error("Transaction Aborted: Failed to acquire "
                                       "Exclusive Lock on relocated tuple.");
            }

            for (auto *c_idx : child_indexes) {
              std::vector<Column> c_key_cols;
              std::vector<Value> c_old_kvals, c_new_kvals;
              for (uint32_t ci : c_idx->key_attrs_) {
                c_key_cols.push_back(child_meta->schema_.GetColumn(ci));
                c_old_kvals.push_back(
                    c_old_tuple.GetValue(&child_meta->schema_, ci));
                c_new_kvals.push_back(
                    c_new_tuple.GetValue(&child_meta->schema_, ci));
              }
              Schema c_key_schema(c_key_cols);
              Tuple c_old_key_tuple(c_old_kvals, &c_key_schema);
              Tuple c_new_key_tuple(c_new_kvals, &c_key_schema);

              c_idx->index_->DeleteEntry(c_old_key_tuple, child_rid, txn);
              IndexWriteRecord c_idx_del(child_rid, WType::DELETE,
                                         c_old_key_tuple, c_idx->index_.get());
              txn->AppendIndexWriteRecord(c_idx_del);

              c_idx->index_->InsertEntry(c_new_key_tuple, c_new_rid, txn);
              IndexWriteRecord c_idx_ins(c_new_rid, WType::INSERT,
                                         c_new_key_tuple, c_idx->index_.get());
              txn->AppendIndexWriteRecord(c_idx_ins);
            }
          }
          // Recursive Cascade: propagate update down to grandchild tables
          EnforceOnUpdate(c_old_tuple, c_new_tuple, child_meta, catalog, txn,
                          acquire_write_lock, lock_mgr);
        }
      }
    }
  }
}

} // namespace tetodb
