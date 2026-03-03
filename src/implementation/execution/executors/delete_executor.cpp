// delete_executor.cpp

#include "execution/executors/delete_executor.h"
#include "execution/execution_context.h"
#include <stdexcept>

namespace tetodb {

    // Clean Constructor!
    DeleteExecutor::DeleteExecutor(ExecutionContext* exec_ctx,
        const DeletePlanNode* plan,
        std::unique_ptr<AbstractExecutor> child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
    }

    void DeleteExecutor::Init() {
        // Read OID directly from the plan!
        table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
        table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableOid());
        child_executor_->Init();
    }

    bool DeleteExecutor::Next(Tuple* tuple, RID* rid) {
        Tuple to_delete;
        RID delete_rid;

        // 1. Get the next tuple from the child executor (e.g., SeqScan)
        if (!child_executor_->Next(&to_delete, &delete_rid)) {
            return false;
        }

        Transaction* txn = exec_ctx_->GetTransaction();
        Catalog* catalog = exec_ctx_->GetCatalog();
        LockManager* lock_mgr = exec_ctx_->GetLockManager();

        // --- Smart Lock Router ---
        auto acquire_write_lock = [&](const RID& target_rid) -> bool {
            if (txn->GetExclusiveLockSet()->find(target_rid) != txn->GetExclusiveLockSet()->end()) {
                return true;
            }
            if (txn->GetSharedLockSet()->find(target_rid) != txn->GetSharedLockSet()->end()) {
                return lock_mgr->LockUpgrade(txn, target_rid);
            }
            return lock_mgr->LockExclusive(txn, target_rid);
            };

        // ==========================================
        // 2. PARENT TABLE: ENFORCE ON DELETE CASCADE/RESTRICT
        // ==========================================
        auto all_tables = catalog->GetAllTables();
        for (TableMetadata* child_meta : all_tables) {
            for (const auto& fk : child_meta->foreign_keys_) {
                if (fk.parent_table_oid_ == table_info_->oid_) {

                    std::vector<RID> dependent_rids;
                    auto child_indexes = catalog->GetTableIndexes(child_meta->oid_);

                    // --- OPTIMIZATION: Check for an available Index on the FK ---
                    IndexMetadata* fk_index = nullptr;
                    for (auto* idx : child_indexes) {
                        // If the index columns exactly match the foreign key columns
                        if (idx->key_attrs_ == fk.child_key_attrs_) {
                            fk_index = idx;
                            break;
                        }
                    }

                    if (fk_index) {
                        // FAST PATH: O(log N) Index Scan
                        std::vector<Column> key_cols;
                        std::vector<Value> key_vals;
                        for (uint32_t col_idx : fk.parent_key_attrs_) {
                            key_cols.push_back(child_meta->schema_.GetColumn(fk.child_key_attrs_[0]));
                            key_vals.push_back(to_delete.GetValue(&table_info_->schema_, col_idx));
                        }

                        Schema key_schema(key_cols);
                        Tuple search_key(key_vals, &key_schema);

                        fk_index->index_->ScanKey(search_key, &dependent_rids, txn);
                    }
                    else {
                        // SLOW PATH: O(N) Sequential Heap Scan
                        Value parent_val = to_delete.GetValue(&table_info_->schema_, fk.parent_key_attrs_[0]);
                        auto child_iter = child_meta->table_->Begin(txn);

                        while (child_iter != child_meta->table_->End()) {
                            RID child_rid = child_iter.GetRid();
                            Tuple child_tuple;

                            if (child_meta->table_->GetTuple(child_rid, &child_tuple, txn)) {
                                Value child_val = child_tuple.GetValue(&child_meta->schema_, fk.child_key_attrs_[0]);
                                if (parent_val.CompareEquals(child_val)) {
                                    dependent_rids.push_back(child_rid);
                                }
                            }
                            ++child_iter;
                        }
                    }

                    // --- PROCESS DEPENDENT ROWS ---
                    if (!dependent_rids.empty()) {
                        if (fk.on_delete_ == ReferentialAction::RESTRICT) {
                            throw std::runtime_error("Constraint Violation: Cannot delete row from '" + table_info_->name_ +
                                "'. Referenced by child table '" + child_meta->name_ + "'.");
                        }
                        else if (fk.on_delete_ == ReferentialAction::CASCADE) {
                            for (const RID& child_rid : dependent_rids) {

                                if (!acquire_write_lock(child_rid)) {
                                    throw std::runtime_error("Transaction Aborted: Failed to acquire Exclusive Lock on Cascade Delete.");
                                }

                                Tuple child_tuple;
                                if (!child_meta->table_->GetTuple(child_rid, &child_tuple, txn)) continue;

                                // Physical Deletion & WAL logging
                                child_meta->table_->MarkDelete(child_rid, txn);
                                TableWriteRecord child_write(child_rid, WType::DELETE, child_tuple, child_meta->table_.get());
                                txn->AppendTableWriteRecord(child_write);

                                // Update ALL child indexes
                                for (auto* child_idx : child_indexes) {
                                    std::vector<Column> child_key_cols;
                                    std::vector<Value> child_key_vals;

                                    for (uint32_t c_idx : child_idx->key_attrs_) {
                                        child_key_cols.push_back(child_meta->schema_.GetColumn(c_idx));
                                        child_key_vals.push_back(child_tuple.GetValue(&child_meta->schema_, c_idx));
                                    }

                                    Schema child_key_schema(child_key_cols);
                                    Tuple child_key_tuple(child_key_vals, &child_key_schema);

                                    child_idx->index_->DeleteEntry(child_key_tuple, child_rid, txn);

                                    IndexWriteRecord idx_rec(child_rid, WType::DELETE, child_key_tuple, child_idx->index_.get());
                                    txn->AppendIndexWriteRecord(idx_rec);
                                }
                            }
                        }
                        else if (fk.on_delete_ == ReferentialAction::SET_NULL) {
                            for (const RID& child_rid : dependent_rids) {
                                if (!acquire_write_lock(child_rid)) {
                                    throw std::runtime_error("Transaction Aborted: Failed to acquire Exclusive Lock on SET NULL.");
                                }

                                Tuple c_old_tuple;
                                if (!child_meta->table_->GetTuple(child_rid, &c_old_tuple, txn)) continue;

                                std::vector<Value> c_new_values;
                                for (uint32_t i = 0; i < child_meta->schema_.GetColumnCount(); ++i) {
                                    if (i == fk.child_key_attrs_[0]) {
                                        // Optional: Check if (!child_meta->schema_.GetColumn(i).IsNullable()) throw error
                                        TypeId col_type = child_meta->schema_.GetColumn(i).GetTypeId();
                                        c_new_values.push_back(Value::GetNullValue(col_type)); // Construct Explicit NULL
                                    }
                                    else {
                                        c_new_values.push_back(c_old_tuple.GetValue(&child_meta->schema_, i));
                                    }
                                }
                                Tuple c_new_tuple(c_new_values, &child_meta->schema_);
                                RID c_new_rid = child_rid;

                                if (child_meta->table_->UpdateTuple(c_new_tuple, &c_new_rid, txn)) {
                                    TableWriteRecord c_write(c_new_rid, WType::UPDATE, c_old_tuple, child_meta->table_.get());
                                    txn->AppendTableWriteRecord(c_write);

                                    for (auto* c_idx : child_indexes) {
                                        std::vector<Column> c_key_cols;
                                        std::vector<Value> c_old_kvals, c_new_kvals;
                                        for (uint32_t ci : c_idx->key_attrs_) {
                                            c_key_cols.push_back(child_meta->schema_.GetColumn(ci));
                                            c_old_kvals.push_back(c_old_tuple.GetValue(&child_meta->schema_, ci));
                                            c_new_kvals.push_back(c_new_tuple.GetValue(&child_meta->schema_, ci));
                                        }
                                        Schema c_key_schema(c_key_cols);
                                        Tuple c_old_key_tuple(c_old_kvals, &c_key_schema);
                                        Tuple c_new_key_tuple(c_new_kvals, &c_key_schema);

                                        c_idx->index_->DeleteEntry(c_old_key_tuple, child_rid, txn);
                                        IndexWriteRecord c_idx_del(child_rid, WType::DELETE, c_old_key_tuple, c_idx->index_.get());
                                        txn->AppendIndexWriteRecord(c_idx_del);

                                        c_idx->index_->InsertEntry(c_new_key_tuple, c_new_rid, txn);
                                        IndexWriteRecord c_idx_ins(c_new_rid, WType::INSERT, c_new_key_tuple, c_idx->index_.get());
                                        txn->AppendIndexWriteRecord(c_idx_ins);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // ==========================================
        // 3. PHYSICAL DELETION OF TARGET ROW
        // ==========================================
        if (!acquire_write_lock(delete_rid)) {

            throw std::runtime_error("Transaction Aborted: Failed to acquire Exclusive Lock on Delete.");
        }

        if (table_info_->table_->MarkDelete(delete_rid, txn)) {
            TableWriteRecord write_record(delete_rid, WType::DELETE, to_delete, table_info_->table_.get());
            txn->AppendTableWriteRecord(write_record);

            for (IndexMetadata* index_info : table_indexes_) {
                std::vector<Column> key_cols;
                std::vector<Value> key_values;

                for (uint32_t col_idx : index_info->key_attrs_) {
                    key_cols.push_back(table_info_->schema_.GetColumn(col_idx));
                    key_values.push_back(to_delete.GetValue(&table_info_->schema_, col_idx));
                }

                Schema key_schema(key_cols);
                Tuple key_tuple(key_values, &key_schema);

                index_info->index_->DeleteEntry(key_tuple, delete_rid, txn);

                IndexWriteRecord idx_record(delete_rid, WType::DELETE, key_tuple, index_info->index_.get());
                txn->AppendIndexWriteRecord(idx_record);
            }

            *tuple = to_delete;
            *rid = delete_rid;
            return true;
        }

        return false;
    }

    const Schema* DeleteExecutor::GetOutputSchema() {
        return &table_info_->schema_;
    }

} // namespace tetodb