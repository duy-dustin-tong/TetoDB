// transaction_manager.cpp

#include "concurrency/transaction_manager.h"
#include "index/index.h"
#include "storage/table/table_heap.h"
#include <iostream>

namespace tetodb {

Transaction *TransactionManager::Begin(IsolationLevel isolation_level) {
  txn_id_t txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  txn->SetState(TransactionState::GROWING);

  // ==========================================================
  // WAL: LOG THE BEGIN RECORD
  // ==========================================================
  if (log_manager_ != nullptr) {
    LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(),
                         LogRecordType::BEGIN);
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
    txn->SetPrevLSN(lsn);
  }

  std::lock_guard<std::mutex> lock(map_mutex_);
  Transaction *ptr = txn.get();
  txn_map_[txn_id] = std::move(txn);

  return ptr;
}

void TransactionManager::Commit(Transaction *txn) {
  if (txn->GetState() == TransactionState::ABORTED) {
    Abort(txn);
    return;
  }

  // ==========================================================
  // WAL: LOG THE COMMIT RECORD AND FLUSH TO DISK
  // ==========================================================
  if (log_manager_ != nullptr) {
    LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(),
                         LogRecordType::COMMIT);
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
    txn->SetPrevLSN(lsn);

    // CRITICAL: Block until the background thread writes this to the physical
    // file!
    log_manager_->Flush();
  }

  txn->SetState(TransactionState::COMMITTED);

  ReleaseLocks(txn);
  GarbageCollect(txn->GetTransactionId());
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);

  // ==========================================================
  // 1. UNDO INDEX OPERATIONS (LIFO)
  // ==========================================================
  auto index_write_set = txn->GetIndexWriteSet();
  for (auto it = index_write_set->rbegin(); it != index_write_set->rend();
       ++it) {
    auto &record = *it;
    if (record.wtype_ == WType::INSERT) {
      // Undo an insertion by deleting the key
      record.index_->DeleteEntry(record.tuple_, record.rid_, txn);
    } else if (record.wtype_ == WType::DELETE) {
      // Undo a deletion by re-inserting the key
      record.index_->InsertEntry(record.tuple_, record.rid_, txn);
    }
  }

  // ==========================================================
  // 2. UNDO TABLE HEAP OPERATIONS (LIFO)
  // ==========================================================

  auto write_set = txn->GetWriteSet();

  // We MUST undo operations in reverse order (LIFO)
  for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
    auto &record = *it;
    if (record.wtype_ == WType::INSERT) {
      record.table_heap_->MarkDelete(record.rid_, txn);
    } else if (record.wtype_ == WType::DELETE) {
      record.table_heap_->RollbackDelete(record.rid_, record.tuple_, txn);
    } else if (record.wtype_ == WType::UPDATE) {
      // --- CORE FIX: Destroy the uncommitted new tuple ---
      record.table_heap_->MarkDelete(record.rid_, txn);

      // --- CORE FIX: Revive the original tuple at its original location ---
      record.table_heap_->RollbackDelete(record.tuple_.GetRid(), record.tuple_,
                                         txn);
    }
  }

  // ==========================================================
  // WAL: LOG THE ABORT RECORD
  // ==========================================================
  if (log_manager_ != nullptr) {
    LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(),
                         LogRecordType::ABORT);
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
    txn->SetPrevLSN(lsn);
  }

  ReleaseLocks(txn);
  GarbageCollect(txn->GetTransactionId());
}

void TransactionManager::ReleaseLocks(Transaction *txn) {
  auto exclusive_set = txn->GetExclusiveLockSet();
  while (!exclusive_set->empty()) {
    RID rid = *exclusive_set->begin();
    lock_manager_->Unlock(txn, rid);
  }

  auto shared_set = txn->GetSharedLockSet();
  while (!shared_set->empty()) {
    RID rid = *shared_set->begin();
    lock_manager_->Unlock(txn, rid);
  }
}

void TransactionManager::GarbageCollect(txn_id_t txn_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  txn_map_.erase(txn_id);
}

} // namespace tetodb