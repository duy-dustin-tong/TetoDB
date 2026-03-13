// transaction.h

#pragma once

#include <atomic>
#include <list>
#include <unordered_set>
#include <vector>


#include "common/config.h"
#include "common/record_id.h"
#include "common/rwlatch.h"
#include "concurrency/index_write_record.h"
#include "concurrency/table_write_record.h"
#include "storage/page/page.h"


namespace tetodb {

/**
 * Transaction States for 2PL:
 * GROWING:   Acquiring locks.
 * SHRINKING: Releasing locks.
 * COMMITTED: Successfully finished.
 * ABORTED:   Rolled back.
 */
enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };

/**
 * Isolation Levels:
 * READ_UNCOMMITTED: Dirty reads allowed.
 * REPEATABLE_READ:  No phantom reads (Default).
 * READ_COMMITTED:   No dirty reads.
 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED };

/**
 * Savepoint: captures write-set sizes at creation time.
 * ROLLBACK TO undoes operations back to these sizes.
 */
struct Savepoint {
  std::string name;
  size_t table_write_set_size;
  size_t index_write_set_size;
};

class Transaction {
public:
  explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level =
                                            IsolationLevel::REPEATABLE_READ)
      : txn_id_(txn_id), isolation_level_(isolation_level),
        state_(TransactionState::GROWING) {
    locked_latches_.reserve(2);
  }

  ~Transaction() = default;

  // --- GETTERS & SETTERS ---
  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline void SetState(TransactionState state) { state_ = state; }
  inline TransactionState GetState() const { return state_; }

  inline void SetIsolationLevel(IsolationLevel level) {
    isolation_level_ = level;
  }
  inline IsolationLevel GetIsolationLevel() const { return isolation_level_; }

  // --- WAL (Write-Ahead Logging) LSN TRACKING ---
  inline int32_t GetPrevLSN() const { return prev_lsn_; }
  inline void SetPrevLSN(int32_t prev_lsn) { prev_lsn_ = prev_lsn; }

  // --- PAGE SET (Leaf/Internal Nodes) ---
  inline void AddIntoPageSet(Page *page) { page_set_.push_back(page); }
  inline std::vector<Page *> *GetPageSet() { return &page_set_; }
  inline void ClearPageSet() { page_set_.clear(); }

  // --- GENERIC LATCH SET (Global Locks) ---
  inline void AddLockedLatch(ReaderWriterLatch *latch) {
    locked_latches_.push_back(latch);
  }
  inline std::vector<ReaderWriterLatch *> *GetLockedLatches() {
    return &locked_latches_;
  }

  // --- DELETED PAGE SET ---
  inline void AddIntoDeletedPageSet(page_id_t page_id) {
    deleted_page_set_.insert(page_id);
  }
  inline std::unordered_set<page_id_t> *GetDeletedPageSet() {
    return &deleted_page_set_;
  }
  inline void ClearDeletedPageSet() { deleted_page_set_.clear(); }

  // --- LOCK SETS (For Lock Manager) ---
  inline std::unordered_set<RID> *GetSharedLockSet() {
    return &shared_lock_set_;
  }
  inline std::unordered_set<RID> *GetExclusiveLockSet() {
    return &exclusive_lock_set_;
  }

  inline std::list<IndexWriteRecord> *GetIndexWriteSet() {
    return &index_write_set_;
  }
  inline std::list<TableWriteRecord> *GetWriteSet() {
    return &table_write_set_;
  }

  inline void AppendTableWriteRecord(const TableWriteRecord &write_record) {
    table_write_set_.push_back(write_record);
  }

  inline void AppendIndexWriteRecord(const IndexWriteRecord &write_record) {
    index_write_set_.push_back(write_record);
  }

  // --- SAVEPOINT SUPPORT ---
  void CreateSavepoint(const std::string &name) {
    savepoints_.push_back(
        {name, table_write_set_.size(), index_write_set_.size()});
  }

  /** Releases the named savepoint and all savepoints created after it.
   *  Returns true if found, false otherwise. */
  bool ReleaseSavepoint(const std::string &name) {
    for (auto it = savepoints_.begin(); it != savepoints_.end(); ++it) {
      if (it->name == name) {
        savepoints_.erase(it, savepoints_.end());
        return true;
      }
    }
    return false;
  }

  /** Finds the named savepoint. Returns nullptr if not found. */
  Savepoint *FindSavepoint(const std::string &name) {
    for (auto &sp : savepoints_) {
      if (sp.name == name)
        return &sp;
    }
    return nullptr;
  }

  std::vector<Savepoint> &GetSavepoints() { return savepoints_; }

private:
  txn_id_t txn_id_;

  // --- WAL FIELDS ---
  int32_t prev_lsn_{-1};

  // --- EXISTING FIELDS ---
  IsolationLevel isolation_level_;
  std::atomic<TransactionState> state_;

  std::vector<Page *> page_set_;
  std::unordered_set<page_id_t> deleted_page_set_;
  std::vector<ReaderWriterLatch *> locked_latches_;

  std::unordered_set<RID> shared_lock_set_;
  std::unordered_set<RID> exclusive_lock_set_;

  std::list<TableWriteRecord> table_write_set_;
  std::list<IndexWriteRecord> index_write_set_;

  // --- SAVEPOINT STACK ---
  std::vector<Savepoint> savepoints_;
};

} // namespace tetodb