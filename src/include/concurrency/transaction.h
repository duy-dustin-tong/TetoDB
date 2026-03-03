// transaction.h

#pragma once

#include <vector>
#include <memory>
#include <unordered_set>
#include <atomic>
#include <list> // Needed for std::list

#include "common/config.h"
#include "storage/page/page.h"
#include "common/rwlatch.h" 
#include "common/record_id.h" 
#include "concurrency/table_write_record.h"
#include "concurrency/index_write_record.h"

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

    class Transaction {
    public:
        // Update Constructor to take Isolation Level (Default to REPEATABLE_READ)
        explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ)
            : txn_id_(txn_id),
            isolation_level_(isolation_level),
            state_(TransactionState::GROWING), // Start in Growing phase
            shared_lock_set_(std::make_shared<std::unordered_set<RID>>()),
            exclusive_lock_set_(std::make_shared<std::unordered_set<RID>>()) {

            // Reserve space for global latches (usually just 1 or 2)
            locked_latches_->reserve(2);
        }

        ~Transaction() = default;

        // --- GETTERS & SETTERS ---
        inline txn_id_t GetTransactionId() const { return txn_id_; }

        inline void SetState(TransactionState state) { state_ = state; }
        inline TransactionState GetState() const { return state_; }

        inline void SetIsolationLevel(IsolationLevel level) { isolation_level_ = level; }
        inline IsolationLevel GetIsolationLevel() const { return isolation_level_; }

        // --- WAL (Write-Ahead Logging) LSN TRACKING ---
        // -1 represents an INVALID_LSN (no logs written yet)
        inline int32_t GetPrevLSN() const { return prev_lsn_; }
        inline void SetPrevLSN(int32_t prev_lsn) { prev_lsn_ = prev_lsn; }

        // --- PAGE SET (Leaf/Internal Nodes) ---
        inline void AddIntoPageSet(Page* page) { page_set_->push_back(page); }
        inline std::shared_ptr<std::vector<Page*>> GetPageSet() { return page_set_; }
        inline void ClearPageSet() { page_set_->clear(); }

        // --- GENERIC LATCH SET (Global Locks) ---
        inline void AddLockedLatch(ReaderWriterLatch* latch) { locked_latches_->push_back(latch); }
        inline std::shared_ptr<std::vector<ReaderWriterLatch*>> GetLockedLatches() { return locked_latches_; }

        // --- DELETED PAGE SET ---
        inline void AddIntoDeletedPageSet(page_id_t page_id) { deleted_page_set_->insert(page_id); }
        inline std::shared_ptr<std::unordered_set<page_id_t>> GetDeletedPageSet() { return deleted_page_set_; }
        inline void ClearDeletedPageSet() { deleted_page_set_->clear(); }

        // --- LOCK SETS (For Lock Manager) ---
        inline std::shared_ptr<std::unordered_set<RID>> GetSharedLockSet() { return shared_lock_set_; }
        inline std::shared_ptr<std::unordered_set<RID>> GetExclusiveLockSet() { return exclusive_lock_set_; }

        inline std::shared_ptr<std::list<IndexWriteRecord>> GetIndexWriteSet() { return index_write_set_; }
        inline std::shared_ptr<std::list<TableWriteRecord>> GetWriteSet() { return table_write_set_; }

        inline void AppendTableWriteRecord(const TableWriteRecord& write_record) {
            table_write_set_->push_back(write_record);
        }

        inline void AppendIndexWriteRecord(const IndexWriteRecord& write_record) {
            index_write_set_->push_back(write_record);
        }

    private:
        txn_id_t txn_id_;

        // --- WAL FIELDS ---
        int32_t prev_lsn_{ -1 };

        // --- EXISTING FIELDS ---
        IsolationLevel isolation_level_;
        std::atomic<TransactionState> state_;

        std::shared_ptr<std::vector<Page*>> page_set_ = std::make_shared<std::vector<Page*>>();
        std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_ = std::make_shared<std::unordered_set<page_id_t>>();

        std::shared_ptr<std::vector<ReaderWriterLatch*>> locked_latches_ = std::make_shared<std::vector<ReaderWriterLatch*>>();

        std::shared_ptr<std::unordered_set<RID>> shared_lock_set_;
        std::shared_ptr<std::unordered_set<RID>> exclusive_lock_set_;

        std::shared_ptr<std::list<TableWriteRecord>> table_write_set_ = std::make_shared<std::list<TableWriteRecord>>();
        std::shared_ptr<std::list<IndexWriteRecord>> index_write_set_ = std::make_shared<std::list<IndexWriteRecord>>();
    
    };

}  // namespace tetodb