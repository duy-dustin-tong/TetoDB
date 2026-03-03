// transaction_manager.h

#pragma once

#include <atomic>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h" // <-- NEW: Include LogManager

namespace tetodb {

    class TransactionManager {
    public:
        // NEW: Inject the LogManager into the constructor
        explicit TransactionManager(LockManager* lock_manager, LogManager* log_manager = nullptr)
            : lock_manager_(lock_manager), log_manager_(log_manager) {
        }

        ~TransactionManager() = default;

        inline std::shared_mutex& GetGlobalTxnLatch() { return global_txn_latch_; }

        Transaction* Begin(IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ);
        void Commit(Transaction* txn);
        void Abort(Transaction* txn);
        void GarbageCollect(txn_id_t txn_id);

    private:
        void ReleaseLocks(Transaction* txn);

        std::shared_mutex global_txn_latch_;
        std::atomic<txn_id_t> next_txn_id_{ 0 };
        LockManager* lock_manager_;
        LogManager* log_manager_; // <-- NEW: The LogManager pointer

        std::mutex map_mutex_;
        std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
    };

} // namespace tetodb