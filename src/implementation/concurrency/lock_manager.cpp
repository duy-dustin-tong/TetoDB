// lock_manager.cpp

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <chrono> // Added for the deadlock timeout

namespace tetodb {

    bool LockManager::LockShared(Transaction* txn, const RID& rid) {
        std::unique_lock<std::mutex> lock(latch_);

        // 1. Get/Create the queue for this RID
        LockRequestQueue& queue = lock_table_[rid];

        // 2. Check if we already have a lock (Reentrancy)
        for (auto& req : queue.request_queue_) {
            if (req.txn_id_ == txn->GetTransactionId()) {
                if (req.lock_mode_ == LockMode::SHARED || req.lock_mode_ == LockMode::EXCLUSIVE) {
                    return true; // Already have S or stronger
                }
            }
        }

        // 3. Add our request to the queue
        queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
        auto it = std::prev(queue.request_queue_.end()); // Iterator to our request

        // 4. WAIT LOOP
        while (true) {
            bool can_grant = true;
            for (auto curr = queue.request_queue_.begin(); curr != it; ++curr) {
                if (curr->lock_mode_ == LockMode::EXCLUSIVE) {
                    can_grant = false; // Writer ahead -> Wait
                    break;
                }
            }

            if (can_grant) {
                it->granted_ = true;
                txn->GetSharedLockSet()->emplace(rid); // Track that we hold this
                return true;
            }

            // TIMEOUT ADDED HERE
            // If we wait more than 50ms, assume deadlock, abort ourselves, and leave.
            if (queue.cv_.wait_for(lock, std::chrono::milliseconds(50)) == std::cv_status::timeout) {
                txn->SetState(TransactionState::ABORTED);
                queue.request_queue_.erase(it);
                queue.cv_.notify_all();
                return false;
            }

            // Check for Abort (e.g., killed by another thread manually)
            if (txn->GetState() == TransactionState::ABORTED) {
                queue.request_queue_.erase(it);
                queue.cv_.notify_all();
                return false;
            }
        }
    }

    bool LockManager::LockExclusive(Transaction* txn, const RID& rid) {
        std::unique_lock<std::mutex> lock(latch_);

        LockRequestQueue& queue = lock_table_[rid];

        // 1. Reentrancy check
        for (auto& req : queue.request_queue_) {
            if (req.txn_id_ == txn->GetTransactionId()) {
                if (req.lock_mode_ == LockMode::EXCLUSIVE) return true;
                return false;
            }
        }

        // 2. Add Request
        queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
        auto it = std::prev(queue.request_queue_.end());

        // 3. WAIT LOOP
        while (true) {
            bool is_first = (it == queue.request_queue_.begin());

            if (is_first) {
                it->granted_ = true;
                txn->GetExclusiveLockSet()->emplace(rid);
                return true;
            }

            // TIMEOUT ADDED HERE
            if (queue.cv_.wait_for(lock, std::chrono::milliseconds(50)) == std::cv_status::timeout) {
                txn->SetState(TransactionState::ABORTED);
                queue.request_queue_.erase(it);
                queue.cv_.notify_all();
                return false;
            }

            if (txn->GetState() == TransactionState::ABORTED) {
                queue.request_queue_.erase(it);
                queue.cv_.notify_all();
                return false;
            }
        }
    }

    bool LockManager::Unlock(Transaction* txn, const RID& rid) {
        std::unique_lock<std::mutex> lock(latch_);

        // 1. ALWAYS clear it from the Transaction's memory first to prevent infinite loops!
        txn->GetSharedLockSet()->erase(rid);
        txn->GetExclusiveLockSet()->erase(rid);

        // 2. Find the queue
        if (lock_table_.find(rid) == lock_table_.end()) return false;
        LockRequestQueue& queue = lock_table_[rid];

        // 3. Find our request and remove it
        bool found = false;
        for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
            if (it->txn_id_ == txn->GetTransactionId()) {
                queue.request_queue_.erase(it);
                found = true;
                break;
            }
        }

        if (!found) return false;

        // 4. Update Transaction State (Shrinking Phase of 2PL)
        if (txn->GetState() == TransactionState::GROWING) {
            txn->SetState(TransactionState::SHRINKING);
        }

        // 5. Wake up waiters
        queue.cv_.notify_all();
        return true;
    }

    bool LockManager::LockUpgrade(Transaction* txn, const RID& rid) {
        std::unique_lock<std::mutex> lock(latch_);
        LockRequestQueue& queue = lock_table_[rid];

        // 1. Check if we can upgrade
        if (queue.upgrading_) return false; // Only 1 upgrade allowed at a time

        // 2. Find our existing Shared lock
        auto it = queue.request_queue_.begin();
        for (; it != queue.request_queue_.end(); ++it) {
            if (it->txn_id_ == txn->GetTransactionId()) break;
        }

        // If we don't have a shared lock to begin with, we can't upgrade
        if (it == queue.request_queue_.end() || it->lock_mode_ != LockMode::SHARED) return false;

        // 3. Mark upgrading
        it->lock_mode_ = LockMode::EXCLUSIVE;
        it->granted_ = false;
        queue.upgrading_ = true;

        // 4. Wait until we are the ONLY one left holding a lock
        while (true) {
            bool can_upgrade = true;
            for (auto curr = queue.request_queue_.begin(); curr != queue.request_queue_.end(); ++curr) {
                // BUG FIX: We only care if someone else actually HOLDS a lock.
                // We don't care if they are just waiting in the queue behind us!
                if (curr->txn_id_ != txn->GetTransactionId() && curr->granted_) {
                    can_upgrade = false;
                    break;
                }
            }

            if (can_upgrade) {
                it->granted_ = true;
                queue.upgrading_ = false;
                txn->GetSharedLockSet()->erase(rid);
                txn->GetExclusiveLockSet()->emplace(rid);
                return true;
            }

            // 50ms Deadlock Timeout
            if (queue.cv_.wait_for(lock, std::chrono::milliseconds(50)) == std::cv_status::timeout) {
                txn->SetState(TransactionState::ABORTED);
                queue.upgrading_ = false;

                // Revert it back to a SHARED lock so the TransactionManager can unlock it normally during Abort().
                it->lock_mode_ = LockMode::SHARED;
                it->granted_ = true;

                queue.cv_.notify_all();
                return false;
            }

            // Manual Abort Check
            if (txn->GetState() == TransactionState::ABORTED) {
                queue.upgrading_ = false;

                // Revert to SHARED lock
                it->lock_mode_ = LockMode::SHARED;
                it->granted_ = true;

                queue.cv_.notify_all();
                return false;
            }
        }
    }

} // namespace tetodb