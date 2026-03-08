// lock_manager.h

#pragma once

#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>


#include "common/record_id.h"
#include "concurrency/transaction.h"

namespace tetodb {

class TransactionManager;

/**
 * LockManager handles tuple-level locks.
 * It uses Two-Phase Locking (2PL) logic.
 */
class LockManager {
public:
  enum class LockMode { SHARED, EXCLUSIVE };

  LockManager() = default;
  ~LockManager() = default;

  /**
   * Acquire a lock on a specific RID.
   * If the lock is incompatible with existing locks, the thread BLOCKS (waits).
   * @return true if lock acquired, false if transaction aborted (deadlock).
   */
  virtual bool LockShared(Transaction *txn, const RID &rid);
  virtual bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a Shared lock to Exclusive.
   * Used when we read a tuple (S) and then decide to update it (X).
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release a lock on a specific RID.
   * Under 2PL/SS2PL, this might not actually unlock immediately if we are in
   * strict mode.
   */
  bool Unlock(Transaction *txn, const RID &rid);

private:
  std::mutex latch_; // Protects the lock table

  /**
   * LockRequest represents a transaction waiting for or holding a lock.
   */
  struct LockRequest {
    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_ = false;

    LockRequest(txn_id_t txn_id, LockMode lock_mode)
        : txn_id_(txn_id), lock_mode_(lock_mode) {}
  };

  /**
   * LockRequestQueue tracks all requests for a single RID.
   */
  struct LockRequestQueue {
    std::list<LockRequest> request_queue_;
    std::condition_variable cv_; // Wait here if you can't get the lock
    bool upgrading_ = false; // Prevent starvation: only one upgrade at a time
  };

  // The Big Hash Table: Map RID -> Queue of requests
  std::unordered_map<RID, LockRequestQueue> lock_table_;
};

} // namespace tetodb