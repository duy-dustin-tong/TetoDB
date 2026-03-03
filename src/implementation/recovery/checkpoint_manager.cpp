// checkpoint_manager.cpp

#include "recovery/checkpoint_manager.h"
#include <iostream>

namespace tetodb {

    void CheckpointManager::PerformCheckpoint() {
        // 1. Grab the global transaction latch
        std::unique_lock<std::shared_mutex> lock(txn_manager_->GetGlobalTxnLatch());

        // ==========================================
        // FIX: ENFORCE WRITE-AHEAD LOGGING (WAL)
        // ==========================================
        // You MUST flush the log buffer to disk BEFORE flushing the pages.
        // If the engine crashes after pages are written but before logs are saved,
        // uncommitted data is permanently stranded on disk.
        log_manager_->Flush();

        // 2. Now it is safe to flush pages to the physical disk
        bpm_->FlushAllPages();

        // 3. Write the CHECKPOINT record
        LogRecord checkpoint_record(INVALID_TRANSACTION_ID, INVALID_LSN, LogRecordType::CHECKPOINT);
        log_manager_->AppendLogRecord(&checkpoint_record);
        log_manager_->Flush();

        lock.unlock();
    }

} // namespace tetodb