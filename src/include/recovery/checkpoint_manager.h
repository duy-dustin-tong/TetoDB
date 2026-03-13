// checkpoint_manager.h

#pragma once

#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>

#include "storage/buffer/buffer_pool_manager.h"
#include "concurrency/transaction_manager.h"
#include "recovery/log_manager.h"

namespace tetodb {

    class CheckpointManager {
    public:
        CheckpointManager(TransactionManager* txn_manager, LogManager* log_manager, BufferPoolManager* bpm)
            : txn_manager_(txn_manager), log_manager_(log_manager), bpm_(bpm), enable_checkpointer_(false) {
        }

        ~CheckpointManager() {
            StopCheckpointer();
        }

        void StartCheckpointer(std::chrono::seconds interval) {
            enable_checkpointer_ = true;
            checkpointer_thread_ = std::thread([this, interval] {
                while (enable_checkpointer_) {
                    std::this_thread::sleep_for(interval);
                    if (enable_checkpointer_) {
                        PerformCheckpoint();
                    }
                }
                });
        }

        void StopCheckpointer() {
            if (enable_checkpointer_) {
                enable_checkpointer_ = false;
                if (checkpointer_thread_.joinable()) {
                    checkpointer_thread_.join();
                }
            }
        }

        void PerformCheckpoint();

    private:
        TransactionManager* txn_manager_;
        LogManager* log_manager_;
        BufferPoolManager* bpm_;

        std::thread checkpointer_thread_;
        std::atomic<bool> enable_checkpointer_;
    };

} // namespace tetodb