// log_manager.h

#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "recovery/log_record.h"
#include "storage/disk/disk_manager.h" 

namespace tetodb {

    constexpr uint32_t LOG_BUFFER_SIZE = 32 * 1024;

    class LogManager {
    public:
        explicit LogManager(DiskManager* disk_manager);
        ~LogManager();

        lsn_t AppendLogRecord(LogRecord* log_record);
        void Flush();
        void RunFlushThread();
        void StopFlushThread();

    private:
        DiskManager* disk_manager_;

        std::atomic<lsn_t> next_lsn_{ 0 };
        std::atomic<lsn_t> persistent_lsn_{ INVALID_LSN };

        char* log_buffer_;
        char* flush_buffer_;
        uint32_t log_buffer_offset_{ 0 };
        uint32_t flush_buffer_offset_{ 0 };

        std::mutex latch_;
        std::condition_variable cv_;         // Wakes the background thread
        std::condition_variable append_cv_;  // Blocks appending threads if active buffer is full
        std::condition_variable flush_cv_;   // FIX: Blocks committing threads until disk I/O finishes

        std::thread* flush_thread_{ nullptr };
        std::atomic<bool> enable_logging_{ false };
    };

} // namespace tetodb