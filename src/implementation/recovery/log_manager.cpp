// log_manager.cpp

#include "recovery/log_manager.h"
#include <cstring>
#include <cassert>

namespace tetodb {

    LogManager::LogManager(DiskManager* disk_manager)
        : disk_manager_(disk_manager) {
        log_buffer_ = new char[LOG_BUFFER_SIZE];
        flush_buffer_ = new char[LOG_BUFFER_SIZE];
    }

    LogManager::~LogManager() {
        StopFlushThread();
        delete[] log_buffer_;
        delete[] flush_buffer_;
    }

    lsn_t LogManager::AppendLogRecord(LogRecord* log_record) {
        std::unique_lock<std::mutex> lock(latch_);

        lsn_t lsn = next_lsn_++;
        log_record->SetLSN(lsn);

        uint32_t size = LogRecord::CalculateSize(log_record->GetLogRecordType(),
            log_record->GetOldTuple(),
            log_record->GetNewTuple());
        log_record->SetSize(size);

        assert(size <= LOG_BUFFER_SIZE && "Log record is too large for the buffer!");

        // If active buffer is full, wake the flush thread and wait
        while (log_buffer_offset_ + size >= LOG_BUFFER_SIZE) {
            cv_.notify_one();
            append_cv_.wait(lock);
        }

        uint32_t bytes_written = log_record->Serialize(log_buffer_ + log_buffer_offset_);
        assert(bytes_written == size);

        log_buffer_offset_ += bytes_written;

        return lsn;
    }

    void LogManager::Flush() {
        std::unique_lock<std::mutex> lock(latch_);

        // FIX: A while loop prevents spurious wakeups and guarantees 
        // the thread cannot exit until the data has physically cleared both RAM buffers.
        while (log_buffer_offset_ > 0 || flush_buffer_offset_ > 0) {

            // Wake the background thread to handle the flush
            cv_.notify_one();

            // Block the executor until the disk I/O is physically finished
            flush_cv_.wait(lock);
        }
    }

    void LogManager::RunFlushThread() {
        if (enable_logging_) return;
        enable_logging_ = true;

        flush_thread_ = new std::thread([&]() {
            while (enable_logging_) {
                std::unique_lock<std::mutex> lock(latch_);

                // 1. Sleep until timeout (30ms Group Commit) OR buffer has data and is triggered
                cv_.wait_for(lock, std::chrono::milliseconds(30), [&]() {
                    return log_buffer_offset_ > 0 || !enable_logging_;
                    });

                if (log_buffer_offset_ > 0) {
                    // 2. SWAP THE BUFFERS!
                    std::swap(log_buffer_, flush_buffer_);
                    flush_buffer_offset_ = log_buffer_offset_;
                    log_buffer_offset_ = 0;

                    // Wake up any threads blocked in AppendLogRecord
                    append_cv_.notify_all();

                    // 3. WRITE TO DISK (Drop the lock so executors can append to the new active buffer!)
                    lock.unlock();

                    disk_manager_->WriteLog(flush_buffer_, flush_buffer_offset_);

                    lock.lock();
                    flush_buffer_offset_ = 0;

                    // 4. FIX: Notify threads blocked in Flush() that data is safely on disk
                    flush_cv_.notify_all();
                }
            }
            });
    }

    void LogManager::StopFlushThread() {
        if (!enable_logging_) return;

        Flush(); // Flush anything left in the active buffer

        enable_logging_ = false;
        cv_.notify_all(); // Wake the thread to exit the loop

        if (flush_thread_ != nullptr && flush_thread_->joinable()) {
            flush_thread_->join();
            delete flush_thread_;
            flush_thread_ = nullptr;
        }
    }

} // namespace tetodb