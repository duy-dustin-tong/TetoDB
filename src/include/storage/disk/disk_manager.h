// disk_manager.h

#pragma once

#include <fstream>
#include <string>
#include <filesystem>
#include <atomic>
#include <stack>
#include <mutex>
#include "common/config.h"

namespace tetodb {
    class DiskManager {
    public:
        explicit DiskManager(std::filesystem::path db_file);
        ~DiskManager() = default;

        void WritePage(page_id_t page_id, const char* page_data);
        void ReadPage(page_id_t page_id, char* page_data);

        // NEW: Write a raw byte buffer to the sequential log file
        void WriteLog(const char* log_data, int size);

        page_id_t AllocatePage();
        void DeallocatePage(page_id_t page_id);

        inline int32_t GetFileSize() {
            return std::filesystem::file_size(file_name_);
        }
    private:
        std::filesystem::path file_name_;
        std::fstream db_io_;

        // NEW: Dedicated path and stream for the log file
        std::filesystem::path log_name_;
        std::fstream log_io_;

        std::stack<page_id_t> free_list_;
        std::mutex latch_;

        std::atomic<page_id_t> next_page_id_;
    };

} // namespace tetodb