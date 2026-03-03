// disk_manager.cpp

#include <iostream>
#include <stdexcept>
#include <cstring>
#include <vector>
#include "storage/disk/disk_manager.h"

namespace tetodb {

    DiskManager::DiskManager(std::filesystem::path db_file)
        : file_name_(std::move(db_file))
    {
        // ==========================================
        // 1. SETUP MAIN DATABASE FILE (.db)
        // ==========================================
        if (!std::filesystem::exists(file_name_)) {
            std::ofstream out(file_name_, std::ios::binary);
            out.close();
        }

        db_io_.rdbuf()->pubsetbuf(nullptr, 0);
        db_io_.open(file_name_, std::ios::binary | std::ios::in | std::ios::out);

        if (!db_io_.is_open()) {
            throw std::runtime_error("Failed to open db file");
        }

        next_page_id_ = static_cast<page_id_t>(GetFileSize() / PAGE_SIZE);

        // ==========================================
        // 2. SETUP SEQUENTIAL LOG FILE (.log)
        // ==========================================
        log_name_ = file_name_;
        log_name_.replace_extension(".log");

        if (!std::filesystem::exists(log_name_)) {
            std::ofstream out(log_name_, std::ios::binary);
            out.close();
        }

        log_io_.rdbuf()->pubsetbuf(nullptr, 0); // Disable buffering for safety
        // Notice the ios::app flag! This ensures all writes go to the absolute end of the file.
        log_io_.open(log_name_, std::ios::binary | std::ios::in | std::ios::out | std::ios::app);

        if (!log_io_.is_open()) {
            throw std::runtime_error("Failed to open log file");
        }
    }

    void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
        std::scoped_lock<std::mutex> lock(latch_);

        db_io_.clear();
        size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

        db_io_.seekp(offset);
        db_io_.write(page_data, PAGE_SIZE);
        db_io_.flush();

        if (db_io_.fail()) {
            std::cerr << "[DISK ERROR] Write/Flush failed for Page " << page_id << std::endl;
        }
    }

    void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
        std::scoped_lock<std::mutex> lock(latch_);

        db_io_.clear();
        size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

        db_io_.seekg(offset);
        db_io_.read(page_data, PAGE_SIZE);

        if (db_io_.bad()) {
            std::cerr << "[DISK ERROR] Read failed for Page " << page_id << std::endl;
        }

        int32_t cnt = db_io_.gcount();
        if (cnt < PAGE_SIZE) {
            std::fill(page_data + cnt, page_data + PAGE_SIZE, 0);
        }
    }

    // NEW: Append-only log writing
    void DiskManager::WriteLog(const char* log_data, int size) {
        std::scoped_lock<std::mutex> lock(latch_);

        log_io_.clear();

        // Write the chunk of memory
        log_io_.write(log_data, size);

        // Force it immediately to disk so we don't lose it in an OS crash
        log_io_.flush();

        if (log_io_.fail()) {
            std::cerr << "[DISK ERROR] Write/Flush failed for Log" << std::endl;
        }
    }

    page_id_t DiskManager::AllocatePage() {
        std::scoped_lock<std::mutex> lock(latch_);
        if (!free_list_.empty()) {
            page_id_t id = free_list_.top();
            free_list_.pop();
            return id;
        }
        return next_page_id_++;
    }

    void DiskManager::DeallocatePage(page_id_t page_id) {
        std::scoped_lock<std::mutex> lock(latch_);
        free_list_.push(page_id);
    }

} // namespace tetodb