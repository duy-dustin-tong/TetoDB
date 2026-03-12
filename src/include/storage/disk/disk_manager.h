// disk_manager.h

#pragma once

#include "common/config.h"
#include <atomic>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <stack>
#include <string>


namespace tetodb {
class DiskManager {
public:
  explicit DiskManager(std::filesystem::path db_file);
  ~DiskManager();
  bool WritePage(page_id_t page_id, const char *page_data);
  void ReadPage(page_id_t page_id, char *page_data);

  // NEW: Write a raw byte buffer to the sequential log file
  void WriteLog(const char *log_data, int size);

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

  // ================================================================
  // LOCK ORDERING CONTRACT
  // ================================================================
  // External callers (BPM) hold their own latch BEFORE calling into
  // DiskManager. To prevent deadlocks, DiskManager uses three
  // INDEPENDENT mutexes that never depend on each other:
  //
  //   io_latch_    — protects db_io_   (ReadPage / WritePage)
  //   log_latch_   — protects log_io_  (WriteLog)
  //   alloc_latch_ — protects free_list_ (AllocatePage / DeallocatePage)
  //
  // None of these locks are ever nested. The allowed acquisition
  // order from any external caller is:
  //   BPM::latch_ → alloc_latch_   (NewPage, DeletePage)
  //   BPM::latch_ → io_latch_      (eviction writeback, FetchPage read)
  //   (independent) → log_latch_   (WAL writes from LogManager)
  // ================================================================
  std::mutex io_latch_;
  std::mutex log_latch_;
  std::mutex alloc_latch_;

  std::atomic<page_id_t> next_page_id_;
};

} // namespace tetodb