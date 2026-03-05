// recovery_manager.cpp

#include "recovery/recovery_manager.h"
#include "storage/page/page_guard.h"
#include <cstring>
#include <iostream>
#include <vector>

namespace tetodb {

void RecoveryManager::Redo() {
  std::ifstream log_file(log_file_name_, std::ios::binary);
  if (!log_file.is_open()) {
    std::cout << "[RECOVERY] No log file found. Starting fresh." << std::endl;
    return;
  }

  std::cout << "[RECOVERY] Phase 1 & 2: Analysis and Redo Started..."
            << std::endl;
  uint32_t offset = 0;

  while (true) {
    // 1. Peek at the first 4 bytes (the size of the record)
    uint32_t record_size;
    char size_buf[sizeof(uint32_t)];
    if (!log_file.read(size_buf, sizeof(uint32_t))) {
      break; // Clean EOF
    }
    std::memcpy(&record_size, size_buf, sizeof(uint32_t));

    // ==========================================
    // FIX 1: PARTIAL CRASH WRITE PROTECTION
    // ==========================================
    // If the power was cut mid-write, the record size will be garbage.
    // Minimum log header is 20 bytes. Max is roughly PAGE_SIZE.
    if (record_size < 20 || record_size > PAGE_SIZE * 2) {
      std::cout << "[RECOVERY] Encountered partial/corrupted log record at end "
                   "of file. Halting Analysis."
                << std::endl;
      break;
    }

    std::vector<char> buffer(record_size);
    std::memcpy(buffer.data(), &record_size, sizeof(uint32_t));

    // If the payload is truncated, read will fail safely
    if (!log_file.read(buffer.data() + sizeof(uint32_t),
                       record_size - sizeof(uint32_t))) {
      std::cout << "[RECOVERY] Log payload truncated. Halting Analysis."
                << std::endl;
      break;
    }

    LogRecord log_record;
    log_record.Deserialize(buffer.data());

    lsn_mapping_[log_record.GetLSN()] = offset;
    did_work_ = true;

    if (log_record.GetTxnId() != INVALID_TRANSACTION_ID) {
      if (log_record.GetLogRecordType() == LogRecordType::COMMIT ||
          log_record.GetLogRecordType() == LogRecordType::ABORT) {
        active_txn_.erase(log_record.GetTxnId());
      } else {
        active_txn_[log_record.GetTxnId()] = log_record.GetLSN();
      }
    }

    RID rid = log_record.GetTargetRID();
    LogRecordType type = log_record.GetLogRecordType();

    if (type == LogRecordType::NEWPAGE) {
      page_id_t new_page_id = rid.GetPageId();
      page_id_t prev_page_id = log_record.GetPrevPageId();

      Page *new_page = bpm_->FetchPage(new_page_id);
      if (new_page != nullptr) {
        WritePageGuard guard(bpm_, new_page);
        guard.As<TablePage>()->Init(new_page_id, PAGE_SIZE, prev_page_id);
        guard.MarkDirty();
      }

      if (prev_page_id != INVALID_PAGE_ID) {
        Page *prev_page = bpm_->FetchPage(prev_page_id);
        if (prev_page != nullptr) {
          WritePageGuard guard(bpm_, prev_page);
          guard.As<TablePage>()->SetNextPageId(new_page_id);
          guard.MarkDirty();
        }
      }
    } else if (type == LogRecordType::INSERT ||
               type == LogRecordType::MARKDELETE ||
               type == LogRecordType::ROLLBACKDELETE) {

      Page *page = bpm_->FetchPage(rid.GetPageId());
      if (page != nullptr) {
        WritePageGuard guard(bpm_, page);
        auto table_page = guard.As<TablePage>();

        // Detect uninitialized pages: Init() sets FreeSpacePointer to
        // PAGE_SIZE (4096). An uninitialized page (all zeros from disk)
        // has FreeSpacePointer == 0. We cannot use GetNextPageId() == 0
        // because page_id 0 is a valid page.
        if (table_page->GetFreeSpacePointer() == 0) {
          table_page->Init(rid.GetPageId(), PAGE_SIZE);
        }

        if (table_page->GetLSN() >= log_record.GetLSN()) {
          // Skip stale log
        } else {
          if (type == LogRecordType::INSERT) {
            // Because we replay logs in exact chronological order,
            // standard insertion will perfectly recreate the original RIDs.
            RID temp_rid;
            table_page->InsertTuple(log_record.GetNewTuple(), &temp_rid);
          } else if (type == LogRecordType::MARKDELETE) {
            table_page->MarkDelete(rid);
          } else if (type == LogRecordType::ROLLBACKDELETE) {
            table_page->RollbackDelete(rid, log_record.GetNewTuple());
          }

          table_page->SetLSN(log_record.GetLSN());
          guard.MarkDirty();
        }
      }
    }
    offset += record_size;
  }

  log_file.close();
  std::cout << "[RECOVERY] Redo Complete." << std::endl;
  std::cout << "[RECOVERY] Found " << active_txn_.size()
            << " active (loser) transactions." << std::endl;
}

void RecoveryManager::Undo() {
  if (active_txn_.empty()) {
    std::cout << "[RECOVERY] No active transactions to undo. Database is clean."
              << std::endl;
    return;
  }

  std::cout << "[RECOVERY] Phase 3: Undo Started..." << std::endl;

  std::ifstream log_file(log_file_name_, std::ios::binary);

  for (auto const &[txn_id, last_lsn] : active_txn_) {
    lsn_t current_lsn = last_lsn;
    std::cout << " -> Rolling back incomplete Transaction " << txn_id
              << std::endl;

    while (current_lsn != INVALID_LSN) {
      auto offset_it = lsn_mapping_.find(current_lsn);
      if (offset_it == lsn_mapping_.end()) {
        std::cerr << "[RECOVERY ERROR] Missing LSN " << current_lsn
                  << " in mapping!" << std::endl;
        break;
      }

      uint32_t offset = offset_it->second;
      log_file.seekg(offset);
      uint32_t record_size;
      char size_buf[sizeof(uint32_t)];
      log_file.read(size_buf, sizeof(uint32_t));
      std::memcpy(&record_size, size_buf, sizeof(uint32_t));

      std::vector<char> buffer(record_size);
      std::memcpy(buffer.data(), &record_size, sizeof(uint32_t));
      log_file.read(buffer.data() + sizeof(uint32_t),
                    record_size - sizeof(uint32_t));

      LogRecord log_record;
      log_record.Deserialize(buffer.data());

      RID rid = log_record.GetTargetRID();
      LogRecordType type = log_record.GetLogRecordType();

      std::cout << "[UNDO TRACE] LSN: " << current_lsn
                << " | Type: " << static_cast<int>(type) << " | Target: Page "
                << rid.GetPageId() << ", Slot " << rid.GetSlotId() << std::endl;

      if (type == LogRecordType::INSERT) {
        Page *page = bpm_->FetchPage(rid.GetPageId());
        if (page != nullptr) {
          WritePageGuard guard(bpm_, page);
          auto table_page = guard.As<TablePage>();
          table_page->ApplyDelete(rid);
          guard.MarkDirty();
        }
      } else if (type == LogRecordType::MARKDELETE) {
        Page *page = bpm_->FetchPage(rid.GetPageId());
        if (page != nullptr) {
          WritePageGuard guard(bpm_, page);
          auto table_page = guard.As<TablePage>();
          table_page->RollbackDelete(rid, log_record.GetNewTuple());
          guard.MarkDirty();
        }
      } else if (type == LogRecordType::NEWPAGE) {
        // Do nothing. Structural changes are not logically undone.
      }

      current_lsn = log_record.GetPrevLSN();
    }
  }

  log_file.close();
  std::cout << "[RECOVERY] Undo Complete. Database is fully ACID compliant and "
               "ready for queries!"
            << std::endl;
}

} // namespace tetodb