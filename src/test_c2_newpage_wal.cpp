// test_c2_newpage_wal.cpp
// Test for C2 fix: NEWPAGE WAL log must record the correct prev_page_id.
//
// Scenario:
// 1. Create a table heap (allocates first page, last_page_id_ = first_page_id_).
// 2. Fill the first page so the next INSERT allocates a new page.
// 3. Insert a tuple that triggers new page allocation.
// 4. Flush the WAL, read back the log, find the NEWPAGE record.
// 5. Verify that the NEWPAGE record's prev_page_id == first_page_id_
//    (NOT new_page_id, which was the bug).

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "recovery/log_manager.h"
#include "recovery/log_record.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include "storage/disk/disk_manager.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using namespace tetodb;

int main() {
  std::cout << "=== C2 Fix: NEWPAGE WAL prev_page_id ===" << std::endl;

  const std::string db_path = "test_c2_wal.db";
  const std::string log_path = "test_c2_wal.log";
  const std::string freelist_path = "test_c2_wal.freelist";

  // Cleanup
  std::filesystem::remove(db_path);
  std::filesystem::remove(log_path);
  std::filesystem::remove(freelist_path);

  auto disk_mgr = std::make_unique<DiskManager>(db_path);
  auto replacer = std::make_unique<TwoQueueReplacer>(50);
  auto bpm =
      std::make_unique<BufferPoolManager>(50, disk_mgr.get(), replacer.get());
  auto log_mgr = std::make_unique<LogManager>(disk_mgr.get());
  auto lock_mgr = std::make_unique<LockManager>();
  auto txn_mgr =
      std::make_unique<TransactionManager>(lock_mgr.get(), log_mgr.get());

  log_mgr->RunFlushThread();

  Transaction *txn = txn_mgr->Begin();

  // Create table heap — allocates first page
  TableHeap heap(bpm.get(), log_mgr.get(), txn);
  page_id_t first_page_id = heap.GetFirstPageId();
  std::cout << "First page ID: " << first_page_id << std::endl;

  // Schema: single large VARCHAR column to fill page quickly
  std::vector<Column> cols = {Column("data", TypeId::VARCHAR, 2000)};
  Schema schema(cols);

  // Fill the first page with large tuples until it's full
  int inserted_on_first_page = 0;
  while (true) {
    Tuple tuple({Value(TypeId::VARCHAR, std::string(1800, 'X'))}, &schema);
    RID rid;
    bool ok = heap.InsertTuple(tuple, &rid, txn, lock_mgr.get());
    if (!ok)
      break;
    if (rid.GetPageId() != first_page_id) {
      // This insert already went to a new page — we're done
      inserted_on_first_page = -1;
      break;
    }
    inserted_on_first_page++;
  }

  std::cout << "Inserted " << inserted_on_first_page
            << " tuples on first page before it was full." << std::endl;

  // Now insert one more — this MUST trigger a new page allocation
  Tuple overflow_tuple({Value(TypeId::VARCHAR, std::string(1800, 'Y'))},
                       &schema);
  RID overflow_rid;
  bool ok = heap.InsertTuple(overflow_tuple, &overflow_rid, txn, lock_mgr.get());
  assert(ok && "Overflow insert must succeed on a new page");

  page_id_t new_page_id = overflow_rid.GetPageId();
  std::cout << "Overflow tuple landed on page ID: " << new_page_id << std::endl;
  assert(new_page_id != first_page_id &&
         "Overflow must be on a different page");

  // Flush WAL to disk
  log_mgr->Flush();

  // Now read the raw log file and find NEWPAGE records
  std::ifstream log_file(log_path, std::ios::binary);
  assert(log_file.is_open() && "Must be able to read log file");

  bool found_newpage = false;
  page_id_t logged_prev_page_id = INVALID_PAGE_ID;
  page_id_t logged_new_page_id = INVALID_PAGE_ID;

  while (log_file.peek() != EOF) {
    LogRecord record;
    // Read size first
    uint32_t size;
    log_file.read(reinterpret_cast<char *>(&size), sizeof(uint32_t));
    if (log_file.eof() || size == 0)
      break;

    // Seek back to read full record
    log_file.seekg(-static_cast<int>(sizeof(uint32_t)), std::ios::cur);

    std::vector<char> buf(size);
    log_file.read(buf.data(), size);
    if (log_file.gcount() < static_cast<std::streamsize>(size))
      break;

    record.Deserialize(buf.data());

    if (record.GetLogRecordType() == LogRecordType::NEWPAGE) {
      found_newpage = true;
      logged_prev_page_id = record.GetPrevPageId();
      logged_new_page_id = record.GetTargetRID().GetPageId();

      std::cout << "Found NEWPAGE log record:" << std::endl;
      std::cout << "  prev_page_id (logged): " << logged_prev_page_id
                << std::endl;
      std::cout << "  new_page_id  (logged): " << logged_new_page_id
                << std::endl;
    }
  }
  log_file.close();

  assert(found_newpage && "Must find a NEWPAGE log record in the WAL");

  // THE KEY ASSERTION: prev_page_id must be the FIRST page, not the new page
  assert(logged_prev_page_id == first_page_id &&
         "NEWPAGE prev_page_id must reference the OLD last page, not the new "
         "page");
  assert(logged_prev_page_id != logged_new_page_id &&
         "prev_page_id and new_page_id must be DIFFERENT");

  std::cout << "\nPASS: NEWPAGE WAL record correctly references prev_page_id="
            << first_page_id << " (not " << new_page_id << ")" << std::endl;

  // Cleanup
  log_mgr->StopFlushThread();
  txn_mgr->Abort(txn);
  bpm = nullptr;
  disk_mgr = nullptr;
  replacer = nullptr;
  log_mgr = nullptr;

  std::filesystem::remove(db_path);
  std::filesystem::remove(log_path);
  std::filesystem::remove(freelist_path);

  std::cout << "\n=== ALL C2 Tests PASSED ===" << std::endl;
  return 0;
}
