#include <cassert>
#include <iostream>
#include <memory>
#include <string>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include "storage/disk/disk_manager.h"
#include "storage/table/table_heap.h"
#include "type/value.h"

using namespace tetodb;

// A mock lock manager that forcefully FAILS the first LockExclusive call
class MockFailingLockManager : public LockManager {
public:
  bool should_fail_next = false;
  bool did_fail = false;

  bool LockExclusive(Transaction *txn, const RID &rid) override {
    if (should_fail_next) {
      should_fail_next = false;
      did_fail = true;
      return false; // Force failure to simulate Wait-Die abort
    }
    return LockManager::LockExclusive(txn, rid);
  }
};

int main() {
  std::string test_db = "test_bug4_orphan.db";
  std::remove(test_db.c_str()); // Clean start

  auto disk_manager = std::make_unique<DiskManager>(test_db);
  auto replacer = std::make_unique<TwoQueueReplacer>(10);
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_manager.get(),
                                                 replacer.get());

  MockFailingLockManager lock_mgr;
  auto log_mgr = std::make_unique<LogManager>(disk_manager.get());
  auto txn_mgr = std::make_unique<TransactionManager>(&lock_mgr, log_mgr.get());

  Transaction *txn = txn_mgr->Begin();
  TableHeap table_heap(bpm.get(), log_mgr.get(), txn);

  // Create a dummy schema and tuple
  std::vector<Column> cols;
  cols.emplace_back("id", TypeId::INTEGER);
  cols.emplace_back("val", TypeId::VARCHAR);
  Schema schema(cols);

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, 42);
  values.emplace_back(TypeId::VARCHAR, "orphan_test");
  Tuple dummy_tuple(values, &schema);

  RID rid;

  std::cout << "[TEST] Attempting INSERT that will suffer lock failure..."
            << std::endl;

  lock_mgr.should_fail_next = true;
  bool success = table_heap.InsertTuple(dummy_tuple, &rid, txn, &lock_mgr);

  if (success != false) {
    std::cout
        << "[FAIL] Insert unexpectedly succeeded despite fake lock rejection!"
        << std::endl;
    return 1;
  }
  if (lock_mgr.did_fail != true) {
    std::cout << "[FAIL] Mock lock manager was not triggered at all!"
              << std::endl;
    return 1;
  }

  std::cout << "[TEST] INSERT failed correctly. RID returned was "
            << rid.ToString() << std::endl;

  // VERIFICATION: Check if the tuple is physically valid on the page
  // The Bug #4 fix (ApplyDelete) should have erased it.
  std::cout << "[TEST] Verifying tuple does NOT exist horizontally..."
            << std::endl;

  // Test GetTuple
  Tuple read_tuple;
  bool get_success = table_heap.GetTuple(rid, &read_tuple, txn);
  if (get_success) {
    std::cout << "[URGENT FAIL] Tuple is still readable via GetTuple!"
              << std::endl;
    return 1;
  }

  // Also check page free space directly if we can
  Page *page = bpm->FetchPage(rid.GetPageId());
  if (page == nullptr) {
    std::cout << "[FAIL] Could not fetch page to verify slot." << std::endl;
    return 1;
  }

  auto *table_page = reinterpret_cast<TablePage *>(page);

  // IsValidTuple should return false for this slot exactly
  bool is_valid = table_page->IsValidTuple(rid.GetSlotId());
  if (is_valid) {
    std::cout << "[URGENT FAIL] Slot metadata still claims tuple is valid via "
                 "IsValidTuple!"
              << std::endl;
    return 1;
  }

  bpm->UnpinPage(rid.GetPageId(), false);
  txn_mgr->Abort(txn);

  std::cout << "[TEST] PASS: Orphan successfully deleted by ApplyDelete() hook!"
            << std::endl;
  std::remove(test_db.c_str());
  return 0;
}
