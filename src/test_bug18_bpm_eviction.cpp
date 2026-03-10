#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include <cstring>
#include <filesystem>
#include <iostream>
#include <vector>

using namespace tetodb;

int main() {
  std::cout << "--- TetoDB Bug 18 BufferPool Eviction Validation ---"
            << std::endl;

  std::filesystem::path db_path = "test_bug18.db";
  std::filesystem::remove(db_path);
  std::filesystem::remove("test_bug18.log");
  std::filesystem::remove("test_bug18.freelist");

  auto disk_mgr = std::make_unique<DiskManager>(db_path);
  auto replacer = std::make_unique<TwoQueueReplacer>(2);
  auto bpm =
      std::make_unique<BufferPoolManager>(2, disk_mgr.get(), replacer.get());

  page_id_t page0_id, page1_id, page2_id;

  // Allocate 2 pages to fill the pool
  Page *p0 = bpm->NewPage(&page0_id);
  std::strcpy(p0->GetData(), "Page 0 Data");
  bpm->UnpinPage(page0_id, true);

  Page *p1 = bpm->NewPage(&page1_id);
  std::strcpy(p1->GetData(), "Page 1 Data");
  bpm->UnpinPage(page1_id, true);

  std::cout << "Evicting Page 0 by allocating Page 2..." << std::endl;
  Page *p2 = bpm->NewPage(&page2_id);
  std::strcpy(p2->GetData(), "Page 2 Data");
  bpm->UnpinPage(page2_id, true);

  if (p2 != nullptr) {
    std::cout << "Eviction logically succeeded under normal conditions."
              << std::endl;
  }

  std::cout << "Fetching Page 0 back from disk..." << std::endl;
  Page *p0_fetched = bpm->FetchPage(page0_id);

  if (p0_fetched != nullptr &&
      std::string(p0_fetched->GetData()) == "Page 0 Data") {
    std::cout << "SUCCESS: Data successfully routed and flushed through the "
                 "bool-guarded WritePage!"
              << std::endl;
  } else {
    std::cout << "FAIL: Data corruption or loss during boolean guard execution!"
              << std::endl;
  }

  bpm->UnpinPage(page0_id, false);

  // Cleanup
  bpm = nullptr;
  replacer = nullptr;
  disk_mgr = nullptr;
  std::filesystem::remove(db_path);
  std::filesystem::remove("test_bug18.log");
  std::filesystem::remove("test_bug18.freelist");

  return 0;
}
