#include "catalog/schema.h"
#include "storage/page/table_page.h"
#include "storage/table/tuple.h"
#include <iostream>
#include <string>
#include <vector>

using namespace tetodb;

// Helper to inject the raw memory buffer into the TablePage wrapper
class TestTablePage : public TablePage {
public:
  void SetData(char *data) { this->data_ = data; }
};

int main() {
  std::cout << "[TEST] Starting Bug #7 Compaction test...\n";

  // 1. Initialize an in-memory TablePage
  char *page_data = new char[PAGE_SIZE];
  std::memset(page_data, 0, PAGE_SIZE);
  TestTablePage table_page;
  table_page.SetData(page_data);
  table_page.Init(0, PAGE_SIZE, INVALID_PAGE_ID, INVALID_LSN);

  // 2. Create a dummy schema
  std::vector<Column> columns = {Column("id", TypeId::INTEGER),
                                 Column("data", TypeId::VARCHAR, 2000)};
  Schema schema(columns);

  // 3. Create a massive tuple that takes up almost half the page
  std::vector<Value> values1;
  values1.push_back(Value(TypeId::INTEGER, 1));
  values1.push_back(Value(TypeId::VARCHAR, std::string(1500, 'A')));
  Tuple massive_tuple1(values1, &schema);

  std::vector<Value> values2;
  values2.push_back(Value(TypeId::INTEGER, 2));
  values2.push_back(Value(TypeId::VARCHAR, std::string(1500, 'B')));
  Tuple massive_tuple2(values2, &schema);

  // 4. Insert both
  RID rid1, rid2;
  bool success1 = table_page.InsertTuple(massive_tuple1, &rid1);
  bool success2 = table_page.InsertTuple(massive_tuple2, &rid2);

  std::cout << "[TEST] Insert Tuple 1 (1500 bytes): "
            << (success1 ? "SUCCESS" : "FAIL") << "\n";
  std::cout << "[TEST] Insert Tuple 2 (1500 bytes): "
            << (success2 ? "SUCCESS" : "FAIL") << "\n";

  // 5. Delete Tuple 1 to create fragmentation
  table_page.MarkDelete(rid1);
  table_page.ApplyDelete(rid1);
  std::cout << "[TEST] Deleted Tuple 1 to create 1500 bytes of fragmented free "
               "space.\n";

  // 6. Attempt to ForceInsert a tuple that needs 2000 bytes.
  std::vector<Value> values3;
  values3.push_back(Value(TypeId::INTEGER, 3));
  values3.push_back(Value(TypeId::VARCHAR, std::string(2000, 'C')));
  Tuple fit_tuple(values3, &schema);
  RID rid3(0, 2);
  bool success_fit = table_page.ForceInsertTuple(fit_tuple, rid3);
  std::cout
      << "[TEST] ForceInsert Tuple 3 (2000 bytes). Expected: SUCCESS. Actual: "
      << (success_fit ? "SUCCESS" : "FAIL") << "\n";

  // 7. Attempt to ForceInsert an oversized tuple that CANNOT fit even after
  // Compaction
  std::vector<Value> values4;
  values4.push_back(Value(TypeId::INTEGER, 4));
  values4.push_back(Value(TypeId::VARCHAR, std::string(1000, 'D')));
  Tuple oversize_tuple(values4, &schema);
  RID rid4(0, 3);

  uint32_t free_before = table_page.GetFreeSpacePointer();
  bool success_oversize = table_page.ForceInsertTuple(oversize_tuple, rid4);
  uint32_t free_after = table_page.GetFreeSpacePointer();

  std::cout << "[TEST] ForceInsert Oversized Tuple (1000 bytes).\n";
  std::cout << "[TEST] Expected: FAIL. Actual: "
            << (success_oversize ? "SUCCESS" : "FAIL") << "\n";

  if (!success_oversize && free_before == free_after) {
    std::cout << "[TEST] PASS: The page correctly rejected the overloaded "
                 "ForceInsert and avoided memory corruption.\n";
  } else {
    std::cout << "[TEST] FAIL: Memory corruption detected or insert "
                 "erroneously succeeded.\n";
    delete[] page_data;
    return 1;
  }

  delete[] page_data;
  return 0;
}
