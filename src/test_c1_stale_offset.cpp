// test_c1_stale_offset.cpp
// Test for C1 fix: UpdateTuple must not use stale slot offset after compaction.
//
// Scenario:
// 1. Fill a page with 2 tuples that together nearly fill it.
// 2. Delete tuple 1 to create fragmented dead space.
// 3. Attempt to UpdateTuple on tuple 2 with data LARGER than can fit
//    even after compaction — this forces the failure path.
// 4. Verify the original tuple 2 is still readable and intact.
// 5. Also test the success path: update tuple 2 with data that fits
//    after compaction, verify the new data is correct.

#include "catalog/schema.h"
#include "storage/page/table_page.h"
#include "storage/table/tuple.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

using namespace tetodb;

// Helper to inject raw memory into TablePage
class TestTablePage : public TablePage {
public:
  void SetData(char *data) { this->data_ = data; }
};

int main() {
  std::cout << "=== C1 Fix: Stale Slot Offset After Compaction ==="
            << std::endl;

  // Schema: (id INTEGER, data VARCHAR(2000))
  std::vector<Column> cols = {Column("id", TypeId::INTEGER),
                              Column("data", TypeId::VARCHAR, 2000)};
  Schema schema(cols);

  // --- Test 1: Failure path — update too large, original must survive ---
  {
    std::cout << "\n[Test 1] Update failure path: original tuple must survive "
                 "compaction"
              << std::endl;

    char *page_data = new char[PAGE_SIZE];
    std::memset(page_data, 0, PAGE_SIZE);
    TestTablePage page;
    page.SetData(page_data);
    page.Init(0, PAGE_SIZE, INVALID_PAGE_ID, INVALID_LSN);

    // Insert tuple 1: ~1500 bytes
    Tuple t1({Value(TypeId::INTEGER, 1),
              Value(TypeId::VARCHAR, std::string(1500, 'A'))},
             &schema);
    RID rid1;
    bool ok = page.InsertTuple(t1, &rid1);
    assert(ok && "Tuple 1 insert must succeed");

    // Insert tuple 2: ~1500 bytes
    Tuple t2({Value(TypeId::INTEGER, 2),
              Value(TypeId::VARCHAR, std::string(1500, 'B'))},
             &schema);
    RID rid2;
    ok = page.InsertTuple(t2, &rid2);
    assert(ok && "Tuple 2 insert must succeed");

    // Delete tuple 1 to create fragmented space
    page.MarkDelete(rid1);
    page.ApplyDelete(rid1);
    std::cout << "  Deleted tuple 1 to create fragmented space." << std::endl;

    // Try to update tuple 2 with something TOO LARGE (3500 bytes)
    // Even after compaction (which reclaims ~1500 from tuple 1),
    // total free space is ~1500 + existing free, but we need ~3500.
    Tuple t2_huge({Value(TypeId::INTEGER, 2),
                   Value(TypeId::VARCHAR, std::string(3500, 'X'))},
                  &schema);
    Tuple old_tuple;
    bool update_ok = page.UpdateTuple(t2_huge, &old_tuple, rid2);
    assert(!update_ok && "Update must fail — not enough space");
    std::cout << "  Update correctly failed (too large)." << std::endl;

    // CRITICAL: Verify original tuple 2 is still readable and intact
    Tuple verify;
    bool read_ok = page.GetTuple(rid2, &verify);
    assert(read_ok &&
           "Original tuple must still be readable after failed update");

    // Verify the data is correct (should be 'B' * 1500)
    Value v = verify.GetValue(&schema, 1);
    std::string s = v.GetAsString();
    assert(s.length() == 1500 &&
           "Original tuple data length must be preserved");
    assert(s[0] == 'B' && s[1499] == 'B' &&
           "Original tuple data must be intact");

    std::cout << "  PASS: Original tuple survived compaction with correct data."
              << std::endl;

    delete[] page_data;
  }

  // --- Test 2: Success path — update fits after compaction ---
  {
    std::cout
        << "\n[Test 2] Update success path: larger tuple fits after compaction"
        << std::endl;

    char *page_data = new char[PAGE_SIZE];
    std::memset(page_data, 0, PAGE_SIZE);
    TestTablePage page;
    page.SetData(page_data);
    page.Init(0, PAGE_SIZE, INVALID_PAGE_ID, INVALID_LSN);

    // Insert tuple 1: ~800 bytes
    Tuple t1({Value(TypeId::INTEGER, 1),
              Value(TypeId::VARCHAR, std::string(800, 'A'))},
             &schema);
    RID rid1;
    bool ok = page.InsertTuple(t1, &rid1);
    assert(ok);

    // Insert tuple 2: ~800 bytes
    Tuple t2({Value(TypeId::INTEGER, 2),
              Value(TypeId::VARCHAR, std::string(800, 'B'))},
             &schema);
    RID rid2;
    ok = page.InsertTuple(t2, &rid2);
    assert(ok);

    // Insert tuple 3: ~800 bytes
    Tuple t3({Value(TypeId::INTEGER, 3),
              Value(TypeId::VARCHAR, std::string(800, 'C'))},
             &schema);
    RID rid3;
    ok = page.InsertTuple(t3, &rid3);
    assert(ok);

    // Delete tuple 1 and tuple 3 to create fragmented space
    page.MarkDelete(rid1);
    page.ApplyDelete(rid1);
    page.MarkDelete(rid3);
    page.ApplyDelete(rid3);
    std::cout << "  Deleted tuples 1 and 3 to create fragmented space."
              << std::endl;

    // Update tuple 2 with larger data (~1400 bytes)
    // This needs compaction since the contiguous free space is fragmented
    Tuple t2_bigger({Value(TypeId::INTEGER, 2),
                     Value(TypeId::VARCHAR, std::string(1400, 'Z'))},
                    &schema);
    Tuple old_tuple;
    bool update_ok = page.UpdateTuple(t2_bigger, &old_tuple, rid2);
    assert(update_ok && "Update must succeed after compaction frees space");
    std::cout << "  Update succeeded after compaction." << std::endl;

    // Verify updated data
    Tuple verify;
    bool read_ok = page.GetTuple(rid2, &verify);
    assert(read_ok && "Updated tuple must be readable");

    Value v = verify.GetValue(&schema, 1);
    std::string s = v.GetAsString();
    assert(s.length() == 1400 && "Updated tuple data length must be correct");
    assert(s[0] == 'Z' && s[1399] == 'Z' &&
           "Updated tuple data must be correct");

    // Verify old_tuple contains the original data
    Value ov = old_tuple.GetValue(&schema, 1);
    std::string os = ov.GetAsString();
    assert(os.length() == 800 && "Old tuple must have original length");
    assert(os[0] == 'B' && os[799] == 'B' &&
           "Old tuple must have original data");

    std::cout << "  PASS: Update succeeded and data verified." << std::endl;

    delete[] page_data;
  }

  std::cout << "\n=== ALL C1 Tests PASSED ===" << std::endl;
  return 0;
}
