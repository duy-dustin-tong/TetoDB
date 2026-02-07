//#include <iostream>
//#include <vector>
//#include <string>
//#include <cassert>
//#include <random>
//#include <chrono>
//
//#include "storage/disk/disk_manager.h"
//#include "storage/buffer/buffer_pool_manager.h"
//#include "storage/table/table_heap.h"
//#include "storage/table/tuple.h"
//
//using namespace tetodb;
//
//// Helper to create a dummy tuple string
//std::string CreateRandomString(int len) {
//    static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
//    std::string s;
//    for (int i = 0; i < len; ++i) {
//        s += alphanum[rand() % (sizeof(alphanum) - 1)];
//    }
//    return s;
//}
//
//void TestInsertAndRead() {
//    std::cout << "--- Test 1: Insert & Read (Basic) ---" << std::endl;
//
//    // 1. Setup
//    auto disk_manager = new DiskManager("test.db");
//    auto replacer = new TwoQueueReplacer(10); // Small pool (10 pages) to force swapping
//    auto bpm = new BufferPoolManager(10, disk_manager, replacer);
//
//    // 2. Create Table
//    auto table = new TableHeap(bpm);
//
//    // 3. Insert 1 Tuple
//    std::string row_data = "Hello TetoDB!";
//    Tuple tuple(RID(), row_data.c_str(), row_data.length());
//    RID rid;
//
//    bool result = table->InsertTuple(tuple, &rid, nullptr);
//    assert(result == true);
//    std::cout << "Inserted at: " << rid.ToString() << std::endl;
//
//    // 4. Read it back
//    Tuple result_tuple;
//    bool found = table->GetTuple(rid, &result_tuple, nullptr);
//    assert(found == true);
//
//    std::string result_str(result_tuple.GetData(), result_tuple.GetSize());
//    assert(result_str == row_data);
//
//    std::cout << "Read Match: " << result_str << std::endl;
//
//    // Cleanup
//    delete table;
//    delete bpm;
//    delete replacer;
//    delete disk_manager;
//    std::remove("test.db"); // Clean up file
//    std::cout << "PASS" << std::endl;
//}
//
//void TestLargeInsert() {
//    std::cout << "\n--- Test 2: Large Insert (Page Splits) ---" << std::endl;
//
//    // 1. Setup
//    auto disk_manager = new DiskManager("test.db");
//    auto replacer = new TwoQueueReplacer(50);
//    auto bpm = new BufferPoolManager(50, disk_manager, replacer);
//    auto table = new TableHeap(bpm);
//
//    int num_tuples = 5000;
//    std::vector<RID> rids;
//    std::vector<std::string> expected_values;
//
//    // 2. Insert 5000 tuples (approx 100 bytes each -> ~500KB total -> ~125 pages)
//    // This forces the Heap to allocate new pages many times.
//    for (int i = 0; i < num_tuples; i++) {
//        std::string val = "Entry_" + std::to_string(i) + "_" + CreateRandomString(50);
//        Tuple tuple(RID(), val.c_str(), val.length());
//        RID rid;
//
//        bool res = table->InsertTuple(tuple, &rid, nullptr);
//        if (!res) {
//            std::cout << "Failed to insert at index " << i << std::endl;
//            break;
//        }
//        rids.push_back(rid);
//        expected_values.push_back(val);
//    }
//
//    std::cout << "Inserted " << rids.size() << " tuples." << std::endl;
//    assert(rids.size() == num_tuples);
//
//    // 3. Verify ALL of them
//    for (int i = 0; i < num_tuples; i++) {
//        Tuple res_tuple;
//        bool found = table->GetTuple(rids[i], &res_tuple, nullptr);
//        assert(found == true);
//
//        std::string actual(res_tuple.GetData(), res_tuple.GetSize());
//        assert(actual == expected_values[i]);
//    }
//    std::cout << "Verified all tuples." << std::endl;
//
//    // 4. Test Persistence (Simulate Crash)
//    // We delete the BufferPool (forcing flush due to destructor fix) and reload.
//    delete table;
//    delete bpm; // Should FlushAllPages()
//    delete replacer;
//
//    std::cout << "Simulating Restart..." << std::endl;
//
//    // Reload
//    auto replacer2 = new TwoQueueReplacer(50);
//    auto bpm2 = new BufferPoolManager(50, disk_manager, replacer2);
//
//    // We need to know where the table started. 
//    // In a real DB, the Catalog stores "Table 'A' starts at Page 0".
//    // Here we assume Page 0.
//    auto table2 = new TableHeap(bpm2, 0);
//
//    // Verify a few random tuples from disk
//    for (int i = 0; i < num_tuples; i += 100) {
//        Tuple res_tuple;
//        bool found = table2->GetTuple(rids[i], &res_tuple, nullptr);
//        assert(found == true);
//
//        std::string actual(res_tuple.GetData(), res_tuple.GetSize());
//        assert(actual == expected_values[i]);
//    }
//
//    std::cout << "Persistence Verified." << std::endl;
//
//    delete table2;
//    delete bpm2;
//    delete replacer2;
//    delete disk_manager;
//    std::remove("test.db");
//    std::cout << "PASS" << std::endl;
//}

//int main() {
//    TestInsertAndRead();
//    TestLargeInsert();
//    return 0;
//}