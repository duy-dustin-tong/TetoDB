// test_units.cpp
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "storage/disk/disk_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "type/value.h"

using namespace tetodb;

// ==========================================
// 1. Value/Type Tests
// ==========================================
TEST(ValueTest, IntegerOperations) {
    Value v1(TypeId::INTEGER, 10);
    Value v2(TypeId::INTEGER, 20);
    
    Value sum = v1.Add(v2);
    EXPECT_EQ(sum.GetTypeId(), TypeId::INTEGER);
    EXPECT_EQ(sum.GetAsInteger(), 30);
    
    bool cmp = v1.CompareLessThan(v2);
    EXPECT_TRUE(cmp);
}

TEST(ValueTest, VarcharOperations) {
    Value str1(TypeId::VARCHAR, "Hello");
    Value str2(TypeId::VARCHAR, "World");
    
    bool cmp1 = str1.CompareEquals(str1);
    EXPECT_TRUE(cmp1);
    
    bool cmp2 = str1.CompareNotEquals(str2);
    EXPECT_TRUE(cmp2);
}

// ==========================================
// 2. DiskManager Tests
// ==========================================
class DiskManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_db_ = "test_disk_mgr.db";
        if (std::filesystem::exists(test_db_)) {
            std::filesystem::remove(test_db_);
        }
        std::filesystem::path freelist_name = test_db_;
        freelist_name.replace_extension(".freelist");
        if (std::filesystem::exists(freelist_name)) std::filesystem::remove(freelist_name);
        std::filesystem::path log_name = test_db_;
        log_name.replace_extension(".log");
        if (std::filesystem::exists(log_name)) std::filesystem::remove(log_name);
    }

    void TearDown() override {
        if (std::filesystem::exists(test_db_)) std::filesystem::remove(test_db_);
        std::filesystem::path freelist_name = test_db_;
        freelist_name.replace_extension(".freelist");
        if (std::filesystem::exists(freelist_name)) std::filesystem::remove(freelist_name);
        std::filesystem::path log_name = test_db_;
        log_name.replace_extension(".log");
        if (std::filesystem::exists(log_name)) std::filesystem::remove(log_name);
    }

    std::filesystem::path test_db_;
};

TEST_F(DiskManagerTest, ReadWritePage) {
    DiskManager dm(test_db_);
    page_id_t page0 = dm.AllocatePage();
    EXPECT_EQ(page0, 0);

    char write_data[PAGE_SIZE] = {0};
    strncpy(write_data, "Hello TetoDB", PAGE_SIZE);
    
    EXPECT_TRUE(dm.WritePage(page0, write_data));
    
    char read_data[PAGE_SIZE] = {0};
    dm.ReadPage(page0, read_data);
    
    EXPECT_STREQ(read_data, "Hello TetoDB");
}

// ==========================================
// 3. BufferPoolManager Tests
// ==========================================
class BufferPoolManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_db_ = "test_bpm.db";
        if (std::filesystem::exists(test_db_)) std::filesystem::remove(test_db_);
        std::filesystem::path freelist = test_db_; freelist.replace_extension(".freelist");
        if (std::filesystem::exists(freelist)) std::filesystem::remove(freelist);
        std::filesystem::path log = test_db_; log.replace_extension(".log");
        if (std::filesystem::exists(log)) std::filesystem::remove(log);
    }
    void TearDown() override {
        if (std::filesystem::exists(test_db_)) std::filesystem::remove(test_db_);
        std::filesystem::path freelist = test_db_; freelist.replace_extension(".freelist");
        if (std::filesystem::exists(freelist)) std::filesystem::remove(freelist);
        std::filesystem::path log = test_db_; log.replace_extension(".log");
        if (std::filesystem::exists(log)) std::filesystem::remove(log);
    }
    std::filesystem::path test_db_;
};

TEST_F(BufferPoolManagerTest, SimpleFetchAndEvict) {
    DiskManager dm(test_db_);
    TwoQueueReplacer replacer(2);
    BufferPoolManager bpm(2, &dm, &replacer);

    page_id_t pid0;
    Page* p0 = bpm.NewPage(&pid0);
    ASSERT_NE(p0, nullptr);
    strncpy(p0->GetData(), "Page0", PAGE_SIZE);
    bpm.UnpinPage(pid0, true);

    page_id_t pid1;
    Page* p1 = bpm.NewPage(&pid1);
    ASSERT_NE(p1, nullptr);
    strncpy(p1->GetData(), "Page1", PAGE_SIZE);
    bpm.UnpinPage(pid1, true);

    // Pool size is 2, so creating a 3rd should cause eviction.
    page_id_t pid2;
    Page* p2 = bpm.NewPage(&pid2);
    ASSERT_NE(p2, nullptr);
    strncpy(p2->GetData(), "Page2", PAGE_SIZE);
    bpm.UnpinPage(pid2, true);

    // Fetch page 0 again -> should read from disk
    Page* p0_re = bpm.FetchPage(pid0);
    ASSERT_NE(p0_re, nullptr);
    EXPECT_STREQ(p0_re->GetData(), "Page0");
    bpm.UnpinPage(pid0, false);
}

// ==========================================
// 4. TwoQueueReplacer Tests
// ==========================================
#include "storage/buffer/two_queue_replacer.h"

TEST(TwoQueueReplacerTest, BasicOperations) {
    TwoQueueReplacer replacer(3);
    
    // Add 1, 2, 3
    replacer.RecordAccess(1);
    replacer.RecordAccess(2);
    replacer.RecordAccess(3);
    
    // Mark them as evictable
    replacer.SetEvictable(1, true);
    replacer.SetEvictable(2, true);
    replacer.SetEvictable(3, true);
    
    frame_id_t evicted;
    
    // 1 should be evicted first (FIFO)
    EXPECT_TRUE(replacer.Evict(&evicted));
    EXPECT_EQ(evicted, 1);
    
    // 2 should be evicted second
    EXPECT_TRUE(replacer.Evict(&evicted));
    EXPECT_EQ(evicted, 2);
    
    // Access 3 again -> moves to LRU queue
    replacer.RecordAccess(3);
    
    // Pin 3 -> cannot be evicted
    replacer.SetEvictable(3, false);
    EXPECT_FALSE(replacer.Evict(&evicted));
}

// ==========================================
// 5. Tuple Tests
// ==========================================
#include "storage/table/tuple.h"

TEST(TupleTest, TupleSerialization) {
    std::vector<Column> cols = {
        Column("A", TypeId::INTEGER),
        Column("B", TypeId::VARCHAR)
    };
    Schema schema(cols);

    std::vector<Value> values = {
        Value(TypeId::INTEGER, 42),
        Value(TypeId::VARCHAR, "TetoDB")
    };
    Tuple tuple(values, &schema);

    // Verify values BEFORE serialization
    EXPECT_EQ(tuple.GetValue(&schema, 0).GetAsInteger(), 42);
    EXPECT_STREQ(tuple.GetValue(&schema, 1).GetAsString().c_str(), "TetoDB");
    
    // Serialize
    char buffer[PAGE_SIZE];
    tuple.SerializeTo(buffer);

    // Deserialize
    Tuple recovered_tuple;
    recovered_tuple.DeserializeFrom(buffer, tuple.GetSize());

    // Verify values AFTER deserialization
    EXPECT_EQ(recovered_tuple.GetValue(&schema, 0).GetAsInteger(), 42);
    EXPECT_STREQ(recovered_tuple.GetValue(&schema, 1).GetAsString().c_str(), "TetoDB");
}

// ==========================================
// 6. LockManager Tests
// ==========================================
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

TEST(LockManagerTest, BasicSharedLocking) {
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, nullptr);
    
    Transaction* txn1 = txn_mgr.Begin();
    Transaction* txn2 = txn_mgr.Begin();
    
    RID rid(0, 0); // page 0, slot 0
    
    // Both can acquire Shared Lock
    EXPECT_TRUE(lock_mgr.LockShared(txn1, rid));
    EXPECT_TRUE(lock_mgr.LockShared(txn2, rid));
    
    // Clean up
    txn_mgr.Commit(txn1);
    txn_mgr.Commit(txn2);
}

TEST(LockManagerTest, ExclusiveLockBlocksShared) {
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, nullptr);
    
    Transaction* txn1 = txn_mgr.Begin();
    Transaction* txn2 = txn_mgr.Begin();
    
    RID rid(1, 1);
    
    // Txn1 gets Exclusive Lock
    EXPECT_TRUE(lock_mgr.LockExclusive(txn1, rid));
    
    EXPECT_TRUE(lock_mgr.Unlock(txn1, rid));
    txn_mgr.Commit(txn1);
    txn_mgr.Commit(txn2);
}
