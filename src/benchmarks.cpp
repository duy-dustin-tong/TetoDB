// benchmarks.cpp
#include <benchmark/benchmark.h>
#include <filesystem>
#include <fstream>
#include <random>
#include "type/value.h"
#include "storage/disk/disk_manager.h"
#include "storage/buffer/buffer_pool_manager.h"

using namespace tetodb;

// ==========================================
// 1. Value/Type Microbenchmarks
// ==========================================

static void BM_ValueIntegerAdd(benchmark::State& state) {
    Value v1(TypeId::INTEGER, 100);
    Value v2(TypeId::INTEGER, 200);
    for (auto _ : state) {
        Value res = v1.Add(v2);
        benchmark::DoNotOptimize(res);
    }
}
BENCHMARK(BM_ValueIntegerAdd);

static void BM_ValueVarcharCompare(benchmark::State& state) {
    Value v1(TypeId::VARCHAR, "TetoDB_Fast_String");
    Value v2(TypeId::VARCHAR, "TetoDB_Fast_String");
    for (auto _ : state) {
        bool res = v1.CompareEquals(v2);
        benchmark::DoNotOptimize(res);
    }
}
BENCHMARK(BM_ValueVarcharCompare);

// ==========================================
// 2. DiskManager Raw Storage Benchmarks
// ==========================================

class DiskManagerFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        db_path_ = "bm_diskmgr.db";
        Cleanup();
    }

    void TearDown(const ::benchmark::State& state) {
        Cleanup();
    }
    
    void Cleanup() {
        if (std::filesystem::exists(db_path_)) std::filesystem::remove(db_path_);
        std::filesystem::path fl = db_path_; fl.replace_extension(".freelist");
        if (std::filesystem::exists(fl)) std::filesystem::remove(fl);
        std::filesystem::path log = db_path_; log.replace_extension(".log");
        if (std::filesystem::exists(log)) std::filesystem::remove(log);
    }

    std::filesystem::path db_path_;
};

BENCHMARK_F(DiskManagerFixture, BM_DiskManagerSequentialWrite)(benchmark::State& state) {
    DiskManager dm(db_path_);
    char payload[PAGE_SIZE];
    std::fill(payload, payload + PAGE_SIZE, 'A');

    for (auto _ : state) {
        page_id_t pid = dm.AllocatePage();
        dm.WritePage(pid, payload);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(PAGE_SIZE));
}

// ==========================================
// 3. BufferPoolManager Eviction & Hit Benchmarks
// ==========================================

class BPMFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        db_path_ = "bm_bpm.db";
        Cleanup();
        dm_ = std::make_unique<DiskManager>(db_path_);
        replacer_ = std::make_unique<TwoQueueReplacer>(POOL_SIZE);
        bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, dm_.get(), replacer_.get());
        
        // Pre-allocate pages
        char payload[PAGE_SIZE] = {0};
        for(size_t i = 0; i < NUM_PAGES; i++) {
            page_id_t pid = dm_->AllocatePage();
            dm_->WritePage(pid, payload);
        }
    }

    void TearDown(const ::benchmark::State& state) {
        bpm_ = nullptr;
        replacer_ = nullptr;
        dm_ = nullptr;
        Cleanup();
    }
    
    void Cleanup() {
        if (std::filesystem::exists(db_path_)) std::filesystem::remove(db_path_);
        std::filesystem::path fl = db_path_; fl.replace_extension(".freelist");
        if (std::filesystem::exists(fl)) std::filesystem::remove(fl);
        std::filesystem::path log = db_path_; log.replace_extension(".log");
        if (std::filesystem::exists(log)) std::filesystem::remove(log);
    }

    std::filesystem::path db_path_;
    std::unique_ptr<DiskManager> dm_;
    std::unique_ptr<TwoQueueReplacer> replacer_;
    std::unique_ptr<BufferPoolManager> bpm_;
    
    const size_t POOL_SIZE = 100;
    const size_t NUM_PAGES = 1000;
};

// Benchmark 100% Cache Hits
BENCHMARK_F(BPMFixture, BM_BPM_CacheHit100)(benchmark::State& state) {
    // Load first 100 pages into cache
    for(size_t i = 0; i < POOL_SIZE; i++) {
        Page* p = bpm_->FetchPage(i);
        bpm_->UnpinPage(i, false);
    }
    
    // Now just fetch and unpin the exact same pages over and over
    for (auto _ : state) {
        for(size_t i = 0; i < POOL_SIZE; i++) {
            Page* p = bpm_->FetchPage(i);
            benchmark::DoNotOptimize(p);
            bpm_->UnpinPage(i, false);
        }
    }
}

// Benchmark Cache Misses with random eviction
BENCHMARK_F(BPMFixture, BM_BPM_RandomAccess)(benchmark::State& state) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(0, NUM_PAGES - 1);
    
    for (auto _ : state) {
        page_id_t pid = distr(gen);
        Page* p = bpm_->FetchPage(pid);
        benchmark::DoNotOptimize(p);
        if (p != nullptr) {
            bpm_->UnpinPage(pid, false);
        }
    }
}

#include "index/generic_key.h"
#include "common/record_id.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "concurrency/transaction.h"

namespace tetodb {
// The B+ Tree implementation uses macros that require these exact type names to be defined in scope
typedef GenericKey<4> KeyType;
typedef RID ValueType;
typedef GenericComparator<4> KeyComparator;
}

#include "index/b_plus_tree.h"

class BPlusTreeFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        db_path_ = "bm_bpt.db";
        Cleanup();
        dm_ = std::make_unique<tetodb::DiskManager>(db_path_);
        replacer_ = std::make_unique<tetodb::TwoQueueReplacer>(500);
        bpm_ = std::make_unique<tetodb::BufferPoolManager>(500, dm_.get(), replacer_.get());
        
        std::vector<tetodb::Column> cols = {tetodb::Column("key", tetodb::TypeId::INTEGER)};
        schema_ = std::make_unique<tetodb::Schema>(cols);
        tetodb::KeyComparator comp(tetodb::TypeId::INTEGER);
        
        tree_ = std::make_unique<tetodb::BPlusTree<tetodb::KeyType, tetodb::ValueType, tetodb::KeyComparator>>("bm_index", bpm_.get(), comp, LEAF_PAGE_SIZE, INTERNAL_PAGE_SIZE, INVALID_PAGE_ID);

        // Pre-populate tree with 1,000 keys. Use a real Transaction to avoid null deref in FindLeafPage.
        tetodb::Transaction txn(0);
        for(int i = 0; i < 1000; i++) {
            tetodb::KeyType k;
            k.SetFromValue(tetodb::Value(tetodb::TypeId::INTEGER, i));
            tetodb::ValueType rid(i, i);
            tree_->Insert(k, rid, &txn);
        }
    }

    void TearDown(const ::benchmark::State& state) {
        tree_ = nullptr;
        bpm_ = nullptr;
        replacer_ = nullptr;
        dm_ = nullptr;
        Cleanup();
    }
    
    void Cleanup() {
        if (std::filesystem::exists(db_path_)) std::filesystem::remove(db_path_);
        std::filesystem::path fl = db_path_; fl.replace_extension(".freelist");
        if (std::filesystem::exists(fl)) std::filesystem::remove(fl);
        std::filesystem::path log = db_path_; log.replace_extension(".log");
        if (std::filesystem::exists(log)) std::filesystem::remove(log);
    }

    std::filesystem::path db_path_;
    std::unique_ptr<tetodb::DiskManager> dm_;
    std::unique_ptr<tetodb::TwoQueueReplacer> replacer_;
    std::unique_ptr<tetodb::BufferPoolManager> bpm_;
    std::unique_ptr<tetodb::Schema> schema_;
    std::unique_ptr<tetodb::BPlusTree<tetodb::KeyType, tetodb::ValueType, tetodb::KeyComparator>> tree_;
};

// Benchmark rapid pinpoint lookups in a 1k-key B+ Tree
BENCHMARK_F(BPlusTreeFixture, BM_BTree_RandomLookups)(benchmark::State& state) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(0, 999);
    
    for (auto _ : state) {
        int key_val = distr(gen);
        tetodb::KeyType k;
        k.SetFromValue(tetodb::Value(tetodb::TypeId::INTEGER, key_val));
        std::vector<tetodb::ValueType> result;
        tree_->GetValue(k, &result, nullptr);
        
        benchmark::DoNotOptimize(result);
    }
}

// ==========================================
// 5. Join Executor Stress Benchmarks
// ==========================================
#include "execution/executors/hash_join_executor.h"

// Note: Testing an executor requires setting up dummy plan nodes and child executors,
// which is extremely verbose. We'll simulate a tight loop inner Hash Join 
// logic (hash table build + probe) to simulate the core CPU pressure of a DB join.
static void BM_HashJoin_CorePressure(benchmark::State& state) {
    // Simulate Left Table (1,000 rows)
    std::vector<int32_t> left_keys;
    for(int i = 0; i < 1000; i++) left_keys.push_back(i);
    
    // Simulate Right Table (10,000 rows, 10-to-1 relationship)
    std::vector<int32_t> right_keys;
    for(int i = 0; i < 10000; i++) right_keys.push_back(i % 1000);
    
    for (auto _ : state) {
        // BUILD Phase
        std::unordered_map<int32_t, int> hash_table;
        for (int k : left_keys) {
            hash_table[k] = 1;
        }
        
        // PROBE Phase
        int match_count = 0;
        for (int k : right_keys) {
            auto it = hash_table.find(k);
            if (it != hash_table.end()) {
                match_count++;
            }
        }
        benchmark::DoNotOptimize(match_count);
    }
}
BENCHMARK(BM_HashJoin_CorePressure);
