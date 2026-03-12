# TetoDB Testing Guide

We use **Google Test (GTest)** for unit tests and **Google Benchmark** for performance benchmarks. Both are downloaded and linked automatically via CMake.

---

## 📋 What is Covered

### Unit Tests (`src/test_units.cpp`)

| Test Suite | Component | What it Tests |
|---|---|---|
| `ValueTest` | `type/value.h` | Integer/VARCHAR arithmetic and comparison ops |
| `DiskManagerTest` | `DiskManager` | Page allocation and raw read/write to disk |
| `BufferPoolManagerTest` | `BufferPoolManager` | Page pinning, eviction, and fetch-from-disk |
| `TwoQueueReplacerTest` | `TwoQueueReplacer` | LRU-K eviction policy, FIFO → LRU promotion |
| `TupleTest` | `Tuple` | Row serialization and deserialization |
| `LockManagerTest` | `LockManager` | Shared and exclusive lock acquisition |

### Benchmarks (`src/benchmarks.cpp`)

| Benchmark | Component | What it Measures |
|---|---|---|
| `BM_ValueIntegerAdd` | `Value` | ns per integer `Add` op |
| `BM_ValueVarcharCompare` | `Value` | ns per varchar `CompareEquals` op |
| `DiskManagerFixture/BM_DiskManagerSequentialWrite` | `DiskManager` | Raw sequential page write throughput (bytes/sec) |
| `BPMFixture/BM_BPM_CacheHit100` | `BufferPoolManager` | 100% cache hit rate — pure cache throughput |
| `BPMFixture/BM_BPM_RandomAccess` | `BufferPoolManager` | Random access — eviction pressure and cache miss cost |
| `BPlusTreeFixture/BM_BTree_RandomLookups` | `BPlusTree` | Random 4-byte key lookups across 1,000-key index |
| `BM_HashJoin_CorePressure` | Hash Join logic | Build + probe phase of a 1K×10K hash join |

---

## 🧪 Running Unit Tests

### 1. Build the Tests Target
```powershell
# From the TetoDB directory:
cmake --build build --config Release --target tetodb_tests
```

### 2. Execute the Tests
```powershell
# Run the executable directly:
.\build\Release\tetodb_tests.exe
```
This runs all tests and prints a colored summary of passes and failures.

_Note on Linux: the executable will likely be at `./build/tetodb_tests`._

## How to Write a New Test

Test files live in the `src/` directory (e.g., `src/test_units.cpp`). You don't need to write a `main()` function — GTest provides it automatically via `gtest_main`.

### Basic Test (No Setup)
Use the `TEST(TestSuiteName, TestName)` macro for simple standalone assertions.

```cpp
#include <gtest/gtest.h>
#include "type/value.h"

using namespace tetodb;

TEST(ValueTest, IntegerAddition) {
    Value v1(TypeId::INTEGER, 10);
    Value v2(TypeId::INTEGER, 20);
    
    Value sum = v1.Add(v2);
    
    // Use EXPECT_EQ for expected equality, EXPECT_TRUE for booleans
    EXPECT_EQ(sum.GetTypeId(), TypeId::INTEGER);
    EXPECT_EQ(sum.GetAsInteger(), 30);
}
```

### Fixture Test (With Setup/Teardown)
If your test requires initializing objects (like files on disk), use `TEST_F(FixtureClass, TestName)`. The fixture class must inherit from `::testing::Test`.

```cpp
#include <gtest/gtest.h>
#include "storage/disk/disk_manager.h"
#include <filesystem>

// 1. Define the Fixture
class DiskManagerTest : public ::testing::Test {
protected:
    // Setup runs BEFORE every test
    void SetUp() override {
        test_db_ = "test_disk.db";
        if (std::filesystem::exists(test_db_)) {
            std::filesystem::remove(test_db_);
        }
    }

    // TearDown runs AFTER every test
    void TearDown() override {
        if (std::filesystem::exists(test_db_)) {
            std::filesystem::remove(test_db_);
        }
    }

    std::filesystem::path test_db_;
};

// 2. Use TEST_F instead of TEST
TEST_F(DiskManagerTest, AllocatePageWorks) {
    // We have access to members defined in the fixture (test_db_)
    DiskManager dm(test_db_);
    page_id_t pid = dm.AllocatePage();
    EXPECT_EQ(pid, 0);
}
```

## Useful Assertions

Google Test provides `EXPECT_*` (non-fatal, continues the test if it fails) and `ASSERT_*` (fatal, aborts the test immediately if it fails).

- `EXPECT_EQ(val1, val2)` — Checks `val1 == val2`
- `EXPECT_NE(val1, val2)` — Checks `val1 != val2`
- `EXPECT_TRUE(condition)` — Checks condition is true
- `EXPECT_STREQ(str1, str2)` — Compares C-strings (char arrays)

---

## 🚀 Performance Benchmarking

TetoDB uses **Google Benchmark** to run microbenchmarks and stress tests on core components. This helps catch performance regressions during development.

### 1. Build the Benchmarks Target
```powershell
cmake --build build --config Release --target tetodb_benchmarks
```

### 2. Run All Benchmarks
```powershell
.\build\Release\tetodb_benchmarks.exe
```

### 3. Filter to Specific Benchmarks
```powershell
# Run only B+ Tree and Hash Join stress tests:
.\build\Release\tetodb_benchmarks.exe --benchmark_filter="BPlusTree|HashJoin"

# Run only Buffer Pool Manager benchmarks:
.\build\Release\tetodb_benchmarks.exe --benchmark_filter="BPM"
```

Google Benchmark auto-determines iteration count so that each benchmark runs long enough to produce statistically reliable results. It reports:
- **Time** — Wall-clock time per iteration
- **CPU** — CPU time per iteration  
- **Iterations** — How many times the loop ran

---

> [!IMPORTANT]
> **BPlusTree Insert / Remove requires a real `Transaction*`**  
> The B+ Tree's internal `FindLeafPage` unconditionally dereferences the transaction pointer for write operations. Always pass a valid `Transaction` object — never `nullptr` — when calling `Insert` or `Remove`. Reads (`GetValue`) can safely use `nullptr`.

---

### How to Write a Benchmark

Benchmarks are located in `src/benchmarks.cpp`.

#### Simple Benchmark (No Setup)
Use `BENCHMARK()` macro for isolated, simple operations:

```cpp
#include <benchmark/benchmark.h>
#include "type/value.h"

using namespace tetodb;

static void BM_ValueIntegerAdd(benchmark::State& state) {
    Value v1(TypeId::INTEGER, 100);
    Value v2(TypeId::INTEGER, 200);
    
    // The benchmark loop
    for (auto _ : state) {
        Value res = v1.Add(v2);
        benchmark::DoNotOptimize(res); // Prevent compiler from optimizing the loop away
    }
}
BENCHMARK(BM_ValueIntegerAdd);
```

#### Fixture Benchmark (With Setup/Teardown)
If your benchmark requires creating database files or allocating a buffer pool, use `benchmark::Fixture`:

```cpp
#include <benchmark/benchmark.h>
#include "storage/disk/disk_manager.h"

// 1. Define the Fixture
class DiskManagerFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        db_path_ = "bm_disk.db";
        // Setup code here...
    }

    void TearDown(const ::benchmark::State& state) {
        // Cleanup code here...
    }
    
    std::filesystem::path db_path_;
};

// 2. Use BENCHMARK_F
BENCHMARK_F(DiskManagerFixture, SequentialWrite)(benchmark::State& state) {
    DiskManager dm(db_path_);
    char payload[4096] = {0};

    for (auto _ : state) {
        page_id_t pid = dm.AllocatePage();
        dm.WritePage(pid, payload);
    }
    
    // Optional: Ask Google Benchmark to calculate IO throughput for us
    state.SetBytesProcessed(state.iterations() * 4096);
}
```
