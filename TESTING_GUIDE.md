# TetoDB Testing Guide

We use **Google Test (GTest)** to write and run unit tests for TetoDB's internal components. GTest is automatically downloaded and linked when you configure the project with CMake.

## Running Tests

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
- `EXPECT_FALSE(condition)` — Checks condition is false
- `EXPECT_STREQ(str1, str2)` — Compares C-strings (char arrays)
