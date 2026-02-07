/**
 * test/tuple_test_comprehensive.cpp
 *
 * THE CATCH-ALL SUITE
 * Advanced stress testing for TetoDB Tuple Storage.
 */

#include <iostream>
#include <vector>
#include <cstring>
#include <string>
#include <cassert>
#include <numeric> // for std::iota

#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/value.h"

using namespace tetodb;

// --- Test Macros ---
#define ASSERT_EQ(val1, val2, msg) \
    if ((val1) != (val2)) { \
        std::cerr << "[\033[31mFAIL\033[0m] " << msg << "\n" \
                  << "       Expected: " << (val1) << "\n" \
                  << "       Actual:   " << (val2) << "\n" \
                  << "       Line:     " << __LINE__ << std::endl; \
        std::exit(1); \
    }

#define ASSERT_TRUE(expr, msg) \
    if (!(expr)) { \
        std::cerr << "[\033[31mFAIL\033[0m] " << msg << "\n" \
                  << "       Expression is FALSE\n" \
                  << "       Line: " << __LINE__ << std::endl; \
        std::exit(1); \
    }

void LogPass(const std::string& name) {
    std::cout << "[\033[32mPASS\033[0m] " << name << std::endl;
}

// =========================================================================
// TEST 1: The "Kitchen Sink" (All Types)
// =========================================================================
void TestKitchenSink() {
    std::cout << "\n--- Test 1: Kitchen Sink (All Types) ---" << std::endl;

    std::vector<Column> cols = {
        Column("col_bool", TypeId::BOOLEAN),
        Column("col_tiny", TypeId::TINYINT),
        Column("col_small", TypeId::SMALLINT),
        Column("col_int", TypeId::INTEGER),
        Column("col_big", TypeId::BIGINT),
        Column("col_decimal", TypeId::DECIMAL),
        Column("col_char", TypeId::CHAR, 10),
        Column("col_varchar", TypeId::VARCHAR, 50)
    };
    Schema schema(cols);

    std::vector<Value> values = {
        Value(TypeId::BOOLEAN, true),
        Value(TypeId::TINYINT, 127),
        Value(TypeId::SMALLINT, 32000),
        Value(TypeId::INTEGER, 2147483647),
        Value(TypeId::BIGINT, 9223372036854775807LL),
        Value(TypeId::DECIMAL, 3.14159),
        Value(TypeId::CHAR, "Fixed"),
        Value(TypeId::VARCHAR, "VariableData")
    };

    Tuple t(values, &schema);

    // Verify Read Back
    ASSERT_EQ(t.GetValue(&schema, 0).GetAsBoolean(), true, "Bool failed");
    ASSERT_EQ(t.GetValue(&schema, 1).GetAsInteger(), 127, "TinyInt failed");
    ASSERT_EQ(t.GetValue(&schema, 2).GetAsInteger(), 32000, "SmallInt failed");
    ASSERT_EQ(t.GetValue(&schema, 3).GetAsInteger(), 2147483647, "Integer failed");
    ASSERT_EQ(t.GetValue(&schema, 4).GetAsBigInt(), 9223372036854775807LL, "BigInt failed");
    // Float comparison with small epsilon
    double d = t.GetValue(&schema, 5).GetAsDecimal();
    ASSERT_TRUE((d > 3.1415 && d < 3.1416), "Decimal failed");

    ASSERT_EQ(t.GetValue(&schema, 6).GetAsString(), "Fixed", "Char failed");
    ASSERT_EQ(t.GetValue(&schema, 7).GetAsString(), "VariableData", "Varchar failed");

    LogPass("Kitchen Sink (Mixed Types)");
}

// =========================================================================
// TEST 2: Multiple VARCHARs (Heap Packing)
// =========================================================================
void TestMultiVarchar() {
    std::cout << "\n--- Test 2: Multiple Varchars (Heap Packing) ---" << std::endl;

    // We want to ensure that writing one VARCHAR doesn't overwrite the next one.
    std::vector<Column> cols = {
        Column("v1", TypeId::VARCHAR, 20),
        Column("v2", TypeId::VARCHAR, 20),
        Column("v3", TypeId::VARCHAR, 20)
    };
    Schema schema(cols);

    std::vector<Value> vals = {
        Value(TypeId::VARCHAR, "First"),
        Value(TypeId::VARCHAR, "Second"),
        Value(TypeId::VARCHAR, "Third")
    };
    Tuple t(vals, &schema);

    // 1. Verify Offsets
    // Header size = 4 + 4 + 4 = 12 bytes.
    const char* data = t.GetData();
    uint32_t off1 = *reinterpret_cast<const uint32_t*>(data + 0);
    uint32_t off2 = *reinterpret_cast<const uint32_t*>(data + 4);
    uint32_t off3 = *reinterpret_cast<const uint32_t*>(data + 8);

    ASSERT_EQ(off1, 12, "First offset should start after header");

    // "First" length is 5. Storage: [4 bytes len] + [5 bytes data] = 9 bytes.
    // So second offset should be 12 + 9 = 21.
    ASSERT_EQ(off2, 21, "Second offset calculation wrong");

    // "Second" length is 6. Storage: [4 bytes len] + [6 bytes data] = 10 bytes.
    // So third offset should be 21 + 10 = 31.
    ASSERT_EQ(off3, 31, "Third offset calculation wrong");

    // 2. Verify Values
    ASSERT_EQ(t.GetValue(&schema, 0).GetAsString(), "First", "Read V1 failed");
    ASSERT_EQ(t.GetValue(&schema, 1).GetAsString(), "Second", "Read V2 failed");
    ASSERT_EQ(t.GetValue(&schema, 2).GetAsString(), "Third", "Read V3 failed");

    LogPass("Multi-Varchar Heap Packing");
}

// =========================================================================
// TEST 3: Binary Safety (Updated for Real-World Constraints)
// =========================================================================
void TestBinaryStrings() {
    std::cout << "\n--- Test 3: Binary Safety (Embedded Nulls) ---" << std::endl;

    std::vector<Column> cols = {
        Column("bin_char", TypeId::CHAR, 10),
        Column("bin_var", TypeId::VARCHAR, 20)
    };
    Schema schema(cols);

    // Construct strings with null bytes in the middle
    // "A\0B" (Length 3)
    std::string bin_data("A\0B", 3);

    std::vector<Value> vals = {
        Value(TypeId::CHAR, bin_data),
        Value(TypeId::VARCHAR, bin_data)
    };
    Tuple t(vals, &schema);

    // 1. Check CHAR (Text Mode)
    // DESIGN CONSTRAINT: CHAR uses zero-padding, so it stops at the first null.
    // It implies CHAR cannot store binary data with embedded nulls.
    Value v_char = t.GetValue(&schema, 0);

    // We expect it to be TRUNCATED to "A" (Length 1)
    ASSERT_EQ(v_char.GetAsString().length(), 1, "CHAR should stop at first null (Text Behavior)");
    ASSERT_EQ(v_char.GetAsString()[0], 'A', "CHAR content check");

    // 2. Check VARCHAR (Binary Mode)
    // VARCHAR stores an explicit length header in the heap.
    // It MUST preserve the embedded null.
    Value v_var = t.GetValue(&schema, 1);

    ASSERT_EQ(v_var.GetAsString().length(), 3, "VARCHAR must preserve data after null");
    ASSERT_EQ(v_var.GetAsString()[1], '\0', "VARCHAR lost the null byte");
    ASSERT_EQ(v_var.GetAsString()[2], 'B', "VARCHAR lost byte after null");

    LogPass("Binary Safety (Behavior Verified)");
}

// =========================================================================
// TEST 4: Boundary Conditions (CHAR Truncation)
// =========================================================================
void TestCharBoundaries() {
    std::cout << "\n--- Test 4: CHAR Truncation & Boundaries ---" << std::endl;

    std::vector<Column> cols = { Column("c", TypeId::CHAR, 4) };
    Schema schema(cols);

    // Case A: Exact Fit (Length 4)
    {
        Tuple t({ Value(TypeId::CHAR, "1234") }, &schema);
        ASSERT_EQ(t.GetValue(&schema, 0).GetAsString(), "1234", "Exact fit failed");
    }

    // Case B: Truncation (Length 5 -> 4)
    {
        Tuple t({ Value(TypeId::CHAR, "12345") }, &schema);
        ASSERT_EQ(t.GetValue(&schema, 0).GetAsString(), "1234", "Truncation failed");
    }

    // Case C: Empty String
    {
        Tuple t({ Value(TypeId::CHAR, "") }, &schema);
        ASSERT_EQ(t.GetValue(&schema, 0).GetAsString(), "", "Empty string failed");
        // Verify manual padding check
        const char* d = t.GetData();
        ASSERT_EQ(d[0], 0, "Empty string byte 0 not zero");
        ASSERT_EQ(d[3], 0, "Empty string byte 3 not zero");
    }

    LogPass("CHAR Boundary Conditions");
}

// =========================================================================
// TEST 5: The "Wide Table" (Many Columns)
// =========================================================================
void TestWideTable() {
    std::cout << "\n--- Test 5: Wide Table (Stress Offsets) ---" << std::endl;

    // Create a schema with 100 INTEGER columns
    std::vector<Column> cols;
    std::vector<Value> vals;
    int num_cols = 100;

    for (int i = 0; i < num_cols; i++) {
        cols.emplace_back("col_" + std::to_string(i), TypeId::INTEGER);
        vals.emplace_back(TypeId::INTEGER, i * 10);
    }
    Schema schema(cols);

    // Verify schema size: 100 * 4 = 400 bytes
    ASSERT_EQ(schema.GetLength(), 400, "Wide Schema size wrong");

    Tuple t(vals, &schema);
    ASSERT_EQ(t.GetSize(), 400, "Wide Tuple size wrong");

    // Check random columns
    ASSERT_EQ(t.GetValue(&schema, 0).GetAsInteger(), 0, "Col 0 wrong");
    ASSERT_EQ(t.GetValue(&schema, 50).GetAsInteger(), 500, "Col 50 wrong");
    ASSERT_EQ(t.GetValue(&schema, 99).GetAsInteger(), 990, "Col 99 wrong");

    LogPass("Wide Table (100 Columns)");
}

int main() {
    std::cout << "=== RUNNING COMPREHENSIVE TUPLE TEST SUITE ===\n";

    TestKitchenSink();
    TestMultiVarchar();
    TestBinaryStrings();
    TestCharBoundaries();
    TestWideTable();

    std::cout << "\n=== ALL TESTS PASSED SUCCESSFULLY ===" << std::endl;
    return 0;
}