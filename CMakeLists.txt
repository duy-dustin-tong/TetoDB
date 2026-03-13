# ==============================================================================
# TetoDB: Lightweight Relational Database Engine
# 
# KEY FEATURES & RECENT FIXES:
# - Full SQL Pipeline (Lexer -> Parser -> Planner -> Optimizer -> Executor)
# - B+ Tree Indexing & Explicit Foreign Key Constraint Validation
# - Strict 2PL Concurrency Control (Thread-safe, mutex-safe, no lock ordering bugs)
# - Write-Ahead Logging (WAL) with ARIES Recovery
# - Column Metadata Persists: NOT NULL, UNIQUE, PRIMARY KEY status survives restarts.
# - Data Types: INTEGER, BIGINT, SMALLINT, TINYINT, BOOLEAN, DECIMAL, VARCHAR, CHAR, TIMESTAMP
# - Supported Statements: SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE/INDEX
# - Aggregations: COUNT, SUM, MIN, MAX, AVG, MEDIAN (Empty tables properly return 0/NULL)
# - Robust LIKE/ILIKE string matching (Iterative backtracking, stack-overflow safe)
# - Escaped Single Quotes Supported ('O''Brien')
# 
# SQLALCHEMY / ORM LIMITATIONS (Must Read!):
# - SAVEPOINT Support: SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO with partial rollback
# - No DEALLOCATE support for prepared statements (throws error).
# - Positional INSERTS Only: `INSERT INTO table VALUES (...)` (No column targeting).
# - No Auto-Increment: Primary keys must be explicitly provided in inserts.
# - No ALTER TABLE schema modifications.
# - No Information Schema: Reflection is limited.
# ==============================================================================

cmake_minimum_required(VERSION 3.10)
project(tetodb)

set(CMAKE_CXX_STANDARD 17)

# 1. Include Directories
include_directories(src/include)

# 2. Define the Database Engine Library
add_library(tetodb_lib
    src/implementation/storage/buffer/buffer_pool_manager.cpp
    src/implementation/storage/disk/disk_manager.cpp
    src/implementation/storage/buffer/two_queue_replacer.cpp
    src/implementation/storage/page/table_page.cpp
    src/implementation/storage/table/table_heap.cpp
    src/implementation/type/value.cpp
    src/implementation/storage/table/tuple.cpp
    src/implementation/type/type.cpp
    src/implementation/catalog/catalog.cpp
    src/implementation/execution/executors/seq_scan_executor.cpp
    src/implementation/storage/table/table_iterator.cpp
    src/implementation/execution/executors/insert_executor.cpp
    src/implementation/storage/page/b_plus_tree_leaf_page.cpp
    src/implementation/storage/page/b_plus_tree_internal_page.cpp
    src/implementation/index/b_plus_tree.cpp
    src/implementation/execution/executors/index_scan_executor.cpp
    src/implementation/concurrency/lock_manager.cpp
    src/implementation/concurrency/transaction_manager.cpp
    src/implementation/execution/executors/delete_executor.cpp
    src/implementation/execution/fk_constraint_handler.cpp
    src/implementation/execution/executors/update_executor.cpp
    src/implementation/recovery/log_record.cpp
    src/implementation/recovery/log_manager.cpp
    src/implementation/recovery/recovery_manager.cpp
    src/implementation/execution/executors/nested_loop_join_executor.cpp
    src/implementation/execution/executors/projection_executor.cpp
    src/implementation/execution/executors/aggregation_executor.cpp
    src/implementation/execution/executors/hash_join_executor.cpp
    src/implementation/parser/lexer.cpp
    src/implementation/parser/parser.cpp
    src/implementation/planner/planner.cpp
    src/implementation/optimizer/optimizer.cpp
    "src/implementation/execution/executors/execution_engine.cpp"
    "src/include/execution/plans/insert_plan.h" 
    "src/include/execution/plans/delete_plan.h" 
    "src/include/execution/plans/filter_plan.h" 
    "src/include/execution/executors/filter_executor.h" 
    "src/implementation/execution/executors/filter_executor.cpp" 
    "src/include/execution/plans/sort_plan.h" 
    "src/include/execution/executors/sort_executor.h" 
    "src/implementation/execution/executors/sort_executor.cpp" 
    "src/include/execution/plans/limit_plan.h" 
    "src/include/execution/executors/limit_executor.h" 
    "src/implementation/execution/executors/limit_executor.cpp" 
    "src/implementation/recovery/checkpoint_manager.cpp" 
    "src/include/concurrency/index_write_record.h" 
    "src/include/execution/plans/topn_plan.h" 
    "src/include/execution/executors/topn_executor.h" 
    "src/implementation/execution/executors/topn_executor.cpp"
    "src/include/execution/plans/distinct_plan.h"
    "src/include/execution/executors/distinct_executor.h"
    "src/implementation/execution/executors/distinct_executor.cpp"
    "src/include/execution/plans/set_op_plan.h"
    "src/include/execution/executors/set_op_executor.h"
    "src/implementation/execution/executors/set_op_executor.cpp"
    "src/include/execution/executors/distinct_executor.h"
    "src/implementation/execution/executors/distinct_executor.cpp"
    "src/implementation/server/tetodb_instance.cpp" 
    "src/include/server/tcp_server.h" 
    "src/implementation/server/tcp_server.cpp"
 "src/include/execution/expressions/parameter_value_expression.h")

# 3. Create the Server Executable (make sure this is named server_main.cpp)
add_executable(teto_main "src/server_main.cpp")

# 4. Create the Client Executable
add_executable(teto_client "src/client_main.cpp" "src/include/execution/expressions/parameter_value_expression.h")












# 5. Link the Core Library to the Server
target_link_libraries(teto_main tetodb_lib)

# 6. Link Windows Sockets to Both Executables
if(WIN32)
    target_link_libraries(teto_main ws2_32)
    target_link_libraries(teto_client ws2_32)
endif()


# ---------------------------------------------------------
# Googletest Integration
# ---------------------------------------------------------
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(CMAKE_TLS_VERIFY 0)
FetchContent_MakeAvailable(googletest)

add_executable(tetodb_tests src/test_units.cpp)
target_link_libraries(tetodb_tests tetodb_lib gtest_main)

if(WIN32)
    target_link_libraries(tetodb_tests ws2_32)
endif()

include(GoogleTest)
gtest_discover_tests(tetodb_tests)

# ---------------------------------------------------------
# Google Benchmark Integration
# ---------------------------------------------------------
FetchContent_Declare(
  googlebenchmark
  URL https://github.com/google/benchmark/archive/refs/tags/v1.8.3.zip
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Disable benchmark tests" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "Disable benchmark install" FORCE)
FetchContent_MakeAvailable(googlebenchmark)

add_executable(tetodb_benchmarks src/benchmarks.cpp)
target_link_libraries(tetodb_benchmarks tetodb_lib benchmark::benchmark_main)

if(WIN32)
    target_link_libraries(tetodb_benchmarks shlwapi)
    target_link_libraries(tetodb_benchmarks ws2_32)
endif()

add_executable(test_bug22_empty_agg test/test_empty_agg.cpp)
target_link_libraries(test_bug22_empty_agg tetodb_lib GTest::gtest_main GTest::gtest)
if(WIN32)
    target_link_libraries(test_bug22_empty_agg shlwapi ws2_32)
endif()
add_test(NAME test_bug22_empty_agg COMMAND test_bug22_empty_agg)

add_executable(test_like_bug test/test_like_bug.cpp)
target_link_libraries(test_like_bug tetodb_lib GTest::gtest_main GTest::gtest)
if(WIN32)
    target_link_libraries(test_like_bug shlwapi ws2_32)
endif()
add_test(NAME test_like_bug COMMAND test_like_bug)

add_executable(test_savepoint test/test_savepoint.cpp)
target_link_libraries(test_savepoint tetodb_lib GTest::gtest_main GTest::gtest)
if(WIN32)
    target_link_libraries(test_savepoint shlwapi ws2_32)
endif()
add_test(NAME test_savepoint COMMAND test_savepoint)
