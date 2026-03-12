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

add_executable(test_bug16_like_microbenchmark src/test_bug16_like_microbenchmark.cpp)
target_link_libraries(test_bug16_like_microbenchmark tetodb_lib)

add_executable(test_bug17_div_microbenchmark src/test_bug17_div_microbenchmark.cpp)
target_link_libraries(test_bug17_div_microbenchmark tetodb_lib)

add_executable(test_bug18_bpm_eviction src/test_bug18_bpm_eviction.cpp)
target_link_libraries(test_bug18_bpm_eviction tetodb_lib)

add_executable(test_bug19_fk_cascade src/test_bug19_fk_cascade.cpp)
target_link_libraries(test_bug19_fk_cascade tetodb_lib)

add_executable(test_bug20_arithmetic src/test_bug20_arithmetic.cpp)
target_link_libraries(test_bug20_arithmetic tetodb_lib)

add_executable(test_bug7 "src/test_bug7_compaction.cpp")
target_link_libraries(test_bug7 tetodb_lib)

add_executable(test_c1_stale_offset "src/test_c1_stale_offset.cpp")
target_link_libraries(test_c1_stale_offset tetodb_lib)

add_executable(test_c2_newpage_wal "src/test_c2_newpage_wal.cpp")
target_link_libraries(test_c2_newpage_wal tetodb_lib)

add_executable(test_h1_multi_column_fk "src/test_h1_multi_column_fk.cpp")
target_link_libraries(test_h1_multi_column_fk tetodb_lib)

add_executable(test_h2_recovery_update "src/test_h2_recovery_update.cpp")
target_link_libraries(test_h2_recovery_update tetodb_lib)

# 5. Link the Core Library to the Server
target_link_libraries(teto_main tetodb_lib)

# 6. Link Windows Sockets to Both Executables
if(WIN32)
    target_link_libraries(teto_main ws2_32)
    target_link_libraries(teto_client ws2_32)
endif()

add_executable(test_m10_drop_table_fk_cleanup src/test_m10_drop_table_fk_cleanup.cpp)
target_link_libraries(test_m10_drop_table_fk_cleanup tetodb_lib)
