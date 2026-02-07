cmake_minimum_required(VERSION 3.10)
project(tetodb)

set(CMAKE_CXX_STANDARD 17)

# 1. Include Directories
include_directories(src/include)

# 2. Define the Database Engine Library
# We explicitly list the implementation files here.
# NOTE: Do NOT include main.cpp here.
add_library(tetodb_lib
    src/implementation/storage/buffer/buffer_pool_manager.cpp
    
    src/implementation/storage/disk/disk_manager.cpp
 "src/include/storage/buffer/two_queue_replacer.h" "src/implementation/storage/buffer/two_queue_replacer.cpp" "src/implementation/storage/page/table_page.cpp" "src/implementation/storage/table/table_heap.cpp" "src/include/storage/page/page_guard.h" "test/storage_test.cpp" "src/implementation/type/value.cpp"  "src/implementation/type/tuple.cpp" "src/include/type/type.h" "src/implementation/type/type.cpp")

# 3. Create the Executable
# We only compile main.cpp here, and then link it to the library above.
add_executable(teto_main src/main.cpp "src/include/storage/buffer/two_queue_replacer.h" "src/implementation/storage/buffer/two_queue_replacer.cpp" "src/implementation/storage/page/table_page.cpp" "src/implementation/storage/table/table_heap.cpp" "src/include/storage/page/page_guard.h"  "src/implementation/type/value.cpp"  "src/implementation/type/tuple.cpp" "src/include/type/type.h" "src/implementation/type/type.cpp")

# 4. Link the Executable to the Library
target_link_libraries(teto_main tetodb_lib)