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
 "src/include/storage/buffer/two_queue_replacer.h" "src/implementation/storage/buffer/two_queue_replacer.cpp")

# 3. Create the Executable
# We only compile main.cpp here, and then link it to the library above.
add_executable(teto_main src/main.cpp "src/include/storage/buffer/two_queue_replacer.h" "src/implementation/storage/buffer/two_queue_replacer.cpp")

# 4. Link the Executable to the Library
target_link_libraries(teto_main tetodb_lib)