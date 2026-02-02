cmake_minimum_required(VERSION 3.20)

project(TetoDB)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Compile database logic into a library and then link the main function and 
# tests to that library
add_subdirectory(src)
add_subdirectory(tests)