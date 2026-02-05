cmake_minimum_required(VERSION 3.10)

# Project Name
project(tetodb)

# Set C++ Standard (We need C++17 for cool features)
set(CMAKE_CXX_STANDARD 17)

# 1. Include Directories
# Tells C++ where to look for .h files
include_directories(src/include)

# 2. Gather Source Files
# (Later we will be more specific, but for now, this grabs all .cpp files)
file(GLOB_RECURSE SOURCES "src/*.cpp")

# 3. Create the Executable
# Creates a program named "teto_shell" using the source files
add_executable(teto_shell tools/shell/shell.cpp ${SOURCES})