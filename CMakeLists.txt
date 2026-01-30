cmake_minimum_required(VERSION 3.20)

project(TetoDB)

set(CMAKE_CXX_STANDARD 23)

file(GLOB CPP_SOURCES "*.cpp")

add_executable(TetoDB ${CPP_SOURCES})

# Use google test framework for testing
enable_testing()

include(FetchContent)
FetchContent_Declare(
	googletest
	GIT_REPOSITORY https://github.com/google/googletest.git
	GIT_TAG        v1.15.2 # Specify the version tag here
)
FetchContent_MakeAvailable(googletest)

add_executable(DatabaseTests 
	tests/DatabaseTests.cpp
	Database.cpp
	Schema.cpp
	Btree.cpp
	Pager.cpp
)
target_link_libraries(DatabaseTests GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(DatabaseTests)