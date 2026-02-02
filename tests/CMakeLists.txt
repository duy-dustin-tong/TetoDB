# Use google test framework for testing
enable_testing()

include(FetchContent)
FetchContent_Declare(
	googletest
	GIT_REPOSITORY https://github.com/google/googletest.git
	GIT_TAG        v1.15.2 # Specify the version tag here
)
FetchContent_MakeAvailable(googletest)

# 1. Create the test executable
# You can add multiple test files here (e.g., PagerTests.cpp, etc)
add_executable(DatabaseTests
    DatabaseTests.cpp
)

# 2. Link the necessary libraries
# Link against our database library and GTest
target_link_libraries(DatabaseTests
    PRIVATE
        TetoDBLib     # Database logic
        GTest::gtest_main
)

# 3. Enable GTest's automatic test discovery
# This makes each TEST() show up as an individual item in IDE test runners
include(GoogleTest)
gtest_discover_tests(DatabaseTests)