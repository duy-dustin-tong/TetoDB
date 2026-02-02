# Collect all source files
file(GLOB_RECURSE SOURCES "*.cpp")

# Exclude main.cpp from the library so tests can have their own entry point
list(REMOVE_ITEM SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/TetoDB.cpp")

# Create the library
add_library(TetoDBLib ${SOURCES})

# Specify where headers are
target_include_directories(TetoDBLib PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Create the CLI executable
add_executable(TetoDB "${CMAKE_CURRENT_SOURCE_DIR}/TetoDB.cpp")
target_link_libraries(TetoDB PRIVATE TetoDBLib)