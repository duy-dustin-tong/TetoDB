cmake_minimum_required(VERSION 3.20)

project(TetoDB)

set(CMAKE_CXX_STANDARD 23)

file(GLOB CPP_SOURCES "*.cpp")

add_executable(TetoDB ${CPP_SOURCES})