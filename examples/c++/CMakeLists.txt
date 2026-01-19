cmake_minimum_required(VERSION 3.13)
cmake_policy(SET CMP0077 NEW)
project(zvec-example-c++)
set(CMAKE_CXX_STANDARD 17)

# Enable compile commands generation
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(../../src/include)
link_directories(../../build/lib)

add_executable(example main.cc)
target_link_libraries(example zvec_db)