cmake_minimum_required(VERSION 3.10)
project(SimpleKVServer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add your source file
add_executable(kvserver
    main.cpp
    hashtable.cpp
    zset.cpp
    heap.cpp
    avl.cpp
   
)

# Link any required libraries
target_link_libraries(kvserver pthread)
