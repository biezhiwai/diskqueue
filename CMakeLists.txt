cmake_minimum_required(VERSION 3.16)
project(diskqueue)

set(CMAKE_CXX_STANDARD 17)

link_libraries(hiredis)
link_libraries(redis++)
link_libraries(pthread)
link_libraries(rt)
link_libraries(boost_filesystem)
link_libraries(boost_system)

add_executable(diskqueue diskqueue.cpp)
