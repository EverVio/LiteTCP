cmake_minimum_required(VERSION 3.12)
project(LiteTCP CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置规范的构建产物输出路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# 包含头文件目录
include_directories(include)

# 核心源代码
set(LITETCP_SOURCES
    src/TcpPacket.cpp
    src/TcpSocket.cpp
    src/TcpSocket_State.cpp
    src/TcpSocket_Timer.cpp
    src/NetworkEngine.cpp
    src/TimerManager.cpp
    src/litetcp.cpp
)

# 编译 LiteTCP 静态库
add_library(litetcp STATIC ${LITETCP_SOURCES})



# 自动化测试可执行文件
add_executable(test_connect tests/test_connect.cpp)
target_link_libraries(test_connect litetcp)

add_executable(test_close tests/test_close.cpp)
target_link_libraries(test_close litetcp)

add_executable(test_reliable tests/test_reliable.cpp)
target_link_libraries(test_reliable litetcp)
