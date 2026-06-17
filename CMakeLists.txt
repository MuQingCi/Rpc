cmake_minimum_required(VERSION 4.0)
project(CMRPC)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 查找 protobuf
find_package(Protobuf REQUIRED)

# 添加 clearmoon 子目录
add_subdirectory(lib/ClearMoon)

# 添加 rpc 核心库
add_subdirectory(rpc_core)