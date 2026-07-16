cmake_minimum_required(VERSION 4.0)
project(CMRPC)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_definitions(PROJECT_ROOT="${CMAKE_SOURCE_DIR}")

# 查找 protobuf
find_package(Protobuf REQUIRED)

# 查找 abseil
find_package(absl REQUIRED)

# 添加 clearmoon 子目录
add_subdirectory(lib/ClearMoon)

# 添加 rpc 核心库
add_subdirectory(rpc_core)

# 添加 RPC 客户端
add_subdirectory(Client)

# 异步客户端测试（C++20 协程）
add_executable(Rpc_Async_Client_Test
    Rpc_Async_Client_Test.cc
    Client/client.cc
    Client/ConnectionPool.cc
    Client/PooledConnection.cc
)
target_include_directories(Rpc_Async_Client_Test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/Client
    ${CMAKE_SOURCE_DIR}/rpc_core/include
    ${CMAKE_SOURCE_DIR}/lib/ClearMoon
)
target_link_libraries(Rpc_Async_Client_Test
    ClearMoon
    rpc_core
    protobuf::libprotobuf
    pthread
    absl::log_internal_check_op
    absl::log_internal_message
)
# 需要 C++20 以支持协程
target_compile_features(Rpc_Async_Client_Test PRIVATE cxx_std_20)

# # RPC 服务端
# add_executable(Rpc_Server
#     rpc_core/Test/Rpc_Server.cc
#     rpc_core/src/rpc_server.cc
#     rpc_core/src/TaskThreadPool.cc
# )
# target_include_directories(Rpc_Server PRIVATE
#     ${CMAKE_SOURCE_DIR}/rpc_core/include
#     ${CMAKE_SOURCE_DIR}/lib/ClearMoon
# )
# target_link_libraries(Rpc_Server
#     ClearMoon
#     rpc_core
#     protobuf::libprotobuf
#     pthread
#     absl::log_internal_check_op
#     absl::log_internal_message
# )

# RPC 服务端测试（带业务逻辑，与 Rpc_Async_Client_Test 配合）
add_executable(Rpc_Server_WithLogic
    Rpc_Server_test.cc
    rpc_core/src/rpc_server.cc
    rpc_core/src/TaskThreadPool.cc
)
target_include_directories(Rpc_Server_WithLogic PRIVATE
    ${CMAKE_SOURCE_DIR}/rpc_core/include
    ${CMAKE_SOURCE_DIR}/lib/ClearMoon
)
target_link_libraries(Rpc_Server_WithLogic
    ClearMoon
    rpc_core
    protobuf::libprotobuf
    pthread
    absl::log_internal_check_op
    absl::log_internal_message
)
