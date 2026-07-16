# 构建 rpc_core 静态库
# 将 message.pb.cc（protobuf 生成的序列化代码）编译成静态库
add_library(rpc_core STATIC
    src/message.pb.cc
    src/toolFunc.cc
    src/TaskThreadPool.cc
)

# 设置公开包含路径，供外部目标使用
# ${CMAKE_CURRENT_SOURCE_DIR}/include 即 rpc_core/include，
# 使得链接 rpc_core 的目标可以用 #include "rpc_server.h" / "message.pb.h"
target_include_directories(rpc_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 链接 rpc_core 所需的库
target_link_libraries(rpc_core PUBLIC
    ClearMoon          # 链接 clearmoon 网络库（TcpServer、EventLoop 等）
    protobuf::libprotobuf  # 链接 protobuf 运行时库（支持 message 序列化/反序列化）
)

find_package(absl REQUIRED)

# RPC 服务端测试
add_executable(Rpc_Server_test
    Test/Rpc_Server.cc
    src/rpc_server.cc
)
target_include_directories(Rpc_Server_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/lib/ClearMoon
)
target_link_libraries(Rpc_Server_test
    ClearMoon
    rpc_core
    protobuf::libprotobuf
    absl::log_internal_check_op
    absl::log_internal_message
    pthread
)
