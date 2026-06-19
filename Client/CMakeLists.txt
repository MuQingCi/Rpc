# RPC 客户端测试
find_package(absl REQUIRED)

add_executable(Rpc_Client_test
    client.cc
    Test/Rpc_Client.cc
)
target_include_directories(Rpc_Client_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/rpc_core/include
    ${CMAKE_SOURCE_DIR}/lib/ClearMoon
)
target_link_libraries(Rpc_Client_test
    ClearMoon
    rpc_core
    protobuf::libprotobuf
    pthread
    absl::log_internal_check_op
    absl::log_internal_message
)