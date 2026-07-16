cmake_minimum_required(VERSION 3.10)
project(ClearMoon)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 生成 compile_commands.json 供 clangd 等工具使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 收集源文件（使用 CMAKE_CURRENT_SOURCE_DIR 指向 lib/ClearMoon/）
file(GLOB_RECURSE LIB_SRCS CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/net/*.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/base/*.cc
)

# 生成静态库
add_library(ClearMoon STATIC ${LIB_SRCS})

# 设置 PUBLIC 包含目录：外部链接者可用 #include "net/..." 引入头文件
target_include_directories(ClearMoon PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# add_compile_definitions(PROJECT_ROOT="${CMAKE_CURRENT_SOURCE_DIR}")

# add_executable(HttpServer_test
#     ${LIB_SRCS}
#     ${CMAKE_CURRENT_SOURCE_DIR}/test/HttpServer_test.cc
#     ${CMAKE_CURRENT_SOURCE_DIR}/test/HttpCallback.cc
# )
# target_include_directories(HttpServer_test PRIVATE
#     ${CMAKE_CURRENT_SOURCE_DIR}
#     ${CMAKE_CURRENT_SOURCE_DIR}/net
#     # ${CMAKE_CURRENT_SOURCE_DIR}/net/Http
#     ${CMAKE_CURRENT_SOURCE_DIR}/base
#     ${CMAKE_CURRENT_SOURCE_DIR}/net/Poller
#     ${CMAKE_CURRENT_SOURCE_DIR}/net/Log
# )
# set_target_properties(HttpServer_test PROPERTIES CXX_EXTENSIONS OFF)