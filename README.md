# CMRPC — 基于 C++20 协程的高性能 RPC 框架

[![C++20](https://img.shields.io/badge/C++-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![Protocol Buffers](https://img.shields.io/badge/Protobuf-3.0+-brightgreen.svg)](https://developers.google.com/protocol-buffers)

**CMRPC** 是一个基于 [ClearMoon 网络库](/lib/ClearMoon) 构建的现代 C++ RPC 框架，支持 **C++20 协程异步调用**、**连接池管理**、**多种服务发现后端**（etcd / 文件配置）、**多种负载均衡策略**、**可插拔过滤器链**（分布式追踪 / 指标埋点 / 限流 / 熔断），并提供完备的异常处理、连接健康检查与**应用层 ACK 超时重传**机制。

---

## 目录

- [特性概览](#特性概览)
- [项目结构](#项目结构)
- [依赖项](#依赖项)
- [构建指南](#构建指南)
- [快速开始](#快速开始)
  - [1. 定义 Protobuf 协议](#1-定义-protobuf-协议)
  - [2. 实现 RPC 服务端](#2-实现-rpc-服务端)
  - [3. 实现 RPC 客户端（协程调用）](#3-实现-rpc-客户端协程调用)
  - [4. 运行](#4-运行)
- [架构设计](#架构设计)
  - [整体架构图](#整体架构图)
  - [通信协议](#通信协议)
  - [服务端核心流程](#服务端核心流程)
  - [客户端核心流程](#客户端核心流程)
  - [协程调用机制](#协程调用机制)
- [过滤器链](#过滤器链)
  - [接口设计](#接口设计)
  - [内置过滤器](#内置过滤器)
  - [使用方式](#使用方式)
- [服务注册与发现](#服务注册与发现)
  - [接口设计](#接口设计-1)
  - [文件配置注册中心 (FileConfigRegister)](#文件配置注册中心-fileconfigregister)
  - [Etcd 注册中心 (EtcdRegister / EtcdDiscovery)](#etcd-注册中心-etcdregister--etcddiscovery)
  - [ServiceDiscoverer 封装](#servicediscoverer-封装)
- [连接池与负载均衡](#连接池与负载均衡)
  - [连接池架构](#连接池架构)
  - [负载均衡策略](#负载均衡策略)
  - [健康检查](#健康检查)
- [RPC Client 详解](#rpc-client-详解)
  - [构造函数总览](#构造函数总览)
  - [静态模式](#静态模式)
  - [动态服务模式](#动态服务模式)
  - [协程调用示例](#协程调用示例)
- [错误处理与异常](#错误处理与异常)
  - [异常类型](#异常类型)
  - [协议错误码](#协议错误码)
- [配置与服务注册](#配置与服务注册)
- [测试示例](#测试示例)
- [许可证](#许可证)

---

## 特性概览

| 特性                | 说明                                                                 |
| ------------------- | -------------------------------------------------------------------- |
| **C++20 协程异步**   | 基于 `Task<T>` 与 `RpcAwaiter` 的 `co_await` 异步 RPC 调用，不依赖第三方协程库 |
| **连接池管理**       | 静态地址池 + 动态服务池，`Borrowed` RAII 句柄管理连接借用与归还          |
| **负载均衡**         | 轮询（RoundRobin）、最少连接（LeastConnection）、随机（Random）         |
| **连接健康检查**     | 定时检测连接状态，自动标记 UNHEALTHY 并重连失效连接                      |
| **服务注册与发现**   | 抽象接口 `isServiceRegister` / `isServiceDiscovery`，多种后端实现      |
| **Etcd 集成**        | 基于 etcd Lease + KeepAlive 的服务注册，Watch 机制的服务发现           |
| **文件配置中心**     | 基于 YAML 文件的注册中心，支持 Server / Client / Both 模式              |
| **过滤器链**         | 可插拔过滤器：TraceFilter / MetricsFilter / RateLimitFilter / CircuitBreakerFilter  |
| **应用层超时重传**   | 请求携带 retransmit 标记，对端立即回 ACK，超时自动重传                   |
| **YAML 配置驱动**    | `ServerConfig` / `ClientConfig` 从 YAML 加载，消除配置硬编码           |
| **Protobuf 序列化**  | 使用 Protocol Buffers 作为消息序列化格式                               |
| **请求超时控制**     | 细粒度超时控制 + 超时自动清理                                          |
| **任务线程池**       | 服务端将业务逻辑异步分派到线程池执行，不阻塞 IO 线程                     |


## 项目结构

```
RPC/
├── CMakeLists.txt                  # 顶层 CMake 构建文件（C++20）
├── config/                         # YAML 配置文件
│   ├── client.yaml                 # 客户端运行配置
│   └── server.yaml                 # 服务端运行配置
├── Client/                         # RPC 客户端库
│   ├── Client.h / Client.cc        # RPCClient 主类（协程调用入口）
│   ├── ClientConfig.h              # 客户端配置结构体（loadClientConfig）
│   ├── ConnectionPool.h / .cc      # 连接池（静态/动态池 + 负载均衡 + 健康检查）
│   ├── PooledConnection.h / .cc    # 池化连接（请求上下文/超时/重传/过滤器）
│   ├── RpcAwaiter.h                # 协程 Awaiter 实现
│   ├── Rpc_exceptions.h            # RPC 异常定义
│   ├── Task.h                      # 通用协程 Task<T> 类型
│   ├── CMakeLists.txt
│   └── Test/Rpc_Client.cc          # 早期客户端测试（旧 API，未同步编译）
├── rpc_core/                       # RPC 核心库
│   ├── include/
│   │   ├── Rpc_server.h            # RPCServer 服务端主类
│   │   ├── ToolFunc.h              # 协议编解码、Header/RPC_Meta、错误码、方法ID
│   │   ├── Message.proto           # Protobuf 消息定义（Echo / Add）
│   │   ├── TaskThreadPool.h        # 业务任务线程池
│   │   ├── ServerConfig.h          # 服务端配置结构体（loadServerConfig）
│   │   ├── Agreement.md            # 接口约定（getLocalIp 说明等）
│   │   ├── Filter/                 # 过滤器链
│   │   │   ├── RpcFilter.h         # 过滤器抽象基类（before / after）
│   │   │   ├── RpcFilterChain.h    # 过滤器链（顺序 before / 逆序 after）
│   │   │   ├── TraceFilter.h       # 分布式 ID 追踪过滤器
│   │   │   ├── MetricsFilter.h     # 指标埋点过滤器
│   │   │   ├── RateLimitFilter.h   # 流量控制过滤器（令牌桶）
│   │   │   ├── CircuitBreakerFilter.h  # 熔断过滤器
│   │   │   ├── TokenBucket.h       # 令牌桶
│   │   │   └── MetricsCollector.h  # 指标收集器（单例）
│   │   └── Service/                # 服务注册与发现
│   │       ├── Endpoint.h          # 端点数据结构
│   │       ├── IsServiceRegister.h # 服务注册抽象接口
│   │       ├── IsServiceDiscovery.h# 服务发现抽象接口
│   │       ├── Registry.h          # 复合接口（注册+发现）
│   │       ├── FileConfigRegister.h# 文件配置注册中心
│   │       ├── EtcdRegister.h      # Etcd 服务注册
│   │       ├── EtcdDiscovery.h     # Etcd 服务发现
│   │       └── ServiceDiscoverer.h # 服务发现封装器
│   ├── src/
│   │   ├── Rpc_server.cc
│   │   ├── ToolFunc.cc
│   │   ├── Message.pb.cc
│   │   ├── TaskThreadPool.cc
│   │   ├── FileConfigRegister.cc
│   │   ├── EtcdRegister.cc
│   │   ├── EtcdDiscovery.cc
│   │   └── RpcFilterChain.cc
│   └── Test/
│       ├── Rpc_Server.cc           # 早期服务端测试（未同步编译）
│       └── test_etcd.cpp           # etcd 手动测试
├── lib/ClearMoon/                  # 底层网络库
│   ├── net/                        # 网络核心（EventLoop, TcpServer, TcpClient, ...）
│   ├── net/Poller/                 # Epoll IO 多路复用
│   ├── net/Http/                   # HTTP 支持
│   ├── net/Log/                    # 异步日志
│   └── base/                       # 基础组件（ThreadPool, BlockQueue...）
├── Rpc_Async_Client_Test.cc        # 异步客户端协程测试入口（读 config/client.yaml）
├── Rpc_Server_test.cc              # 服务端带业务逻辑测试（构建 Rpc_Server_WithLogic）
└── server_timeout_example.cc       # 早期超时处理示例（未编译）
```

## 依赖项

| 依赖            | 版本要求     | 用途                                                        |
| ----------------| ----------- | ------------------------------------------------------------|
| **ClearMoon**   | C++17       | 底层网络库（项目内子目录 `lib/ClearMoon`）                     |
| **Protobuf**    | ≥ 3.0       | 消息序列化                                                   |
| **Abseil**      | LTS         | 日志、错误检查等（`absl::log_*`）                             |
| **yaml-cpp**    | ≥ 0.6       | YAML 配置文件解析（FileConfig / ServerConfig / ClientConfig） |
| **cpprestsdk**  | ≥ 2.10      | HTTP 客户端库（etcd-cpp-api 底层依赖）                        |
| **etcd-cpp-api**| ≥ 0.1       | etcd 客户端 C++ API（含 nlohmann/json）                       |
| **CMake**       | ≥ 3.10      | 构建系统                                                     |

> 注意：`etcd-cpp-api` 在 `rpc_core/CMakeLists.txt` 中以 `libetcd-cpp-api.so` 直接链接（`/usr/local/lib/`），头文件路径直接指定为 `/usr/local/include`，以避免其 CMake config 触发 gRPC→protobuf 版本冲突。

## 构建指南

```bash
# 1. 克隆项目
git clone https://github.com/MuQingCi/Rpc.git
cd Rpc

# 2. 安装系统依赖（Ubuntu/Debian 示例）
sudo apt install cmake g++ libprotobuf-dev protobuf-compiler \
                 libyaml-cpp-dev libcpprest-dev libcurl4-openssl-dev

# etcd-cpp-api 需手动编译安装
# 详见: https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3

# 3. 构建
mkdir build && cd build
cmake ..
make -j$(nproc)

# 4. 配置（可选，config/ 下已提供默认配置）
# 编辑 config/server.yaml 与 config/client.yaml 中的注册中心、端口、过滤器参数

# 5. 运行
# 先启动 etcd（如果使用 etcd 服务发现）
./etcd --data-dir=./default.etcd &

./build/Rpc_Server_WithLogic     # 终端1：启动服务端
./build/Rpc_Async_Client_Test    # 终端2：启动客户端测试
```

> 构建产物：
> - `Rpc_Server_WithLogic` — 服务端测试（`Rpc_Server_test.cc`，注册 Echo/Add 两个方法）
> - `Rpc_Async_Client_Test` — 客户端协程异步测试（`Rpc_Async_Client_Test.cc`）
> - `rpc_core/Test/Rpc_Server.cc` 与 `Client/Test/Rpc_Client.cc` 为早期测试文件，当前**未加入构建**。

## 快速开始

### 1. 定义 Protobuf 协议

`rpc_core/include/Message.proto`：

```protobuf
syntax = "proto3";

package CLRPC;

service SimpleServer{
    rpc Echo (EchoRequest) returns (EchoResponse);
    rpc Add (AddRequest) returns (AddResponse);
}

message EchoRequest{
    string msg = 1;
}

message EchoResponse{
    string reply = 1;
    int32 code = 2;
}

message AddRequest{
    int64 a = 1;
    int64 b = 2;
}

message AddResponse{
    int64 result = 1;
    string Error = 2;
}
```

方法 ID 由 `ToolFunc.h` 中的特化模板 `getMethodId<T>()` 统一映射（新增业务方法需扩展该模板）：

| 方法           | 请求类型               | 响应类型                 | ID |
| ------------- | ---------------------- | ----------------------- | -- |
| **Echo**      | `CLRPC::EchoRequest`   | `CLRPC::EchoResponse`   | 0  |
| **Add**       | `CLRPC::AddRequest`    | `CLRPC::AddResponse`    | 1  |

### 2. 实现 RPC 服务端

`Rpc_Server_test.cc`（构建为 `Rpc_Server_WithLogic`）给出了完整示例。核心流程如下：

```cpp
#include "Rpc_server.h"
#include "ServerConfig.h"
#include "Service/EtcdRegister.h"
#include "Service/FileConfigRegister.h"
#include "Message.pb.h"
#include "Filter/TraceFilter.h"
#include "Filter/MetricsFilter.h"
#include "Filter/RateLimitFilter.h"
#include "Filter/CircuitBreakerFilter.h"

int main()
{
    // 0. 从 YAML 加载服务端配置（config/server.yaml）
    auto cfg = loadServerConfig(PROJECT_ROOT "/config/server.yaml");

    cmlib::EventLoop loop;
    cmlib::InetAddress listenAddr(cfg.host, cfg.port, false); // 第三个参数: ipv6=false

    // 1. 业务任务线程池（业务逻辑在池中执行，不阻塞 IO 线程）
    auto taskPool = std::make_shared<TaskThreadPool>(cfg.threadPoolSize);
    taskPool->start();

    // 2. 创建注册中心（etcd / 文件配置二选一）
    std::shared_ptr<isServiceRegister> registry;
    if (cfg.registryType == "file") {
        registry = std::make_shared<FileConfigRegister>(
            &loop, cfg.filePath, cfg.pollInterval, RegistryMode::Server);
    } else {
        registry = std::make_shared<EtcdRegister>(
            &loop, taskPool, cfg.etcdUrl, cfg.ttl, cfg.keepaliveInterval);
    }

    // 3. 创建 RPCServer，并注册服务名
    RPCServer server(&loop, listenAddr, registry, cfg.threadPoolSize);
    server.setServiceWeight(cfg.serviceWeight);
    for (const auto& svc : cfg.services)
        server.addService(svc);

    // 4. 添加过滤器链（追踪 → 指标 → 限流 → 熔断）
    auto bucket = std::make_shared<TokenBucket>(cfg.rateLimit, cfg.bucketCapacity);
    if (cfg.trace)       server.addFilter(std::make_shared<TraceFilter>());
    if (cfg.metrics)     server.addFilter(std::make_shared<MetricsFilter>());
    if (cfg.rateLimit>0) server.addFilter(std::make_shared<RateLimitFilter>(bucket));
    server.addFilter(std::make_shared<CircuitBreakerFilter>(cfg.breakerErrorRate, cfg.breakerWindow));

    // 5. 注册业务方法（lambda 签名: unique_ptr<Response>(Request&)）
    server.registerMethod<CLRPC::EchoRequest, CLRPC::EchoResponse>(
        [](CLRPC::EchoRequest& req) -> std::unique_ptr<CLRPC::EchoResponse> {
            auto res = std::make_unique<CLRPC::EchoResponse>();
            res->set_reply("Server Echo: " + req.msg());
            res->set_code(0);
            return res;
        });

    server.registerMethod<CLRPC::AddRequest, CLRPC::AddResponse>(
        [](CLRPC::AddRequest& req) -> std::unique_ptr<CLRPC::AddResponse> {
            auto res = std::make_unique<CLRPC::AddResponse>();
            res->set_result(req.a() + req.b());
            return res;
        });

    // 6. 启动
    server.start();
    loop.loop();
    return 0;
}
```


### 3. 实现 RPC 客户端（协程调用）

`Rpc_Async_Client_Test.cc` 给出了完整示例。核心流程如下：

```cpp
#include "Client.h"
#include "ClientConfig.h"
#include "Service/EtcdDiscovery.h"
#include "Service/FileConfigRegister.h"
#include "Message.pb.h"
#include "Filter/TraceFilter.h"
#include "Filter/MetricsFilter.h"

// 协程调用示例：co_await 返回响应对象，异常统一抛出
Task<void> runEcho(std::shared_ptr<RPCClient> client) {
    CLRPC::EchoRequest req;
    req.set_msg("Hello RPC!");

    try {
        auto res = co_await client->CallAsync<CLRPC::EchoRequest, CLRPC::EchoResponse>(
            "EchoService", req, std::chrono::seconds(3),
            static_cast<uint32_t>(MethodID::Echo));
        std::cout << "Reply: " << res.reply() << " code=" << res.code() << std::endl;
    } catch (const RpcTimeoutException& e) {
        std::cerr << "Timeout: " << e.what() << std::endl;
    } catch (const RpcConnectionException& e) {
        std::cerr << "Connection error: " << e.what() << std::endl;
    } catch (const RpcRemoteException& e) {
        std::cerr << "Remote error: code=" << e.code() << std::endl;
    }
}

int main() {
    // 0. 从 YAML 加载客户端配置（config/client.yaml）
    auto cfg = loadClientConfig(PROJECT_ROOT "/config/client.yaml");

    cmlib::EventLoop loop;

    // 1. 创建服务发现（etcd / 文件配置二选一）
    std::shared_ptr<isServiceDiscovery> discovery;
    if (cfg.registryType == "file") {
        discovery = std::make_shared<FileConfigRegister>(
            &loop, cfg.configFileDir, cfg.pollInterval, RegistryMode::Client);
    } else {
        discovery = std::make_shared<EtcdDiscovery>(&loop, cfg.etcdUrl);
    }

    // 2. 创建 RPCClient（动态服务发现版）
    auto client = std::make_shared<RPCClient>(&loop, cfg, discovery);

    // 3. 添加过滤器链
    client->addFilter(std::make_shared<TraceFilter>());
    client->addFilter(std::make_shared<MetricsFilter>());

    // 4. 订阅服务（自动建立连接池）
    for (const auto& svc : cfg.subscribeServices)
        client->subscribe(svc);

    // 5. 协程调用（Task::get() 阻塞等待完成）
    runEcho(client).get();
    return 0;
}
```

### 4. 运行

```bash
# 终端1：启动服务端（读 config/server.yaml，默认监听 127.0.0.1:12345）
./build/Rpc_Server_WithLogic

# 终端2：启动客户端（读 config/client.yaml，订阅 EchoService / AddService）
./build/Rpc_Async_Client_Test
```


## 架构设计

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         客户端 (Client)                              │
│  ┌──────────┐      ┌──────────────┐     ┌─────────────────────────┐ │
│  │RPCClient │      │ConnectionPool│     │ PooledConnection[n]     │ │
│  │(C++20    │      │ 负载均衡策略  │     │ · 请求上下文 / 超时      │ │
│  │ 协程)    │ ───▶ │ · RoundRobin │ ──▶│ · ACK 超时重传          │ │
│  │          │      │ · LeastConn  │     │ · RpcFilterChain        │ │
│  │          │      │ · Random     │     └─────────────────────────┘ │
│  └──────────┘      └──────────────┘                                 │
│        │                                                            │
│        └── RpcFilterChain ── Trace / Metrics / RateLimit / Breaker  │
│        │                                                            │
│  ┌──────────────┐          ┌──────────────────┐                     │
│  │FileConfig    │          │EtcdDiscovery     │                     │
│  │Register      │          │(etcd Watch)      │                     │
│  └──────────────┘          └──────────────────┘                     │
└─────────────────────────────────────────────────────────────────────┘
                           │ TCP / 二进制协议（Header + Meta + Body）
                           ▼
┌───────────────────────────────────────────────────────────────────┐
│                       服务端 (rpc_core)                            │
│  ┌──────────┐      ┌──────────────┐      ┌─────────────────────┐  │
│  │RPCServer │      │TaskThreadPool│      │ Handler 业务逻辑     │  │
│  │ onMessage│───▶ │ (业务线程池)  │ ──▶ │ 异常→InternalError   │  │
│  │ 过滤器链  │      │              │      │ 响应经 runInLoop 发送│  │
│  └──────────┘      └──────────────┘      └─────────────────────┘  │
│        │                                                          │
│  ┌──────────────┐          ┌──────────────────┐                   │
│  │FileConfig    │          │EtcdRegister      │                   │
│  │Register      │          │(Lease+KeepAlive) │                   │
│  └──────────────┘          └──────────────────┘                   │
└───────────────────────────────────────────────────────────────────┘
```

### 通信协议

每个 RPC 请求/响应使用以下二进制协议格式（网络字节序，即大端）：

```
┌──────────────────────────────────────────────────────────────┐
│                        Header (8 Bytes)                      │
├────────┬───────┬─────────┬───────────────────────────────────┤
│ Magic  │ Flags │ Version │         Total Length              │
│ (2 B)  │ (1 B) │ (1 B)   │            (4 B)                  │
│0xC1EA  │0/1/2/3│   1     │  header + meta + body 总长度       │
├────────┴───────┴─────────┴───────────────────────────────────┤
│                      RPC_Meta (32 Bytes)                     │
├────────┬──────────┬─────────┬──────────┬────────────┬────────┤
│  seq   │ method_id│ timeout │ err_code │ retransmit │ traceID│
│ (8 B)  │  (4 B)   │ (4 B)   │  (4 B)   │   (1 B)    │ (8 B)  │
├────────┴──────────┴─────────┴──────────┴────────────┴────────┤
│                     reserver[3] (3 B)                        │
├──────────────────────────────────────────────────────────────┤
│                     Body (Protobuf 序列化)                    │
└──────────────────────────────────────────────────────────────┘
```

| 字段              | 说明                                             |
| ----------------- | ------------------------------------------------|
| **Magic**         | 魔数 `0xC1EA`，用于校验协议标识                   |
| **Flags**         | `0`=请求，`1`=响应，`2`=心跳，`3`=ACK             |
| **Version**       | 协议版本号（当前 `1`）                            |
| **Total Length**  | 整个消息长度（Header + Meta + Body，最大 10MB）   |
| **seq**           | 请求序列号，用于匹配请求与响应/ACK                 |
| **method_id**     | 方法 ID（`0`=Echo，`1`=Add，或自定义）            |
| **timeout**       | 超时时间（毫秒）                                  |
| **err_code**      | 错误码（0 表示成功，详见 [协议错误码](#协议错误码)）|
| **retransmit**    | 超时重传标记：`1`=要求对端回 ACK（应用层超时重传）  |
| **traceID**       | 分布式追踪 ID（TraceFilter 生成/透传）            |
| **reserver[3]**   | 保留字段                                         |
| **Body**          | Protobuf 序列化的消息体                          |

**decode 健壮性设计**（`ToolFunc.cc`）：
- **魔数校验**：魔数不匹配时在 Buffer 中查找下一个合法魔数并丢弃前置脏数据
- **半包/粘包**：`TotalLength` 大于当前可读字节时等待下次读取；超长/超短报文按 1 字节步进跳过
- **长度上限**：单条消息最大 10MB，防止恶意大包
- **字节序**：encode 时 Header/Meta 统一转为网络字节序，decode 时转回主机序

### 服务端核心流程

```
TcpServer onMessage() 收到请求
    │
    ▼
decode() 解码（魔数校验 / 半包粘包处理 / 魔数重同步）
    │
    ├─ Flags == ACK ──▶ conn->ackReceived(seq) ──▶ 结束
    │
    └─ Flags == 请求 (kRequest)
        │
        ├─ 过滤器链 executeBefore()（Trace→Metrics→限流→熔断）
        │    └─ 被拦截 ──▶ sendErrorResponse(UnknownError) + executeAfter()
        │
        ├─ retransmit == 1 ──▶ 立即发送 ACK（启用超时重传）
        │
        ├─ method_id 未注册 ──▶ sendErrorResponse(MethodNotFound)
        │
        └─ method_id 已注册
            │
            ▼
     taskThreadPool_->tryEnqueue(handler)
        │  └─ 队列已满(>100) ──▶ sendErrorResponse(TaskPoolFull)
        ▼  （线程池中执行业务逻辑）
     handler(body) — 用户注册的方法
        │  ├─ 业务异常 ──▶ runInLoop 发送 InternalError 错误响应
        │  └─ 正常返回（或返回空 → ParseError）
        ▼  （切换回 IO 线程）
     ioloop->runInLoop([=] {
         encode() 编码响应
         conn->send() 发送响应
         chain_.executeAfter() 逆序执行后置过滤器
     })
```

- **不阻塞 IO 线程**：所有业务逻辑在 `TaskThreadPool` 中异步执行（任务上限 100）
- **线程切换**：结果通过 `conn->getLoop()->runInLoop()` 切回原 IO 线程发送
- **异常兜底**：业务 lambda 抛出异常时，自动编码 `InternalError` 错误响应发回客户端
- **多服务**：`addService()` / 构造参数注册多个服务名，`start()` 时逐一向注册中心注册（`getLocalIp()` + 端口 + 权重）

### 客户端核心流程

```
CallAsync<Req, Res>(service, req, timeout, method_id)
    │
    ▼
connPool_->acquire(service)  — 负载均衡策略选择健康连接，返回 Borrowed RAII 句柄
    │                          （Borrowed 析构时自动将连接状态从 BUSY 恢复为 IDLE）
    ▼
conn->sendRequest(req, awaiter, timeout, method_id, chain_)
    │
    ├─ 1. 分配 seq（nextSeq++，线程安全）
    ├─ 2. 构造 RPC_Meta（retransmit=1 启用应用层超时重传）
    ├─ 3. chain_.executeBefore() — 前置过滤器（被拦截则快速失败并 executeAfter）
    ├─ 4. encode() 编码
    ├─ 5. 在 pending_ 表注册 PendingContext：
    │       ├─ onResponse: 收到响应 → setResponse + resume（锁外回调）
    │       ├─ cancel:     超时/取消 → setError + resume
    │       └─ timerId:    超时定时器（loop_->runAfter）
    ├─ 6. conn->sendWithRetransmit() 发送（服务端立即回 ACK，超时自动重传）
    │
    ▼
co_await *awaiter  — 挂起当前协程
    │
    ▼  (响应到达 / 超时 / 连接断开)
awaiter->resume()  — 恢复协程（自动切换到 IO 线程执行）
    │
    ▼
awaiter->await_resume()
    │
    ├─ error_ && errCode < 0 ──▶ 抛 RpcTimeoutException
    ├─ error_ && errCode > 0 ──▶ 抛 RpcRemoteException(errCode)
    ├─ 响应体为空 ──▶ 抛 RpcConnectionException
    ├─ 反序列化失败 ──▶ 抛 RpcConnectionException
    └─ 成功 ──▶ 返回 Response 对象（随后 chain_.executeAfter 逆序执行后置过滤器）
```

### 协程调用机制

本项目使用 **C++20 标准协程** 实现异步 RPC 调用，不依赖第三方协程库。

**核心组件：**

| 组件                        | 说明                                       |
| --------------------------- | ------------------------------------------|
| `Task<T>`                   | 通用协程任务类型，支持 `co_return`、`get()` 阻塞等待、嵌套 `co_await` |
| `Task<void>`                | void 版本偏特化                            |
| `RpcAwaiter<Response>`      | RPC 调用 Awaiter，封装挂起/恢复/结果/异常    |
| `PooledConnection`          | 管理 `PendingContext` 请求上下文与超时定时器 |

**协程生命周期：**

```
CallAsync()
    │
    ▼
co_await awaiter        ←── 协程挂起（await_suspend 保存 handle_）
    │                           ↑
    ▼                           │
  [等待响应或超时]               │
    │                           │
    ├─ onResponse() ────────────┤ 恢复：
    │     awaiter->resume()     │ handle_.resume()
    │                           │
    ├─ onTimeout() ─────────────┤ 恢复 + setError：
    │     cancel() → setError() │ handle_.resume()
    │     awaiter->resume()    ─┘
    │
    ▼
awaiter->await_resume() — 抛异常 或 返回 Response
```

**线程安全设计：**
- `awaiter->resume()` 检测当前线程是否为 IO 线程
- 如果不是 IO 线程，通过 `loop_->runInLoop()` 调度到 IO 线程恢复协程
- 避免在非 IO 线程直接操作协程句柄，杜绝数据竞争与悬空句柄
- `Task::get()` 通过条件变量阻塞等待协程完成（修复了早期版本的自旋忙等）

## 过滤器链

客户端与服务端均内置可插拔的过滤器链（`rpc_core/include/Filter/`），用于在编解码与业务逻辑之外实现横切关注点。

### 接口设计

```cpp
// RpcContext：跨 before/after 传递数据的上下文（key-value）
using RpcContext = std::map<std::string, std::string>;

class RpcFilter {
public:
    // 前置处理：返回 false 表示拦截该请求（不进入后续处理）
    virtual bool before(const Header& header, RPC_Meta& meta,
                        const std::string& body, RpcContext& ctx) = 0;
    // 后置处理：请求处理完成后逆序调用
    virtual void after(const Header& header, const RPC_Meta& meta,
                       const RpcContext& ctx) = 0;
};
```

**RpcFilterChain 执行语义：**
- `executeBefore()`：按添加顺序执行，任一返回 `false` 立即短路拦截
- `executeAfter()`：按添加顺序的**逆序**执行（后进先出，类似洋葱模型）
- 服务端被拦截：`sendErrorResponse(conn, meta, UnknownError)` + `executeAfter()`
- 客户端被拦截：`meta.err_code = ClientBlock` 传给 `executeAfter()`，随后 `awaiter->setError(UnknownError)` + `resume()` 快速失败

### 内置过滤器

| 过滤器                    | 类/文件                         | 功能说明                                                                 |
| ------------------------- | ------------------------------- | ------------------------------------------------------------------------ |
| **TraceFilter**           | `Filter/TraceFilter.h`          | 分布式 ID 追踪。对端未设置 `traceID` 时本地生成（steady_clock 计数），写入 `ctx["trace_id"]`，完成后输出完成日志 |
| **MetricsFilter**         | `Filter/MetricsFilter.h`        | 指标埋点。记录请求总数、成功/失败数（按错误码分类）、延迟直方图，数据落入 `MetricsCollector` 单例 |
| **RateLimitFilter**       | `Filter/RateLimitFilter.h`      | 流量控制。基于 `TokenBucket(rate, capacity)` 令牌桶，令牌耗尽则拦截并标记 `block_reason=rate_limited` |
| **CircuitBreakerFilter**  | `Filter/CircuitBreakerFilter.h` | 熔断保护。窗口期内总请求 > 10 且错误率超过阈值时开启熔断，直接拒绝请求（`block_reason=circuitBreaker_open`），窗口结束后自动复位 |

**MetricsCollector（单例）**：线程安全的指标收集器，支持：
- 计数器（`inCounter(name)`）：请求总数、成功数、错误数等
- 延迟直方图（`observeLatency(name, seconds)`）：9 个桶（0.001s ~ 5.0s+）
- `getCounters()` 导出快照

### 使用方式

```cpp
// 服务端：在 server.start() 之前添加
server.addFilter(std::make_shared<TraceFilter>());
server.addFilter(std::make_shared<MetricsFilter>());
server.addFilter(std::make_shared<RateLimitFilter>(bucket));
server.addFilter(std::make_shared<CircuitBreakerFilter>(errorRate, window));

// 客户端：在调用前添加
client->addFilter(std::make_shared<TraceFilter>());
client->addFilter(std::make_shared<MetricsFilter>());
```

> 添加顺序即 `executeBefore` 的执行顺序；`executeAfter` 逆序执行。`Rpc_Server_test.cc` / `Rpc_Async_Client_Test.cc` 中均演示了完整的过滤器装配。



## 服务注册与发现

### 接口设计

服务注册与发现通过抽象接口解耦，支持多种后端实现：

```
┌────────────────────────────────────────────────────────────────┐
│                   抽象接口层                                    │
│  ┌──────────────────────┐  ┌──────────────────────────┐        │
│  │ isServiceRegister    │  │ isServiceDiscovery       │        │
│  │──────────────────────│  │──────────────────────────│        │
│  │ + registerService()  │  │ + subscribe(service, cb) │        │
│  │ + deregisterService()│  │ + unsubscribe(service)   │        │
│  │ + shutdown()         │  │ + shutdown()             │        │
│  └──────────────────────┘  └──────────────────────────┘        │
└────────────────────────────────────────────────────────────────┘
                          ▲              ▲
                          │              │
           ┌──────────────┴──────────────┴──────────────┐
           │                                            │
┌─────────────────────────┐            ┌─────────────────────────┐
│ FileConfigRegister      │            │ EtcdRegister            │
│ (YAML 文件, 轮询)        │            │ (Lease + KeepAlive)     │
│ 实现 注册 + 发现         │            │  实现 isServiceRegister  │
└─────────────────────────┘            └────────────┬────────────┘
                                                    │
                              ┌─────────────────────┴──────────┐
                              │                                │
                     ┌──────────────────┐          ┌──────────────────┐
                     │   EtcdRegister   │          │ EtcdDiscovery    │
                     │   (服务端注册)    │          │ (客户端 Watch)   │
                     └──────────────────┘          └──────────────────┘
```

| 特性                    | FileConfigRegister      | EtcdRegister + EtcdDiscovery|
| -----------------------| ------------------------ | ----------------------------|
| **后端存储**            | 本地 YAML 文件           | 外部 etcd 服务（通过 etcd-cpp-api 客户端连接） |
| **数据变更通知**        | 定时轮询（可配间隔）      | Watch 机制（实时推送）        |
| **服务存活保证**        | 文件存在即存活            | Lease 续约 + TTL 自动过期    |
| **进程异常退出处理**    | 需手动清理文件            | Lease 超时后 key 自动清理     |
| **适用场景**            | 单机测试 / 小规模部署     | 生产环境 / 微服务架构         |
| **模式支持**            | Server / Client / Both    | 注册端与发现端分离          |

### 文件配置注册中心 (FileConfigRegister)

**工作模式：**

| 模式                   | 行为                                     |
| -----------------------| ----------------------------------------|
| `RegistryMode::Server` | 仅服务端：将端点信息写入 YAML 文件         |
| `RegistryMode::Client` | 仅客户端：轮询扫描配置目录，获取端点列表    |
| `RegistryMode::Both`   | 同时具备注册与发现能力                     |

**YAML 文件格式：**

每个端点存储为一个独立文件，命名规则：`{serviceName}_{host}_{port}.yaml`（支持单对象或多端点数组两种结构）：

```yaml
# EchoService_127_0_0_1_12345.yaml
service: EchoService
host: "127.0.0.1"
port: 12345
weight: 1
# metadata:          # 可选
#   region: cn-east
```

**客户端轮询机制：**
- 定期扫描配置目录下所有 `.yaml` 文件（间隔可配，默认 5s）
- 增量 diff 后通过回调通知 `ConnectionPool.updateServiceEndpoints()` 更新连接池
- `subscribe`/`unsubscribe` 采用延迟清理（`pendingUnsubscribe_`），避免与轮询竞争

### Etcd 注册中心 (EtcdRegister / EtcdDiscovery)

**EtcdRegister — 服务端注册（注意需要传入 TaskThreadPool）：**

```cpp
// 业务线程池同时服务于 RPC 业务逻辑与 etcd 注册（避免阻塞 IO 线程）
auto taskPool = std::make_shared<TaskThreadPool>(8);
taskPool->start();

auto registry = std::make_shared<EtcdRegister>(&loop, taskPool,
                                               "http://127.0.0.1:2379",
                                               10 /*ttl*/, 3 /*续约间隔*/);
```

```
服务器启动 start()
    │
    ▼
对每个注册的服务，投递到 taskPool 执行：
    ├─ 创建 etcd Lease（TTL 可配，默认 10s）
    ├─ 将端点信息以 JSON 写入 /clearmoon/services/{svc}/{host}:{port}
    └─ KeepAlive 自动续约（默认每 3 秒）
    │
    ▼
进程退出 → stop() → deregisterService → rm(key) + 取消 KeepAlive
进程异常退出 → Lease 超时后 key 自动清理
```

**EtcdDiscovery — 客户端发现：**

```
subscribe(serviceName, callback)
    │
    ▼
fetchAndNotify: ls(/clearmoon/services/{svc}/) — 获取当前所有端点
    │
    ├─ 与 lastEndpoints_ 比对，发生变化才回调
    │
    ▼
startWatch: watch(prefix, recursive=true)
    │   （Watcher 运行在内部线程）
    ▼  (端点变更事件)
再次 fetchAndNotify() 拉取全量列表 → 回调通知 ConnectionPool
```

### ServiceDiscoverer 封装

`ServiceDiscoverer` 是对 `isServiceDiscovery` 接口的上层封装，用于简化单个服务的订阅管理（注意：其订阅/取消订阅操作需保证在 IO 线程执行）：

```cpp
auto discoverer = std::make_shared<ServiceDiscoverer>(loop, serviceName, etcdDiscovery);
discoverer->setEndpointChangeCallback([](const std::vector<Endpoint>& eps) {
    for (auto& ep : eps)
        std::cout << "Endpoint: " << ep.address() << std::endl;
});
discoverer->start();
```

## 连接池与负载均衡

### 连接池架构

`ConnectionPool` 支持两种模式：

```
ConnectionPool
    │
    ├─ 静态池（单地址多连接）
    │   └─ connections_: vector<PooledConnection>
    │      ├─ conn[0]  ├─ conn[1]  ├─ ...  ├─ conn[n-1]
    │      └─ 所有连接连接到同一服务器地址
    │
    └─ 动态池（多服务多地址）
        └─ servers_: map<ServerName, ServiceEntry>
            └─ ServiceEntry
                └─ groups: vector<ServerConnGroup>     # 每个端点一组
                    └─ ServerConnGroup
                        ├─ endpoint: Endpoint（具体服务地址）
                        └─ connections: vector<PooledConnection>
                            ├─ conn[0]                 # connPerServer 个
                            ├─ conn[1]
                            └─ ...
```

| 池类型       | 构造函数                                    | 用途                     |
| ------------ | ------------------------------------------- | ------------------------ |
| **静态池**   | `ConnectionPool(loop, addr, poolSize, strategy, healthCheckInterval)` | 直连单个服务器（默认 4 连接 + RoundRobin + 5s 健康检查）|
| **动态池**   | `ConnectionPool(loop, strategy, connPerServer, healthCheckInterval)` | 搭配服务发现自动管理多服务 |

**Borrowed RAII 句柄**：
- `acquire()` 返回 `Borrowed` 包装的 `shared_ptr<PooledConnection>`，借用期间连接状态置为 `BUSY`
- `Borrowed` 析构时自动调用 `returnToIdleIfBusy()` 将连接归还为 `IDLE`，无需手动释放
- 禁止拷贝、允许移动，可像原始指针一样使用（`operator->` / `operator*`）

**动态端点更新**：
- `updateServiceEndpoints(serviceName, eps)`：增量更新单个服务端点，不影响其他服务
- 移除的端点立即断开其全部连接（正在处理中的请求将失败）
- `removeService()` / `ignoreService()`：移除或忽略指定服务
- 优雅关闭：待关闭连接进入 `pendingClose_` 队列，等 `closeCv_` 全部断开后再析构

### 负载均衡策略

| 策略                       | 说明                                                       |
| -------------------------- | ---------------------------------------------------------- |
| **RoundRobin** (轮询)       | 原子计数依次取模，均匀分配请求（端点级 + 连接级双层轮询）   |
| **LeastConnection** (最少连接) | 选择 `activeRequest()` 最小的健康连接，动态负载感知       |
| **Random** (随机)           | 随机选择，最多尝试 10 次                                    |

所有策略均支持静态池与动态池两种模式；若无健康可用连接，抛出 `RpcConnectionException`。

### 健康检查

- **定时执行**：`loop_->runEvery(healthCheckInterval_)`，默认每 5 秒调用 `doHealthCheck()`
- **检测标准**：连接状态为 `IDLE` 或 `UNHEALTHY` 且 `!isConnected()`
- **自动恢复**：对不健康连接调用 `markUnHealthy()` 并调度 `conn->Connect()` 重连
- **覆盖范围**：静态池与动态池中所有连接统一检查
- **连接状态机**：

```
IDLE ──acquire()──▶ BUSY ──Borrowed 析构──▶ IDLE
  │                                              │
  │ 连接断开（onClose）                           │ 健康检查发现 !isConnected()
  ▼                                              ▼
UNHEALTHY ──Connect()──▶ IDLE (重连成功)         (重连失败保持 UNHEALTHY)
```



## RPC Client 详解

### 构造函数总览

| 构造函数                          | 模式     | 说明                                        |
| --------------------------------- | -------- | ------------------------------------------- |
| `RPCClient(loop, serverAddr)`     | 静态     | 直连单地址，默认 4 连接 + RoundRobin          |
| `RPCClient(loop, serverAddr, cfg)`| 静态     | 直连单地址 + 从 `ClientConfig` 读取连接池参数 |
| `RPCClient(loop, connPerServer, discovery)` | 动态 | 服务发现，指定每端点连接数 + RoundRobin      |
| `RPCClient(loop, cfg, discovery)` | 动态     | 服务发现 + 从 `ClientConfig` 读取全部参数     |

公共接口：
- `subscribe(serviceName)` / `unsubscribe(serviceName)`：订阅/取消服务（自动维护连接池）
- `addFilter(shared_ptr<RpcFilter>)`：向过滤器链追加过滤器
- `CallAsync<Req, Res>(req, timeout, method_id)`：静态模式协程调用
- `CallAsync<Req, Res>(serviceName, req, timeout, method_id)`：动态模式协程调用
- 析构时自动取消订阅、关闭健康检查并优雅断开连接池

### 静态模式

直连已知地址的服务器，无需服务发现：

```cpp
cmlib::EventLoop loop;
cmlib::InetAddress addr("127.0.0.1", 12345, false);

auto client = std::make_shared<RPCClient>(&loop, addr);

// 调用时自动从连接池获取连接
auto res = co_await client->CallAsync<Req, Res>(req, timeout, method_id);
```

### 动态服务模式

通过服务发现获取可用服务器端点，自动管理多服务连接：

```cpp
cmlib::EventLoop loop;

auto discovery = std::make_shared<EtcdDiscovery>(&loop, "http://127.0.0.1:2379");
auto client = std::make_shared<RPCClient>(&loop, 2, discovery);

// 订阅服务（自动维护连接池，每个端点建立 2 个连接）
client->subscribe("EchoService");
client->subscribe("AddService");

// 指定服务名调用
auto res = co_await client->CallAsync<Req, Res>("EchoService", req, timeout, method_id);
```

### 协程调用示例

```cpp
Task<void> runEchoTest(std::shared_ptr<RPCClient> client) {
    CLRPC::EchoRequest req;
    req.set_msg("Hello, Async RPC!");

    try {
        auto res = co_await client->CallAsync<CLRPC::EchoRequest, CLRPC::EchoResponse>(
            "EchoService", req, std::chrono::seconds(5),
            static_cast<uint32_t>(MethodID::Echo));

        LOG_INFO << "Echo reply: " << res.reply()
                 << " (code=" << res.code() << ")";
    } catch (const std::exception& e) {
        LOG_ERROR << "RPC call failed: " << e.what();
    }
}

// 启动异步调用（Task 支持嵌套 co_await，或用 get() 阻塞等待）
auto task = runEchoTest(client);
```

**应用层超时重传（ACK 机制）：**
- `PooledConnection::sendRequest()` 默认设置 `meta.retransmit = 1`
- 通过 `conn->sendWithRetransmit(&buff, seq)` 发送，ClearMoon 传输层维护重传队列
- 服务端收到携带 `retransmit=1` 的请求后**立即回 ACK**；收到响应侧同理
- 请求成功送达后由 ACK 确认，避免因丢失重传造成的重复处理


## 错误处理与异常

### 异常类型

| 异常类型                     | 触发时机                       | 说明                        |
| --------------------------- | ------------------------------ | ---------------------------|
| `RpcTimeoutException`       | 请求超时（超时期间未收到响应）  | 包含 seq 和 timeoutMs 信息    |
| `RpcConnectionException`    | 连接不可用 / 连接断开 / 响应解析失败 | 包含具体错误描述          |
| `RpcRemoteException`        | 服务端返回非零错误码（err_code > 0）| 包含远端错误码 `code()`   |

`RpcAwaiter::await_resume()` 的判定逻辑：

```
error_ == true 且 errCode_ < 0  ──▶ 抛 RpcTimeoutException
error_ == true 且 errCode_ > 0  ──▶ 抛 RpcRemoteException(errCode_)
response_ 为空                   ──▶ 抛 RpcConnectionException
ParseFromString 失败             ──▶ 抛 RpcConnectionException
其余                             ──▶ 返回 Response
```

**超时处理双重保障：**

```
1. PooledConnection::onTimeout() 定时器回调（loop_->runAfter 注册）
   └─ 从 pending_ 表取出上下文 → 调用 cancel()
       └─ awaiter->setError(-1) + awaiter->resume()
       └─ 连接若存活则回送 ACK（撤销重传队列中的该 seq）

2. 协程在 await_resume() 中检测 error_
   └─ 抛出 RpcTimeoutException / RpcRemoteException
```

**连接断开处理：**
- `PooledConnection::onClose()` / 析构调用 `cancelAllPending()` 取消所有待处理请求
- 待处理请求的 Awaiter 通过 `setError()` + `resume()` 唤醒协程
- 协程在 `await_resume()` 中检测到空响应，抛出 `RpcConnectionException`
- 对重复/未知 seq 的迟到响应，`pending_` 表中已无上下文，直接丢弃并记录 WARNING

### 协议错误码

`ToolFunc.h` 中 `RpcErrorCode` 枚举：

| 错误码 | 名称                 | 说明                              |
| ------ | ------------------- | --------------------------------- |
| 0      | `NoError`           | 成功                              |
| 1      | `SerializeFailed`   | 消息序列化失败                    |
| 2      | `MagicError`        | 魔数错误（decode 时发现）         |
| 3      | `LengthError`       | 长度非法                          |
| 4      | `IncompletePacket`  | 半包（不直接通知对端）            |
| 20     | `TaskPoolFull`      | 服务端任务线程池已满              |
| 21     | `MethodNotFound`    | 未找到请求方法                    |
| 22     | `ParseError`        | 请求体反序列化失败                |
| 23     | `InternalError`     | 业务处理内部异常                  |
| 50     | `ClientBlock`       | 客户端过滤器拦截                  |
| 99     | `UnknownError`      | 未知错误                          |

## 配置与服务注册

项目使用 YAML 配置驱动运行参数（`ServerConfig` / `ClientConfig`），配置文件位于 `config/` 目录。

**config/server.yaml：**

```yaml
server:
  host: 127.0.0.1          # 监听地址
  port: 12345              # 监听端口
  thread_pool_size: 2      # 业务线程池大小
  service_weight: 1        # 注册端点权重
  registry:
    type: etcd             # etcd / file
    etcd_url: http://127.0.0.1:2379
    ttl: 10                # Lease TTL（秒）
    keepalive_interval: 3  # 续约间隔（秒）
    file_path: ./config    # file 模式配置目录
    poll_interval: 5.0     # file 模式轮询间隔（秒）
  services:                # 注册的服务名列表
    - EchoService
    - AddService

filters:
  trace: true
  metrics: true
  rate_limit:
    rate: 100              # 令牌/秒
    capacity: 200          # 桶容量
  circuit_breaker:
    error_rate: 60         # 错误率阈值（%）
    window_seconds: 10     # 熔断窗口（秒）
```

**config/client.yaml：**

```yaml
client:
  registry:
    type: etcd             # etcd / file
    etcd_url: http://127.0.0.1:2379
    file_path: ./config    # file 模式配置目录
    poll_interval: 1.0     # file 模式轮询间隔（秒）
  conn_per_server: 4       # 每个端点的连接数
  load_balance: round_robin # round_robin / least_connection / random
  health_check_interval: 5 # 健康检查间隔（秒）
  call_timeout_ms: 5000    # 默认 RPC 调用超时（毫秒）
  subscribe:               # 订阅的服务
    - EchoService
    - AddService

filters:
  trace: true
  metrics: true
  rate_limit:
    rate: 10               # 令牌/秒
    capacity: 200
  circuit_breaker:
    error_rate: 50         # 错误率阈值（%）
    window_seconds: 10
```

**加载方式：**

```cpp
auto cfg = loadServerConfig(PROJECT_ROOT "/config/server.yaml"); // 顶层定义 PROJECT_ROOT
auto cfg = loadClientConfig(PROJECT_ROOT "/config/client.yaml");
```

> 亦可直接硬编码创建注册中心：服务端 `FileConfigRegister(&loop, "./config", 3.0, RegistryMode::Server)` 或 `EtcdRegister(&loop, taskPool, "http://127.0.0.1:2379", 10, 3)`；客户端 `FileConfigRegister(&loop, "./config", 3.0, RegistryMode::Client)` 或 `EtcdDiscovery(&loop, "http://127.0.0.1:2379")`。

## 测试示例

项目包含以下测试入口：

| 文件                        | 说明                                | 构建目标             |
| --------------------------- | -----------------------------------| --------------------|
| `Rpc_Server_test.cc`        | 服务端带业务逻辑测试（Echo/Add + 过滤器链 + YAML 配置） | `Rpc_Server_WithLogic` |
| `Rpc_Async_Client_Test.cc`  | 客户端协程异步调用测试（动态服务发现 + 过滤器链） | `Rpc_Async_Client_Test` |
| `Client/Test/Rpc_Client.cc` | 早期客户端基础测试（旧 API，未编译）   | —                   |
| `rpc_core/Test/Rpc_Server.cc` | 早期服务端测试（未编译）            | —                   |
| `server_timeout_example.cc` | 早期超时处理示例（未编译）            | —                   |

**运行测试：**

```bash
# 0.（可选）若使用 etcd 服务发现，先启动 etcd
# ./etcd --data-dir=./default.etcd &

# 1. 终端1：启动服务端（读取 config/server.yaml）
./build/Rpc_Server_WithLogic

# 2. 终端2：启动异步客户端（读取 config/client.yaml，测试 Echo / Add）
./build/Rpc_Async_Client_Test
```

预期输出（客户端）：

```
========== Running Echo Test ==========
[Echo Test] reply: Server Echo: Hello from async client!, code: 0
[Echo Test] PASSED

========== Running Add Test ===========
[Add Test] result: 300, error:
[Add Test] PASSED

========== All Tests Completed ==========
Passed: 2/2
```

## 许可证

该项目基于 [MIT License](LICENSE) 开源。

