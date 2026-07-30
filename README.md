# CMRPC — 基于 C++20 协程的高性能 RPC 框架

[![C++20](https://img.shields.io/badge/C++-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![Protocol Buffers](https://img.shields.io/badge/Protobuf-3.0+-brightgreen.svg)](https://developers.google.com/protocol-buffers)

**CMRPC** 是一个基于 [ClearMoon 网络库](/lib/ClearMoon) 构建的现代 C++ RPC 框架，支持 **C++20 协程异步调用**、**连接池管理**、**多种服务发现后端**（etcd / 文件配置）、**多种负载均衡策略**，同时提供完备的异常处理与连接健康检查机制。

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
- [服务注册与发现](#服务注册与发现)
  - [接口设计](#接口设计)
  - [文件配置注册中心 (FileConfigRegister)](#文件配置注册中心-fileconfigregister)
  - [Etcd 注册中心 (EtcdRegister / EtcdDiscovery)](#etcd-注册中心-etcdregister--etcddiscovery)
  - [ServiceDiscoverer 封装](#servicediscoverer-封装)
- [连接池与负载均衡](#连接池与负载均衡)
  - [连接池架构](#连接池架构)
  - [负载均衡策略](#负载均衡策略)
  - [健康检查](#健康检查)
- [RPC Client 详解](#rpc-client-详解)
  - [静态模式](#静态模式)
  - [动态服务模式](#动态服务模式)
  - [协程调用示例](#协程调用示例)
- [错误处理与异常](#错误处理与异常)
- [配置与服务注册](#配置与服务注册)
- [测试示例](#测试示例)
- [许可证](#许可证)

---

## 特性概览

| 特性                | 说明                                                                 |
| ------------------- | -------------------------------------------------------------------- |
| **C++20 协程异步**   | 基于 `Task<T>` 与 `RpcAwaiter` 的 `co_await` 异步 RPC 调用           |
| **连接池管理**       | 静态地址池 + 动态服务池，支持连接复用与自动回收                       |
| **负载均衡**         | 轮询（RoundRobin）、最少连接（LeastConnection）、随机（Random）       |
| **连接健康检查**     | 定时检测连接状态，自动重连失效连接                                    |
| **服务注册与发现**   | 抽象接口 `isServiceRegister` / `isServiceDiscovery`，多种后端实现     |
| **Etcd 集成**        | 基于 etcd Lease + KeepAlive 的服务注册与 Watch 机制的服务发现         |
| **文件配置中心**     | 基于 YAML 文件的注册中心，支持 Server/Client/Both 模式                 |
| **Protobuf 序列化**  | 使用 Protocol Buffers 作为消息序列化格式                              |
| **请求超时控制**     | 细粒度超时控制 + 超时自动清理                                         |
| **任务线程池**       | 服务端将业务逻辑异步分派到线程池执行，不阻塞 IO 线程                   |

## 项目结构

```
RPC/
├── CMakeLists.txt                  # 顶层 CMake 构建文件
├── Client/                         # RPC 客户端库
│   ├── Client.h / Client.cc        # RPCClient 主类（协程调用入口）
│   ├── ConnectionPool.h / .cc      # 连接池（静态/动态池管理）
│   ├── PooledConnection.h / .cc    # 池化连接（请求上下文管理）
│   ├── RpcAwaiter.h                # 协程 Awaiter 实现
│   ├── Rpc_exceptions.h            # RPC 异常定义
│   ├── Task.h                      # 通用协程 Task<T> 类型
│   └── CMakeLists.txt
├── rpc_core/                       # RPC 核心库
│   ├── include/
│   │   ├── Rpc_server.h            # RPCServer 服务端主类
│   │   ├── ToolFunc.h              # 协议编解码函数
│   │   ├── Message.proto / .pb.h   # Protobuf 消息定义
│   │   ├── TaskThreadPool.h        # 任务线程池
│   │   ├── RpcFilter.h / .h        # 过滤器链（预留）
│   │   └── Service/                # 服务注册与发现
│   │       ├── Endpoint.h          # 端点数据结构
│   │       ├── IsServiceRegister.h # 服务注册抽象接口
│   │       ├── IsServiceDiscovery.h# 服务发现抽象接口
│   │       ├── Registry.h          # 复合接口（注册+发现）
│   │       ├── FileConfigRegister.h# 文件配置注册中心
│   │       ├── EtcdRegister.h      # Etcd 服务注册
│   │       ├── EtcdDiscovery.h     # Etcd 服务发现
│   │       └── ServiceDiscoverer.h # 服务发现封装器
│   └── src/
│       ├── Rpc_server.cc
│       ├── ToolFunc.cc
│       ├── Message.pb.cc
│       ├── TaskThreadPool.cc
│       ├── FileConfigRegister.cc
│       ├── EtcdRegister.cc
│       ├── EtcdDiscovery.cc
│       └── ServiceDiscoverer.cc
├── lib/ClearMoon/                  # 底层网络库
│   ├── net/                        # 网络核心（EventLoop, TcpServer, TcpClient...）
│   ├── net/Poller/                 # Epoll IO 多路复用
│   ├── net/Http/                   # HTTP 支持
│   ├── net/Log/                    # 异步日志
│   └── base/                       # 基础组件（ThreadPool, BlockQueue...）
├── Rpc_Async_Client_Test.cc        # 异步客户端测试入口
├── Rpc_Server_test.cc              # 服务端测试入口
└── server_timeout_example.cc       # 超时处理示例
```

## 依赖项

| 依赖            | 版本要求     | 用途                               |
| ----------------| ----------- | ----------------------------------|
| **ClearMoon**   | C++17       | 底层网络库（项目内包含）            |
| **Protobuf**    | ≥ 3.0       | 消息序列化                         |
| **Abseil**      | LTS         | 日志、错误检查等                   |
| **yaml-cpp**    | ≥ 0.6       | YAML 配置文件解析（FileConfig）    |
| **cpprestsdk**  | ≥ 2.10      | HTTP 请求（Etcd 客户端底层依赖）   |
| **etcd-cpp-api**| ≥ 0.1       | etcd 客户端 C++ API               |
| **CMake**       | ≥ 3.10      | 构建系统                          |

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

# 4. 运行测试
# 先启动 etcd（如果使用动态服务发现）
# ./etcd --data-dir=./default.etcd &

./build/Rpc_Async_Client_Test     # 启动客户端测试
./build/Rpc_Server_WithLogic      # 启动服务端测试（另一终端）
```

## 快速开始

### 1. 定义 Protobuf 协议

```protobuf
syntax = "proto3";
package CLRPC;

message EchoRequest {
    string msg = 1;
}

message EchoResponse {
    string reply = 1;
    int32 code = 2;
}

message AddRequest {
    int64 a = 1;
    int64 b = 2;
}

message AddResponse {
    int64 result = 1;
    string Error = 2;
}
```

### 2. 实现 RPC 服务端

```cpp
#include "Rpc_server.h"
#include "Service/FileConfigRegister.h"
#include "Message.pb.h"

cmlib::EventLoop loop;

// 1. 创建文件配置注册中心（服务端模式）
auto registry = std::make_shared<FileConfigRegister>(
    &loop, "/path/to/config/dir", 3.0, RegistryMode::Server);

// 2. 创建 RPCServer，监听 9988 端口，注册服务名 "EchoService"
cmlib::InetAddress addr("0.0.0.0", 9988);
RPCServer server(&loop, addr, registry, "EchoService");

// 3. 注册业务方法
server.registerMethod<CLRPC::EchoRequest, CLRPC::EchoResponse>(
    [](CLRPC::EchoRequest& req) -> std::unique_ptr<CLRPC::EchoResponse> {
        auto res = std::make_unique<CLRPC::EchoResponse>();
        res->set_reply("Echo: " + req.msg());
        res->set_code(0);
        return res;
    }
);

server.registerMethod<CLRPC::AddRequest, CLRPC::AddResponse>(
    [](CLRPC::AddRequest& req) -> std::unique_ptr<CLRPC::AddResponse> {
        auto res = std::make_unique<CLRPC::AddResponse>();
        res->set_result(req.a() + req.b());
        return res;
    }
);

// 4. 启动服务
server.start();
loop.loop();
```

### 3. 实现 RPC 客户端（协程调用）

```cpp
#include "Client.h"
#include "Service/FileConfigRegister.h"
#include "Message.pb.h"

cmlib::EventLoop loop;

// 客户端模式的文件配置注册中心 + 服务发现
auto discovery = std::make_shared<FileConfigRegister>(
    &loop, "/path/to/config/dir", 3.0, RegistryMode::Client);

// 创建 RPCClient（每个服务建立 2 个连接）
auto client = std::make_shared<RPCClient>(&loop, 2, discovery);

// 订阅服务
client->subscribe("EchoService");

// 协程调用
auto echoTask = [&]() -> Task<void> {
    CLRPC::EchoRequest req;
    req.set_msg("Hello RPC!");

    try {
        auto res = co_await client->CallAsync<CLRPC::EchoRequest, CLRPC::EchoResponse>(
            "EchoService", req, std::chrono::seconds(3), 0);
        std::cout << "Reply: " << res.reply() << std::endl;
    } catch (const RpcTimeoutException& e) {
        std::cerr << "Timeout: " << e.what() << std::endl;
    } catch (const RpcConnectionException& e) {
        std::cerr << "Connection error: " << e.what() << std::endl;
    }
};
```

### 4. 运行

```bash
# 终端1：启动服务端
./build/Rpc_Server_WithLogic

# 终端2：启动客户端
./build/Rpc_Async_Client_Test
```

## 架构设计

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        客户端 (Client)                              │
│  ┌──────────┐      ┌──────────────┐     ┌──────────────────────┐    │
│  │RPCClient │      │ConnectionPool│     │ PooledConnection[n]  │    │
│  │(C++20    │      │ 负载均衡策略  │     │ · 连接管理            │    │
│  │ 协程)     │ ──▶ │ · RoundRobin │──▶ │ · 请求上下文管理      │    │
│  │          │      │ · LeastConn  │     │ · 超时控制            │    │
│  │          │      │ · Random     │     │ · 健康检查            │    │
│  └──────────┘      └──────────────┘     └──────────────────────┘    │
│                          │                                          │
│         ┌────────────────┴──────────────┐                           │
│         ▼                               ▼                           │
│  ┌──────────────┐           ┌──────────────────┐                    │
│  │FileConfig    │           │EtcdDiscovery     │                    │
│  │Register      │           │(etcd Watch)      │                    │
│  └──────────────┘           └──────────────────┘                    │
└─────────────────────────────────────────────────────────────────────┘
                           │ TCP / 协议
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                       服务端 (rpc_core)                          │
│  ┌──────────┐    ┌──────────────┐      ┌──────────────────────┐ │
│  │RPCServer │──▶│TaskThreadPool│     │ Handler 业务逻辑       │ │
│  │          │    │ (线程池)     │ ──▶ │ (用户注册的方法)       │ │
│  └──────────┘    └──────────────┘      └──────────────────────┘ │
│                        │                                        │
│         ┌──────────────┴──────────────┐                         │
│         ▼                             ▼                         │
│  ┌──────────────┐           ┌──────────────────┐                │
│  │FileConfig    │           │EtcdRegister      │                │
│  │Register      │           │(etcd Lease +     │                │
│  │(YAML写入)    │           │ KeepAlive)        │                │
│  └──────────────┘           └──────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

### 通信协议

每个 RPC 请求/响应使用以下二进制协议格式：

```
┌──────────────────────────────────────────────────────────────┐
│                        Header (8 Bytes)                      │
├────────┬───────┬─────────┬───────────────────────────────────┤
│ Magic  │ Flags │ Version │         Total Length              │
│ (2 B)  │ (1 B) │ (1 B)   │            (4 B)                  │
│0xC1EA  │0/1/3  │   1     │  header + meta + body 总长度       │
├────────┴───────┴─────────┴───────────────────────────────────┤
│                      RPC_Meta (32 Bytes)                     │
├────────┬──────────┬─────────┬──────────┬─────────────────────┤
│  seq   │ method_id│ timeout │ err_code │   reserved[12]      │
│ (8 B)  │  (4 B)   │ (4 B)   │  (4 B)   │                     │
├────────┴──────────┴─────────┴──────────┴─────────────────────┤
│                     Body (Protobuf 序列化)                    │
└──────────────────────────────────────────────────────────────┘
```

| 字段              | 说明                                        |
| ----------------- | --------------------------------------------|
| **Magic**         | 魔数 `0xC1EA`，用于校验协议标识              |
| **Flags**         | `0`=请求，`1`=响应，`2`=心跳，`3`=ACK        |
| **Version**       | 协议版本号                                  |
| **Total Length**  | 整个消息长度（Header + Meta + Body）         |
| **seq**           | 请求序列号，用于匹配请求与响应                |
| **method_id**     | 方法 ID（`0`=Echo, `1`=Add, 或自定义）       |
| **timeout**       | 超时时间（毫秒）                             |
| **err_code**      | 错误码（0 表示成功）                         |
| **Body**          | Protobuf 序列化的消息体                      |

### 服务端核心流程

```
TcpServer onMessage() 收到请求
    │
    ▼
decode() 解码 Header + RPC_Meta + Body
    │
    ├─ Flags == 3 (ACK) ──▶ conn->ackReceived(seq) ──▶ 结束
    │
    └─ Flags == 0 (请求)
        │
        ├─ method_id 未注册 ──▶ LOG_ERROR ──▶ 跳过
        │
        └─ method_id 已注册
            │
            ▼
     taskThreadPool_.enqueue(handler)
        │
        ▼  (线程池中执行)
     handler(body) — 用户注册的业务逻辑
        │
        ▼  (切换回 IO 线程)
     ioloop->runInLoop([=] {
         encode() 编码响应
         conn->send() 发送响应
     })
```

- **不阻塞 IO 线程**：所有业务逻辑在 `TaskThreadPool` 线程池中异步执行
- **自动切换线程**：结果通过 `runInLoop()` 回到原 IO 线程发送响应

### 客户端核心流程

```
CallAsync<Req, Res>(req, timeout, method_id)
    │
    ▼
connPool_->acquire()  — 从连接池获取一个健康连接
    │
    ▼
conn->sendRequest(req, awaiter, timeout, method_id)
    │
    ├─ 1. 分配 seq
    ├─ 2. 构造 RPC_Meta + encode()
    ├─ 3. 在 pending_ 中注册 Awaiter 上下文
    │   ├─ onResponse: 收到响应时 setResponse + resume
    │   ├─ cancel:     超时时 setError + resume
    │   └─ timerId:    超时定时器
    ├─ 4. conn->sendWithRetransmit() 发送
    │
    ▼
co_await *awaiter  — 挂起当前协程
    │
    ▼  (响应到达 / 超时)
awaiter->resume()  — 恢复协程执行
    │
    ▼
Response res = co_await awaiter  — 返回解析后的响应
```

### 协程调用机制

本项目使用 **C++20 标准协程** 实现异步 RPC 调用，不依赖第三方协程库。

**核心组件：**

| 组件                        | 说明                                       |
| --------------------------- | ------------------------------------------|
| `Task<T>`                   | 通用的协程任务类型，支持 `co_return`        |
| `Task<void>`                | void 版本的协程任务特化                     |
| `RpcAwaiter<Response>`      | RPC 调用的 Awaiter，封装挂起/恢复逻辑       |
| `PooledConnection`          | 管理 `PendingContext` 请求上下文           |

**协程生命周期：**

```
CallAsync()
    │
    ▼
co_await awaiter        ←── 协程挂起（await_suspend）
    │                           ↑ 保存 continuation_
    ▼                           │
  [等待响应或超时]               │
    │                           │
    ├─ onResponse() ────────────┤ 恢复：
    │     awaiter->resume()     │ handle_.resume()
    │                           │
    └─ onTimeout() ─────────────┤ 恢复 + setError
          awaiter->setError()   │
          awaiter->resume()    ─┘
    │
    ▼
awaiter->await_resume()
    │
    ├─ error_ == true ──▶ 抛 RpcTimeoutException
    ├─ response_ 为空 ──▶ 抛 RpcConnectionException
    └─ 解析成功 ────────▶ 返回 Response 对象
```

**线程安全设计：**
- `awaiter->resume()` 检测当前线程是否为 IO 线程
- 如果不是 IO 线程，通过 `loop_->runInLoop()` 调度到 IO 线程恢复协程
- 避免在非 IO 线程直接操作协程句柄

## 服务注册与发现

### 接口设计

服务注册与发现通过抽象接口解耦，支持多种后端实现：

```
┌────────────────────────────────────────────────────────────────┐
│                   抽象接口层                                    │
│  ┌──────────────────────┐  ┌──────────────────────────┐        │
│  │ isServiceRegister    │  │ isServiceDiscovery       │        │
│  │──────────────────────│  │──────────────────────────│        │
│  │ + registerService()  │  │ + subscribe()            │        │
│  │ + deregisterService()│  │ + unsubscribe()          │        │
│  │ + shutdown()         │  │ + shutdown()             │        │
│  └──────────────────────┘  └──────────────────────────┘        │
└────────────────────────────────────────────────────────────────┘
                          ▲              ▲
                          │              │
           ┌──────────────┴──────────────┴──────────────┐
           │                                             │
┌─────────────────────────┐              ┌─────────────────────────┐
│ FileConfigRegister      │              │ EtcdRegister            │
│ (C++17 文件轮询)         │              │ (Etcd Lease + KeepAlive)│
│                         │              │                         │
│ 实现 isServiceRegister   │             │  实现 isServiceRegister  │
│ 实现 isServiceDiscovery  │             │                         │
└─────────────────────────┘              └─────────────────────────┘
                                                  │
                                          ┌───────┴────────┐
                                          │                │
                                 ┌──────────────┐  ┌──────────────┐
                                 │ EtcdRegister │  │EtcdDiscovery│
                                 │ (服务端注册)  │  │ (客户端发现) │
                                 └──────────────┘  └──────────────┘
```

| 特性                    | FileConfigRegister      | EtcdRegister + EtcdDiscovery|
| -----------------------| ------------------------ | ----------------------------|
| **后端存储**            | 本地 YAML 文件           | etcd 分布式 KV 存储          |
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

每个端点存储为一个独立文件，命名规则：`{serviceName}_{host}_{port}.yaml`

```yaml
# EchoService_127_0_0_1_9988.yaml
service: EchoService
host: "127.0.0.1"
port: 9988
weight: 1
```

**客户端轮询机制：**
- 定期扫描配置目录下的全部 YAML 文件（默认每 3 秒）
- 解析服务端点，增量更新
- 通过回调通知 `ConnectionPool` 更新连接池

### Etcd 注册中心 (EtcdRegister / EtcdDiscovery)

**EtcdRegister — 服务端注册：**

```
服务器启动
    │
    ▼
为每个服务创建一个 Lease（TTL=10s）
    │
    ▼
将端点信息写入 /clearmoon/services/{svc}/{host}:{port}
    │
    ▼
启动 KeepAlive 自动续约（每 3 秒）
    │
    ▼
进程退出 → deregisterService → 撤销 Lease → key 自动清理
```

**EtcdDiscovery — 客户端发现：**

```
subscribe(serviceName)
    │
    ▼
ls(/clearmoon/services/{svc}/) — 获取当前所有端点
    │
    ├─ 回调通知 ConnectionPool.updateServiceEndpoints()
    │
    ▼
启动 Watch(prefix, recursive)
    │
    ▼  (端点变更时)
再次 ls() 获取最新端点列表 → 回调通知
```

### ServiceDiscoverer 封装

`ServiceDiscoverer` 是对 `isServiceDiscovery` 接口的上层封装，用于简化单个服务的订阅管理：

```cpp
auto discoverer = std::make_shared<ServiceDiscoverer>(loop, "MyService", etcdDiscovery);
discoverer->setEndpointChangeCallback([](const std::vector<Endpoint>& eps) {
    for (auto& ep : eps)
        std::cout << "Endpoint: " << ep.address() << std::endl;
});
discoverer->start();
```

## 连接池与负载均衡

### 连接池架构

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
                └─ groups: vector<ServerConnGroup>
                    └─ ServerConnGroup
                        ├─ endpoint: Endpoint（具体服务地址）
                        └─ connections: vector<PooledConnection>
                            ├─ conn[0]
                            ├─ conn[1]
                            └─ ...
```

| 池类型       | 构造函数                                    | 用途                     |
| ------------ | ------------------------------------------- | ------------------------ |
| **静态池**   | `ConnectionPool(loop, addr, poolSize, strategy)` | 直连单个服务器            |
| **动态池**   | `ConnectionPool(loop, strategy, connPerServer)`  | 搭配服务发现自动管理多服务 |

### 负载均衡策略

| 策略                       | 说明                                                       |
| -------------------------- | ---------------------------------------------------------- |
| **RoundRobin** (轮询)       | 原子计数依次取模，均匀分配请求                              |
| **LeastConnection** (最少连接) | 选择 `activeRequest()` 最小的连接，动态负载感知           |
| **Random** (随机)           | 随机选择，最多重试 10 次                                   |

所有策略均支持静态池与动态池两种模式。

### 健康检查

- **定时执行**：默认每 5 秒调用 `doHealthCheck()`
- **检测标准**：`!isConnected() && (state == IDLE || state == UNHEALTHY)`
- **自动恢复**：对不健康连接调用 `conn->Connect()` 尝试重连
- **连接状态机**：

```
IDLE ──acquire()──▶ BUSY ──returnToIdleIfBusy()──▶ IDLE
  │                                                       │
  │ 断开连接                                               │ 健康检查发现
  ▼                                                       ▼
UNHEALTHY ──Connect()──▶ IDLE (重连成功后)
```

## RPC Client 详解

### 静态模式

直连已知地址的服务器，无需服务发现：

```cpp
cmlib::InetAddress addr("127.0.0.1", 9988);
auto client = std::make_shared<RPCClient>(&loop, addr);

// 调用时自动从连接池获取连接
auto res = co_await client->CallAsync<Req, Res>(req, timeout, method_id);
```

### 动态服务模式

通过服务发现获取可用服务器端点，自动管理多服务连接：

```cpp
auto discovery = std::make_shared<EtcdDiscovery>(&loop, "http://127.0.0.1:2379");
auto client = std::make_shared<RPCClient>(&loop, 2, discovery);

// 订阅服务（自动维护连接池）
client->subscribe("EchoService");
client->subscribe("CalcService");

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
            "EchoService", req, std::chrono::seconds(5), 0);

        LOG_INFO << "Echo reply: " << res.reply()
                 << " (code=" << res.code() << ")";
    } catch (const std::exception& e) {
        LOG_ERROR << "RPC call failed: " << e.what();
    }
}

// 启动异步调用
auto task = runEchoTest(client);
```

## 错误处理与异常

| 异常类型                    | 触发时机                       | 说明                         |
| --------------------------- | ------------------------------ | ---------------------------- |
| `RpcTimeoutException`       | 请求超时（超时期间未收到响应）  | 包含 seq 和 timeoutMs 信息    |
| `RpcConnectionException`    | 连接不可用 / 未连接 / 连接断开   | 包含具体错误描述             |

**超时处理双重保障：**

```
1. 用户协程 co_await 等待超时
   └─ await_resume() 检测到 error_ → 抛 RpcTimeoutException

2. PooledConnection::onTimeout() 定时器回调
   └─ 清理 pending_ 表中的请求上下文
       └─ 调用 cancel → awaiter->setError() + awaiter->resume()
```

**连接断开处理：**
- 调用 `cancelAllPending()` 取消所有待处理请求
- 待处理请求的 Awaiter 通过 `setError()` + `resume()` 唤醒协程
- 协程在 `await_resume()` 中检测空响应，抛出 `RpcConnectionException`

## 配置与服务注册

**FileConfigRegister 配置示例：**

配置目录结构：
```
/path/to/config/
├── EchoService_127_0_0_1_9988.yaml
├── EchoService_10_0_0_1_9988.yaml
└── CalcService_127_0_0_1_9989.yaml
```

**Etcd 服务注册配置：**

```cpp
// 服务端
auto registry = std::make_shared<EtcdRegister>(&loop, "http://127.0.0.1:2379", 10, 3);
RPCServer server(&loop, addr, registry, "MyService");

// 客户端
auto discovery = std::make_shared<EtcdDiscovery>(&loop, "http://127.0.0.1:2379");
auto client = std::make_shared<RPCClient>(&loop, 2, discovery);
client->subscribe("MyService");
```

## 测试示例

项目包含以下测试入口：  
注:Rpc_Client.cc和server_timeout_example.cc为早期测试文件，未同步

| 文件                        | 说明                                |
| --------------------------- | -----------------------------------|
| `Rpc_Server_test.cc`        | 服务端测试（带业务逻辑，含 Echo/Add） |
| `Rpc_Async_Client_Test.cc`  | 客户端协程异步调用测试               |
| `Client/Test/Rpc_Client.cc` | 客户端基础测试                      |
| `server_timeout_example.cc` | 超时处理示例                        |

**运行测试：**

```bash
# 终端1：启动服务端
./build/Rpc_Server_WithLogic

# 终端2：启动异步客户端
./build/Rpc_Async_Client_Test
```

## 许可证

该项目基于 [MIT License](LICENSE) 开源。