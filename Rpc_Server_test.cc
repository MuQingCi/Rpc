#include "EtcdRegister.h"
#include "Service/FileConfigRegister.h"
#include "TaskThreadPool.h"
#include "rpc_core/include/Rpc_server.h"
#include "ServerConfig.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "Message.pb.h"

#include "../include/Filter/TraceFilter.h"
#include "../include/Filter/MetricsFilter.h"
#include "../include/Filter/RateLimitFilter.h"
#include "../include/Filter/CircuitBreakerFilter.h"


#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>

/// @brief 带业务逻辑的 RPC 服务端测试程序
/// 注册 Echo 和 Add 两个函数，与 Rpc_Async_Client_Test 配合使用
int main()
{
    //读取配置文件（config/server.yaml）
    auto cfg = loadServerConfig(PROJECT_ROOT "/config/server.yaml");

    cmlib::EventLoop   loop;
    cmlib::InetAddress listenAddr(cfg.host, cfg.port, false);

    //注册中心（etcd / 文件配置二选一）
    auto taskPool = std::make_shared<TaskThreadPool>(cfg.threadPoolSize);
    taskPool->start();

    std::shared_ptr<isServiceRegister> registry;
    if(cfg.registryType == "file")
    {
        registry = std::make_shared<FileConfigRegister>(&loop, cfg.filePath, cfg.pollInterval, RegistryMode::Server);
    }
    else
    {
        registry = std::make_shared<EtcdRegister>(&loop, taskPool, cfg.etcdUrl, cfg.ttl, cfg.keepaliveInterval);
    }

    RPCServer server(&loop, listenAddr, registry, cfg.threadPoolSize);
    server.setServiceWeight(cfg.serviceWeight);

    //注册服务名
    for(const auto& svc : cfg.services)
    {
        server.addService(svc);
    }

    //添加过滤器
    auto bucket = std::make_shared<TokenBucket>(cfg.rateLimit, cfg.bucketCapacity);//令牌/秒，最大容量

    //按照TraceID过滤器、性能度量过滤器、流量控制过滤器、熔断过滤器的顺序添加
    if(cfg.trace)
        server.addFilter(std::make_shared<TraceFilter>());
    if(cfg.metrics)
        server.addFilter(std::make_shared<MetricsFilter>());
    if(cfg.rateLimit > 0)
        server.addFilter(std::make_shared<RateLimitFilter>(bucket));
    server.addFilter(std::make_shared<CircuitBreakerFilter>(cfg.breakerErrorRate, cfg.breakerWindow));

    // ---- 注册 Echo 函数 ----
    server.registerMethod<CLRPC::EchoRequest, CLRPC::EchoResponse>(
        [](CLRPC::EchoRequest& req) -> std::unique_ptr<CLRPC::EchoResponse>
        {
            auto resp = std::make_unique<CLRPC::EchoResponse>();
            resp->set_reply("Server Echo: " + req.msg());
            resp->set_code(0);
            return resp;
        });

    // ---- 注册 Add 函数 ----
    server.registerMethod<CLRPC::AddRequest, CLRPC::AddResponse>(
        [](CLRPC::AddRequest& req) -> std::unique_ptr<CLRPC::AddResponse>
        {
            auto resp = std::make_unique<CLRPC::AddResponse>();
            resp->set_result(req.a() + req.b());
            // Error 字段默认为空字符串
            return resp;
        });

    std::cout << "RPC Server running on " << cfg.host << ":" << cfg.port << " ..." << std::endl;
    std::cout << "Registered methods: Echo (id=0), Add (id=1)" << std::endl;

    server.start();
    loop.loop();

    return 0;
}