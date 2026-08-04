#include "EtcdRegister.h"
#include "Service/FileConfigRegister.h"
#include "TaskThreadPool.h"
#include "rpc_core/include/Rpc_server.h"
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
    cmlib::EventLoop   loop;
    cmlib::InetAddress listenAddr("127.0.0.1", 12345, false);

    //文件配置
    // auto registry = std::make_shared<FileConfigRegister>(&loop, "./config",5.0,RegistryMode::Server);

    //etcd配置
    auto taskPool = std::make_shared<TaskThreadPool>(2);
    taskPool->start();
    auto registry = std::make_shared<EtcdRegister>(&loop, taskPool, "http://127.0.0.1:2379",10,3);

    RPCServer server(&loop, listenAddr,registry);

    //注册服务名
    server.addService("EchoService");
    server.addService("AddService");

    //添加过滤器
    auto bucket = std::make_shared<TokenBucket>(100, 200);//100令牌/秒，最大容量200

    //按照TraceID过滤器、性能度量过滤器、流量控制过滤器、熔断过滤器的顺序添加
    server.addFilter(std::make_shared<TraceFilter>());
    server.addFilter(std::make_shared<MetricsFilter>());
    server.addFilter(std::make_shared<RateLimitFilter>(bucket));
    server.addFilter(std::make_shared<CircuitBreakerFilter>(60, std::chrono::seconds(10))); //失误率为60%,每10s更新熔断窗口

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

    std::cout << "RPC Server running on 127.0.0.1:12345 ..." << std::endl;
    std::cout << "Registered methods: Echo (id=0), Add (id=1)" << std::endl;

    server.start();
    loop.loop();

    return 0;
}