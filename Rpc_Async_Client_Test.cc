#include "CircuitBreakerFilter.h"
#include "Client/Client.h"
#include "Client/ClientConfig.h"
#include "EtcdDiscovery.h"
#include "MetricsFilter.h"
#include "RateLimitFilter.h"
#include "Service/FileConfigRegister.h"
#include "TokenBucket.h"
#include "TraceFilter.h"
#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "net/Log/Logger.h"
#include "Message.pb.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

using namespace clearmoon;
using namespace clearmoon::net;

/// @brief 测试 Echo 函数（method_id = 0），返回 1=通过, 0=失败
Task<int> testEcho(std::shared_ptr<RPCClient> client, std::chrono::milliseconds timeout)
{
    CLRPC::EchoRequest req;
    req.set_msg("Hello from async client!");

    uint32_t method_id = static_cast<uint32_t>(MethodID::Echo);

    CLRPC::EchoResponse res =
        co_await client->CallAsync<CLRPC::EchoRequest, CLRPC::EchoResponse>("EchoService",
            req, timeout, method_id);

    std::cout << "[Echo Test] reply: " << res.reply()
              << ", code: " << res.code() << std::endl;

    if (res.reply() == "Server Echo: Hello from async client!" && res.code() == 0)
    {
        std::cout << "[Echo Test] PASSED" << std::endl;
        co_return 1;
    }
    else
    {
        std::cout << "[Echo Test] FAILED" << std::endl;
        co_return 0;
    }
}

/// @brief 测试 Add 函数（method_id = 1），返回 1=通过, 0=失败
Task<int> testAdd(std::shared_ptr<RPCClient> client, std::chrono::milliseconds timeout)
{
    CLRPC::AddRequest req;
    req.set_a(100);
    req.set_b(200);

    uint32_t method_id = static_cast<uint32_t>(MethodID::Add);

    CLRPC::AddResponse res =
        co_await client->CallAsync<CLRPC::AddRequest, CLRPC::AddResponse>("AddService",
            req, timeout, method_id);

    std::cout << "[Add Test] result: " << res.result()
              << ", error: " << res.error() << std::endl;

    if (res.result() == 300 && res.error().empty())
    {
        std::cout << "[Add Test] PASSED" << std::endl;
        co_return 1;
    }
    else
    {
        std::cout << "[Add Test] FAILED" << std::endl;
        co_return 0;
    }
}

/// @brief 汇集所有测试，返回通过的测试数
Task<int> runAllTests(std::shared_ptr<RPCClient> client, std::chrono::milliseconds timeout)
{
    int passed = 0;

    std::cout << "\n========== Running Echo Test ==========" << std::endl;
    passed += co_await testEcho(client, timeout);

    std::cout << "\n========== Running Add Test ===========" << std::endl;
    passed += co_await testAdd(client, timeout);

    std::cout << "\n========== All Tests Completed ==========" << std::endl;
    std::cout << "Passed: " << passed << "/2" << std::endl;

    co_return passed;
}

int main()
{
    //读取配置文件（config/client.yaml）
    auto cfg = loadClientConfig(PROJECT_ROOT "/config/client.yaml");

    EventLoopThread loopThread;
    EventLoop* loop = loopThread.start();

    //服务发现（etcd / 文件配置二选一）
    std::shared_ptr<isServiceDiscovery> discovery;
    if(cfg.registryType == "file")
    {
        discovery = std::make_shared<FileConfigRegister>(loop,
            cfg.configFileDir, cfg.pollInterval, RegistryMode::Client);
    }
    else
    {
        discovery = std::make_shared<EtcdDiscovery>(loop, cfg.etcdUrl);
    }

    int ret = 0;
    {
        auto rpcClient = std::make_shared<RPCClient>(loop, cfg, discovery);

        auto bucket = std::make_shared<TokenBucket>(cfg.rateLimit, cfg.bucketCapacity);

        //添加过滤器
        if(cfg.trace)
            rpcClient->addFilter(std::make_shared<TraceFilter>());
        if(cfg.metrics)
            rpcClient->addFilter(std::make_shared<MetricsFilter>());
        if(cfg.rateLimit > 0)
            rpcClient->addFilter(std::make_shared<RateLimitFilter>(bucket));
        rpcClient->addFilter(std::make_shared<CircuitBreakerFilter>(cfg.breakerErrorRate, cfg.breakerWindow));

        //订阅服务
        for(const auto& svc : cfg.subscribeServices)
        {
            rpcClient->subscribe(svc);
        }

        // 等待连接池建立 TCP 连接
        std::this_thread::sleep_for(std::chrono::seconds(1));

        try
        {
            int passed = runAllTests(rpcClient, cfg.callTimeout).get();
            if (passed < 2) ret = 1;
        }
        catch (const std::exception& e)
        {
            std::cerr << "\n[FATAL] Test failed with exception: " << e.what()
                      << std::endl;
            ret = 1;
        }
    } // rpcClient 析构，触发 ConnectionPool 析构（Disconnect + cancelAllPending）

    // 退出事件循环，使 EventLoopThread 内部的 threadFunc 返回
    loop->quit();

    return ret;
}