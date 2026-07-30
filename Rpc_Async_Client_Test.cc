#include "Client/Client.h"
#include "EtcdDiscovery.h"
#include "Service/FileConfigRegister.h"
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
Task<int> testEcho(std::shared_ptr<RPCClient> client)
{
    CLRPC::EchoRequest req;
    req.set_msg("Hello from async client!");

    auto timeout = std::chrono::seconds(5);
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
Task<int> testAdd(std::shared_ptr<RPCClient> client)
{
    CLRPC::AddRequest req;
    req.set_a(100);
    req.set_b(200);

    auto timeout = std::chrono::seconds(5);
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
Task<int> runAllTests(std::shared_ptr<RPCClient> client)
{
    int passed = 0;

    std::cout << "\n========== Running Echo Test ==========" << std::endl;
    passed += co_await testEcho(client);

    std::cout << "\n========== Running Add Test ===========" << std::endl;
    passed += co_await testAdd(client);

    std::cout << "\n========== All Tests Completed ==========" << std::endl;
    std::cout << "Passed: " << passed << "/2" << std::endl;

    co_return passed;
}

int main()
{
    EventLoopThread loopThread;
    EventLoop* loop = loopThread.start();

    // InetAddress serverAddr("127.0.0.1", 1234, false);
    // auto discovery = std::make_shared<FileConfigRegister>(loop,
    // "./config",1.0,RegistryMode::Client);
    auto discovery = std::make_shared<EtcdDiscovery>(loop,"http://127.0.0.1:2379");

    int ret = 0;
    {
        auto rpcClient = std::make_shared<RPCClient>(loop, 4,discovery);

        rpcClient->subscribe("EchoService");
        rpcClient->subscribe("AddService");
        // 等待连接池建立 TCP 连接
        std::this_thread::sleep_for(std::chrono::seconds(1));

        try
        {
            int passed = runAllTests(rpcClient).get();
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