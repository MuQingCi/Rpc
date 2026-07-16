#include "rpc_core/include/rpc_server.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "message.pb.h"

#include <iostream>
#include <memory>

/// @brief 带业务逻辑的 RPC 服务端测试程序
/// 注册 Echo 和 Add 两个函数，与 Rpc_Async_Client_Test 配合使用
int main()
{
    EventLoop   loop;
    InetAddress listenAddr("127.0.0.1", 1234, false);

    RPCServer server(&loop, listenAddr);

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

    std::cout << "RPC Server running on 127.0.0.1:1234 ..." << std::endl;
    std::cout << "Registered methods: Echo (id=0), Add (id=1)" << std::endl;

    server.start();
    loop.loop();

    return 0;
}