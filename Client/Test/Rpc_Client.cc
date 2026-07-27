#include "Client.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Log/Logger.h"
#include "net/Log/AsyncLogger.h"

#include <chrono>
#include <iostream>
#include <thread>

#define LOG_DIR PROJECT_ROOT "/Client_Log"

int main()
{
    AsyncLogger asyncLogger("ClearMoon_RPC_Client", 1 * 1024, LOG_DIR);
    Logger::set_AsyncLogger(&asyncLogger);
    asyncLogger.start();
    Logger::set_GlobalLevel(net::INFO);


    EventLoop loop;
    InetAddress serverAddr("127.0.0.1", 1234, false);

    RPCClient rpcClient(&loop, serverAddr);
    rpcClient.start();

    std::thread t([&]() {
        // 轮询等待连接建立
        while (!rpcClient.connected())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

        CLRPC::EchoRequest req;
        req.set_msg("Hello RPC Server!");

        try
        {
            CLRPC::EchoResponse res = rpcClient.Call<CLRPC::EchoRequest, CLRPC::EchoResponse>(req);
            std::cout << "Response: " << res.reply() << " (code=" << res.code() << ")" << std::endl;
        }
        catch (const std::exception& e)
        {
            std::cerr << "RPC call failed: " << e.what() << std::endl;
        }

        loop.quit();
    });

    loop.loop();
    t.join();

    return 0;
}