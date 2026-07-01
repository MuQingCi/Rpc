#include "client.h"
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
    AsyncLogger asyncLogger("ClearMoon_RPC", 1 * 1024, LOG_DIR);
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

        CLRPC::request req;
        req.set_message("Hello RPC Server!");

        CLRPC::response res = rpcClient.Call(req);
        std::cout << "Response: " << res.message() << std::endl;

        loop.quit();
    });

    loop.loop();
    t.join();

    return 0;
}