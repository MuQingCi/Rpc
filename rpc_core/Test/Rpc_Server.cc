#include "../include/rpc_server.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"

int main()
{
    EventLoop loop;
    InetAddress listenAddr("127.0.0.1", 1234, false);

    RPCServer rpcServer(&loop, listenAddr);

    rpcServer.start();

    loop.loop();
    return 0;
}