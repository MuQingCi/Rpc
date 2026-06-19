#ifndef CLEARMOON_RCP_SERVER_H
#define CLEARMOON_RCP_SERVER_H

// ClearMoon 网络库
#include "net/TcpServer.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"

#include <netinet/in.h>
#include <string>

using namespace clearmoon;
using namespace clearmoon::net;

class RPCServer
{
public:
    RPCServer(EventLoop* loop, InetAddress& listenAddr);

    void onMessage(const TcpConnectionPtr&, Buffer*, Timestamp);

    void start();
private:
    std::string handleMessage(std::string msg);

    TcpServer tcpServer_;

    bool started_ = false;
};
#endif
