#ifndef CLEARMOON_RPC_CLIENT_H
#define CLEARMOON_RPC_CLIENT_H

#include "message.pb.h"
#include "net/Callbacks.h"
#include "net/TcpClient.h"
#include "net/TcpConnection.h"

#include <cstdint>
#include <future>
#include <mutex>

using namespace clearmoon;
using namespace clearmoon::net;

class RPCClient
{
public:
    RPCClient(EventLoop* loop, const InetAddress& serverAddr);
    
    ~RPCClient() { tcpClient_.disconnect(); }

    void onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm);
    void onConnection(const TcpConnectionPtr& conn)
    {
        if(conn->connected()) conn_ = conn;
        else conn_.reset();
    }
    bool connected() const { return conn_ && conn_->connected(); }
    CLRPC::response Call(CLRPC::request& req);

    void start();

private:
    TcpClient tcpClient_;
    TcpConnectionPtr conn_;

    uint16_t nextId_;

    std::map<uint16_t, std::promise<CLRPC::response> >pending_;

    std::mutex mutex_;
};

#endif