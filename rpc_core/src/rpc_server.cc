#include "../include/rpc_server.h"
#include "message.pb.h"
#include "net/Buffer.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"

// Protobuf 消息定义
#include "message.pb.h"

//工具函数
#include "toolFunc.h"
#include <cstdint>
#include <string>

// using namespace clearmoon;
// using namespace clearmoon::net;

RPCServer::RPCServer(EventLoop* loop, InetAddress& listenAddr) : tcpServer_(loop, TcpServer::ThreadPoolInitCallback(), listenAddr)
{
    tcpServer_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm) { onMessage(conn, buff, tm); });
}

void RPCServer::onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm)
{   
    std::string body;
    Header header;

    while(decode(buff, header, body))
    {
        if(header.status == 0) //0请求 | 1回应
        {
            // if(header.totalLen < body.size()) continue;

            CLRPC::request req;
            if(!req.ParseFromString(body))
            {
                conn->shutdown();
                return;
            }

            std::string result = handleMessage(req.message());
            CLRPC::response rep;
            rep.set_message(result);

            uint16_t reqId = header.id;
            uint16_t status = 1;

            Buffer buff;
            encode(&buff, reqId, status, rep);
            conn->send(&buff);
        }
    }
}

std::string RPCServer::handleMessage(std::string msg)
{
    return "RPCServer Echo: " + msg;
}

void RPCServer::start()
{
    if(started_) return;
    tcpServer_.start();
}