#include "../include/rpc_server.h"
#include "message.pb.h"
#include "net/Buffer.h"
#include "net/Log/Logger.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"

// Protobuf 消息定义
#include "message.pb.h"

//工具函数
#include "toolFunc.h"
#include <cstdint>
#include <string>

RPCServer::RPCServer(EventLoop* loop, InetAddress& listenAddr) : tcpServer_(loop, TcpServer::ThreadPoolInitCallback(), listenAddr)
{
    tcpServer_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm) { onMessage(conn, buff, tm); });
}

void RPCServer::onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm)
{   

    uint32_t minLen = sizeof(Header) + sizeof(RPC_Meta);
    // while(decode(buff, header, body))
    while(buff->readableBytes() >= minLen)
    {
        Header header;
        RPC_Meta meta;
        std::string body;
        
        if(!decode(buff, header, meta, body)) break;

        //此时的body中存储的是序列化后的数据，需要反序列化
        if(header.Flags == 0) //0请求 | 1回应
        {
            uint32_t methodId = meta.method_id;
            auto it = handles_.find(methodId);
            if(it != handles_.end())
            {
                auto res = it->second(body);
                if(res)
                {
                    Buffer sendBuff;
                    encode(&sendBuff, 1, 1, meta, *res);
                    conn->send(&sendBuff);
                }
            }
            else 
            {
                // LOG_INFO<<"一个非法函数调用";
                // meta.err_code = kValidFunc; 
                
            }
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

