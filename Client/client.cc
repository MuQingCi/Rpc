#include "client.h"
#include "message.pb.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "net/Log/Logger.h"
#include <cstdint>
#include <mutex>
#include <utility>

RPCClient::RPCClient(EventLoop* loop, const InetAddress& serverAddr) : tcpClient_(loop, serverAddr), nextSeq_(1)
{
    tcpClient_.setConnectionCallback([this](TcpConnectionPtr conn){ onConnection(conn);});
    
    tcpClient_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm) { onMessage(conn, buff, tm); });
}

void RPCClient::onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm)
{
    LOG_INFO<< "New Message recived!";

    uint32_t minLen = sizeof(Header) + sizeof(RPC_Meta);

    //while中只解码，把业务数据body传给pending[it]->second后会在
    //lambda表达式中反序列化
    while(buff->readableBytes() >= minLen)
    {
        Header header{};
        RPC_Meta meta{};
        std::string body;

        if(!decode(buff, header, meta, body)) break;

        if(header.Flags == 1)
        {
            std::unique_lock<std::mutex> lock(mutex_);
            auto it = pending_.find(meta.seq);
            if(it != pending_.end())
            {
                it->second(body);
                pending_.erase(it);
            }
        }   
    }
}

void RPCClient::start()
{
    tcpClient_.connect();
}

