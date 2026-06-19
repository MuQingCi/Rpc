#include "client.h"
#include "message.pb.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "toolFunc.h"
#include <future>
#include <mutex>

RPCClient::RPCClient(EventLoop* loop, const InetAddress& serverAddr) : tcpClient_(loop, serverAddr), nextId_(1)
{
    tcpClient_.setConnectionCallback([this](TcpConnectionPtr conn){ onConnection(conn);});
    tcpClient_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm) { onMessage(conn, buff, tm); });
}

void RPCClient::onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm)
{
    std::string body;
    Header header;
    while(decode(buff, header, body))
    {
        if(header.status == 1) //0请求/1响应
        {
            CLRPC::response res;
            if(!res.ParseFromString(body)) continue;;
            
            std::unique_lock<std::mutex> lock(mutex_);

            auto it = pending_.find(header.id);
            if(it != pending_.end())
            {
                it->second.set_value(std::move(res));
                pending_.erase(it);
            }
        }
        
    }
}

void RPCClient::start()
{
    tcpClient_.connect();
}

CLRPC::response RPCClient::Call(CLRPC::request& req)
{
    uint16_t id = nextId_++;
    
    std::promise<CLRPC::response> prom;
    std::future<CLRPC::response> fut = prom.get_future();

    {
        std::unique_lock<std::mutex> lock(mutex_);
        pending_[id] = std::move(prom);
    }

    Buffer sendBuff;
    encode(&sendBuff, id, 0, req);
    conn_->send(&sendBuff);

    return fut.get();
}