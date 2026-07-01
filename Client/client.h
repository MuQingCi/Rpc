#ifndef CLEARMOON_RPC_CLIENT_H
#define CLEARMOON_RPC_CLIENT_H

#include "message.pb.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "net/TcpClient.h"
#include "net/TcpConnection.h"
#include "toolFunc.h"

#include <cstdint>
#include <functional>
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
    
    template<typename Request, typename Response>
    Response Call(Request& req);

    void start();

private:
    template<typename Response> 
    uint64_t registerPending(std::promise<Response>&& prom);

    TcpClient tcpClient_;
    TcpConnectionPtr conn_;

    uint64_t nextSeq_;

    std::map<uint64_t, std::function<void(const std::string&)>> pending_;

    std::mutex mutex_;
};



template<typename Request, typename Response>
Response RPCClient::Call(Request& req)
{
    std::promise<Response> prom;
    std::future<Response> fut = prom.get_future();
    
    uint64_t seq = registerPending(std::move(prom));

    RPC_Meta meta{};
    meta.seq = seq;
    meta.method_id = getMethodId<Request>();
    meta.timeout = 300;
    meta.err_code = 0;

    Buffer sendBuff;
    encode(&sendBuff, 0, 1, meta, req);
    conn_->send(&sendBuff);

    return fut.get();
}

template<typename Response> 
uint64_t RPCClient::registerPending(std::promise<Response>&& prom)
{
    std::unique_lock<std::mutex> lock(mutex_);

    uint64_t seq = nextSeq_++;

    pending_[seq] = [prom = std::move(prom)](const std::string& body) mutable
    {
        Response res;
        if(res.ParseFromString(body))
        {
            prom.set_value(std::move(res));
        }
        else {
            prom.set_exception(
                std::make_exception_ptr(std::runtime_error("Parse faild"))
            );
        }
    };
    
    return seq;
}

#endif