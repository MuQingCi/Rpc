#ifndef CLEARMOON_RPC_RPCAWAITER_H
#define CLEARMOON_RPC_RPCAWAITER_H

#include "client.h"
#include "net/EventLoop.h"
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <string>
#include <sys/types.h>
#include <utility>

namespace cmlib = clearmoon::net;


template<typename Response>
class RpcAwaiter
{
public:
    RpcAwaiter(cmlib::EventLoop* loop, uint64_t seq, std::chrono::milliseconds timeout) : loop_(loop), seq_(seq), timeout_(timeout){}

    //----Awaiter接口----
    bool await_ready() const noexcept { return error_ || !response_.empty(); }
    void await_suspend(std::coroutine_handle<> handle) noexcept { 
        handle_ = handle;
    }
    Response await_resume()
    {
        if(error_)
            throw RpcTimeoutException(seq_, static_cast<uint32_t>(timeout_.count()));
        if(response_.empty())
            throw RpcConnectionException("Connection closed or cancelled!");
        Response res;
        if(!res.ParseFromString(response_))
            throw RpcConnectionException("RPC response parse failed!");
        return res;
    }

    //成员函数
    void setResponse(std::string body) { response_ = std::move(body); }
    void setError() { error_ = true; }

    void resume()
    {
        if(handle_)
        {
            auto h = std::exchange(handle_, nullptr);

            if(loop_ && !loop_->isInThread())
            {
                loop_->runInLoop([h]() mutable { h.resume(); });
            }
            else if(loop_->isInThread()){
                h.resume();
            }
        }
    }

    uint64_t seq() const { return seq_; }
    void setSeq(uint64_t seq) { seq_ = seq; }

private:
    cmlib::EventLoop* loop_;
    uint64_t seq_;
    std::chrono::milliseconds timeout_;
    std::coroutine_handle<> handle_;
    std::string response_;
    bool error_{false};
};

#endif //CLEARMOON_RPC_RPCAWAITER_H