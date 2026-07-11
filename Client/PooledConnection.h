#ifndef CLEARMOON_RPC_POOLCONNECTION_H
#define CLEARMOON_RPC_POOLCONNECTION_H

#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "net/TcpClient.h"
#include "RpcAwaiter.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sys/types.h>

namespace cmlib = clearmoon::net; 

enum ConnState{
    IDLE,
    BUSY,
    UNHEALTHY
};

class PooledConnection
{
public:
    PooledConnection(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr, size_t Index);

    //成员函数

    //连接与断开连接
    void Connect() { tcpClient_.connect(); }
    void Disconnect() { tcpClient_.disconnect();}

    bool isConnected() const { return conn_&&conn_->connected(); }

    ConnState state() const { return state_.load(std::memory_order_acquire); }
    void setState(ConnState state) { state_.store(state, std::memory_order_release); }

    //获取成员函数值
    cmlib::EventLoop* getLoop() const { return loop_; }
    size_t getIndex() const { return index_; }
    int activeRequest() const { return activerequests_.load(std::memory_order_relaxed); }


    //异步注册模板
    template<typename Request, typename Response>
    uint64_t registerPendingAsync(std::shared_ptr<RpcAwaiter<Response>> awaiter, std::chrono::milliseconds timeout);

    void removePending(uint64_t seq);
    void cancelAllPending();

    //判断连接是否健康与标记连接
    bool isHealthy() const { return isConnected() && state_ != ConnState::UNHEALTHY; }
    void markUnHealthy() { setState(ConnState::UNHEALTHY); }

    void send(cmlib::Buffer* buff)
    {
        conn_->send(buff);
    }
private:
    struct PendingContext
    {
        std::shared_ptr<void> awaiter;  //类型擦除
        std::function<void()> resume;
        cmlib::TimerId timerId;
        uint64_t seq;
        bool completed = false;
    };  

    //三个回调
    void onMessage(const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm);
    void onConnection(const cmlib::TcpConnectionPtr& conn);
    void OnTimeout(uint64_t seq);

    //成员变量
    cmlib::TcpClient tcpClient_;
    cmlib::TcpConnectionPtr conn_;
    cmlib::EventLoop* loop_;
    size_t index_;

    std::atomic<ConnState> state_{ConnState::IDLE};
    std::atomic<int>activerequests_;

    uint64_t nextSeq{1};
    std::map<uint64_t, PendingContext> pending_;

    mutable std::mutex mutex_;;
};


template<typename Request, typename Response>
uint64_t PooledConnection::registerPendingAsync(std::shared_ptr<RpcAwaiter<Response>> awaiter, std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(mutex_);
    uint64_t seq = nextSeq++;

    PendingContext ctx;
    ctx.seq = seq;
    ctx.awaiter = awaiter;
    
    ctx.resume = [awaiter]() mutable{
        awaiter->resume();
    };
 
    ctx.timerId = loop_->runAfter(timeout / 1000.0, [this,seq]() { OnTimeout(seq); });

    ctx.completed = false;
    pending_[seq] = std::move(ctx);

    activerequests_.fetch_add(1, std::memory_order_relaxed);
    return seq;
}


#endif  //CLEARMOON_RPC_POOLCONNECTION_H