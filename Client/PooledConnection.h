#ifndef CLEARMOON_RPC_POOLCONNECTION_H
#define CLEARMOON_RPC_POOLCONNECTION_H

#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "net/TcpClient.h"
#include "ToolFunc.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <sys/types.h>
#include <utility>

namespace cmlib = clearmoon::net;

// 前置声明，打破循环依赖
// （RpcAwaiter.h → client.h → ConnectionPool.h → PooledConnection.h）
template<typename Response>
class RpcAwaiter;

enum ConnState{
    IDLE,
    BUSY,
    UNHEALTHY
};

class PooledConnection : public std::enable_shared_from_this<PooledConnection>
{
public:
using DisconnectCallback = std::function<void()>;
    PooledConnection(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr, size_t Index);
    ~PooledConnection();

    //连接
    void Connect() { tcpClient_.connect(); }
    //断开连接
    void Disconnect() { tcpClient_.disconnect();}

    bool isConnected() const { return conn_&&conn_->connected(); }

    ConnState state() const { return state_.load(std::memory_order_acquire); }
    void setState(ConnState state) { state_.store(state, std::memory_order_release); }

    void returnToIdleIfBusy(){
        ConnState expected = ConnState::BUSY;
        state_.compare_exchange_strong(expected, ConnState::IDLE);
    }
    //获取成员函数值
    cmlib::EventLoop* getLoop() const { return loop_; }
    size_t getIndex() const { return index_; }
    size_t activeRequest() const { return activerequests_.load(std::memory_order_relaxed); }

    void removePending(uint64_t seq);
    void cancelAllPending();

    void setOnDisconnected(DisconnectCallback cb) { onDisconnected_ = std::move(cb); }

    //判断连接是否健康与标记连接
    bool isHealthy() const { return isConnected() && state_ != ConnState::UNHEALTHY; }
    void markUnHealthy() { setState(ConnState::UNHEALTHY); }

    void send(cmlib::Buffer* buff)
    {
        conn_->send(buff);
    }

    template<typename Request, typename Response>
    void sendRequest(const Request& request,
                     std::shared_ptr<RpcAwaiter<Response>> awaiter,
                     std::chrono::milliseconds timeout,
                     uint32_t method_id);
private:
    //请求上下文
    struct PendingContext
    {
        std::shared_ptr<void> awaiter;  //类型擦除
        std::function<void()> cancel;
        std::function<void(std::string)> onResponse;

        cmlib::TimerId timerId;   //超时取消定时器
        uint64_t seq;             //请求序列号
        bool completed = false;   //是否完成
    };

    //三个回调
    void onMessage(const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm);
    void onConnection(const cmlib::TcpConnectionPtr& conn);
    void onTimeout(uint64_t seq);

    //成员变量
    //网络相关
    cmlib::TcpClient tcpClient_;
    cmlib::TcpConnectionPtr conn_;
    cmlib::EventLoop* loop_;
    size_t index_;  //该连接在连接池中的标识

    std::atomic<bool> destroyed_{false};  //析构标志，防止 use-after-free
    std::atomic<ConnState> state_{ConnState::IDLE};     //连接状态
    std::atomic<size_t>activerequests_;    //该连接中的活跃请求

    //请求序列号
    uint64_t nextSeq{1};
    //序列号对应的Awaiter
    std::map<uint64_t, PendingContext> pending_;

    mutable std::mutex mutex_;;

    DisconnectCallback onDisconnected_;
};

// ===================================================================
// 以下需要 RpcAwaiter 的完整定义，因此 include 放在类定义之后
// 打破了 PooledConnection.h → RpcAwaiter.h → client.h → ConnectionPool.h → PooledConnection.h 的循环
// ===================================================================
#include "RpcAwaiter.h"

template<typename Request, typename Response>
void PooledConnection::sendRequest(const Request& request,
                    std::shared_ptr<RpcAwaiter<Response>> awaiter,
                    std::chrono::milliseconds timeout,
                    uint32_t method_id)
{
    //1.获取seq
    uint64_t seq;
    {
        std::unique_lock<std::mutex> lock(mutex_);

        seq = nextSeq++;
    }

    //2.设置RPC_Meta
    RPC_Meta meta;
    meta.seq = seq;
    meta.method_id = method_id;
    meta.timeout = static_cast<uint32_t>(timeout.count());
    meta.err_code = 0;

    //3.编码为Buffer
    cmlib::Buffer sendBuff;
    encode(&sendBuff, 0, 1, meta, request);

    //4.在pendings_中注册
    {
        std::unique_lock<std::mutex> lock(mutex_);

        //先创建并完善PendingContext
        PendingContext ctx;
        awaiter->setSeq(seq);
        ctx.awaiter = awaiter;
        ctx.seq = seq;

        auto self = shared_from_this();

        ctx.cancel = [awaiter,seq,self]
        {
            awaiter->setError();
            awaiter->resume();
            if(self->conn_)
                self->conn_->ackReceived(seq);
            self->activerequests_.fetch_sub(1,std::memory_order_relaxed);
        };

        ctx.onResponse = [awaiter](std::string body){
            awaiter->setResponse(std::move(body));
            awaiter->resume();
        };

        ctx.timerId = loop_->runAfter(timeout.count() / 1000.0, [self,seq]{ self->onTimeout(seq); });

        ctx.completed = false;

        pending_[seq] = std::move(ctx);
        activerequests_.fetch_add(1,std::memory_order_relaxed);
    }

    if(conn_ && conn_->connected())
    {
        conn_->sendWithRetransmit(&sendBuff,seq);
    }
    else {
        onTimeout(seq);
    }
}

#endif  //CLEARMOON_RPC_POOLCONNECTION_H