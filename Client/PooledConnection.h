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
#include <string>
#include <sys/types.h>
#include <utility>

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

    //连接
    void Connect() { tcpClient_.connect(); }
    //断开连接
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
    //请求上下文
    struct PendingContext
    {
        std::shared_ptr<void> awaiter;  //类型擦除
        //函数闭包
        std::function<void()> resume;
        std::function<void()> cancel;
        std::function<void(std::string)> setResponse;

        
        cmlib::TimerId timerId;   //超时取消定时器
        uint64_t seq;             //请求序列号
        bool completed = false;   //是否完成

        //重传相关
        std::string requestPayload;
        size_t retriesLeft = 4;   //剩余重传次数
        std::chrono::milliseconds retryInterval;            //重试间隔
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

    std::atomic<ConnState> state_{ConnState::IDLE};     //连接状态
    std::atomic<int>activerequests_;    //该连接中的活跃请求

    //请求序列号
    uint64_t nextSeq{1};
    //序列号对应的Awaiter
    std::map<uint64_t, PendingContext> pending_;

    mutable std::mutex mutex_;;
};


//异步注册模板
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

    ctx.setResponse = [awaiter](std::string body) mutable 
    {
        awaiter->setResponse(std::move(body));
    };

    ctx.cancel = [awaiter]() {
        awaiter->setError();
        awaiter->resume();
    };

    ctx.timerId = loop_->runAfter(timeout / 1000.0, [this,seq]() { onTimeout(seq); });

    ctx.completed = false;
    pending_[seq] = std::move(ctx);

    activerequests_.fetch_add(1, std::memory_order_relaxed);
    return seq;
}


#endif  //CLEARMOON_RPC_POOLCONNECTION_H