#ifndef CLEARMOON_RPC_CONNECTIONPOOL_h
#define CLEARMOON_RPC_CONNECTIONPOOL_h

#include "PooledConnection.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TimerId.h"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <vector>

namespace cmlib = clearmoon::net;

//负载均衡策略
enum class LoadBalanceStrategy
{
    RoundRobin,         //轮询
    LeastConnection,    //最少连接数
    Random              //随机
};

class ConnectionPool
{
public:
    ConnectionPool(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr, size_t poolSize, LoadBalanceStrategy strategy = LoadBalanceStrategy::RoundRobin);

    ~ConnectionPool();

    //RAII借用句柄
    /**
     * @brief 内部封装了池化连接的shared_ptr,创建时自动赋值使得对应指针计数+1,析构时则自动将其计数-1
     * 
     */
    class Borrowed
    {
    public:
        Borrowed(std::shared_ptr<PooledConnection> conn) : conn_(std::move(conn)){}
        ~Borrowed() 
        { 
            if(conn_)
            {
                conn_->returnToIdleIfBusy();
            }
        };

        //运算符重载--以便该类可以像原始指针一样使用
        PooledConnection* operator->() const { return conn_.get(); }
        PooledConnection& operator*() const { return *conn_; }

        //禁止拷贝
        Borrowed(const Borrowed&) = delete;
        Borrowed& operator=(const Borrowed&) = delete;

        //允许移动
        Borrowed(Borrowed&&) = default;
        Borrowed& operator=(Borrowed&&) = default;
    private:
        std::shared_ptr<PooledConnection> conn_;        
    };

    //开始/关闭心跳检查
    void startHealthCheck();
    void stopHealthCheck();

    //获取一条可用的连接(Borrowed类)
    Borrowed acquire();

    size_t size() const { return connections_.size(); }

private:
    //负载均衡策略对应函数
    Borrowed acquireRoundRobin();
    Borrowed acquireLeastConnection();
    Borrowed acquireRandom();

    void doHealthCheck();

    using ConnPtr = std::shared_ptr<PooledConnection>; 

    //网络相关
    cmlib::EventLoop* loop_;
    cmlib::InetAddress serverAddr_;

    //连接池相关
    size_t connNum_;
    LoadBalanceStrategy strategy_;

    std::vector<ConnPtr> connections_;
    std::atomic<size_t> rrIndex_{0};

    //健康检查相关
    cmlib::TimerId healthCheckTimerId_;
    std::chrono::seconds healthCheckInterval_{5};
    
    mutable std::mutex mutex_;
};
#endif