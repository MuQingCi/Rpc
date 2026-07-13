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

    //开始/关闭心跳检查
    void startHealthCheck();
    void stopHealthCheck();

    //获取一条可用的连接
    PooledConnection& acquire();

    void release(PooledConnection& conn)
    {
        (void) conn;
    }

    size_t size() const { return connections_.size(); }

private:
    //负载均衡策略对应函数
    PooledConnection& acquireRoundRobin();
    PooledConnection& acquireLeastConnection();
    PooledConnection& acquireRandom();

    void doHealthCheck();

    using ConnPtr = std::unique_ptr<PooledConnection>; 

    //网络相关
    cmlib::EventLoop* loop_;
    cmlib::InetAddress serverAddr_;

    //连接池相关
    size_t connNum_;
    LoadBalanceStrategy strategy_;

    std::vector<ConnPtr> connections_;
    std::atomic<size_t> rrIndex_{0};

    //健康检查相关
    TimerId healthCheckTimerId_;
    std::chrono::seconds healthCheckInterval_{5};
    
};
#endif