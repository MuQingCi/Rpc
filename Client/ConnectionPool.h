#ifndef CLEARMOON_RPC_CONNECTIONPOOL_h
#define CLEARMOON_RPC_CONNECTIONPOOL_h

#include "PooledConnection.h"
#include "Service/Endpoint.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TimerId.h"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
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
using ConnPtr = std::shared_ptr<PooledConnection>; 
using ServerKey = std::string;

    //无服务分发版
    ConnectionPool(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr, size_t poolSize, LoadBalanceStrategy strategy = LoadBalanceStrategy::RoundRobin);

    //服务分发版
    ConnectionPool(cmlib::EventLoop* loop, 
                   const cmlib::InetAddress& serverAddr, 
                   size_t poolSize, 
                   LoadBalanceStrategy strategy,
                   size_t connPerServer
                );

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
    //带服务发现版
    Borrowed acquire(const ServerKey& key);

    size_t size() const { return connections_.size(); }

    void updateEndpoints(const std::vector<Endpoint>& epVec);

private:

    struct ServerConnGroup{
        Endpoint endpoint;
        std::vector<ConnPtr> connections;
    };
    //返回 host:port
    std::string makeServerKey(const Endpoint& ep);

    //负载均衡策略对应函数
    Borrowed acquireRoundRobin();
    Borrowed acquireLeastConnection();
    Borrowed acquireRandom();

    //服务发现版
    Borrowed acquireRoundRobin(const ServerKey& key);
    Borrowed acquireLeastConnection(const ServerKey& key);
    Borrowed acquireRandom(const ServerKey& key);


    void doHealthCheck();

    //网络相关
    cmlib::EventLoop* loop_;
    cmlib::InetAddress serverAddr_;

    //连接池相关
    size_t connNum_;
    LoadBalanceStrategy strategy_;

    std::vector<ConnPtr> connections_;
    std::atomic<size_t> rrIndex_{0};

    //服务发现相关
    std::map<ServerKey, ServerConnGroup> servers_;
    std::vector<Endpoint> currentEndpoints_;

    size_t connPerServer_;
    std::map<ServerKey, std::atomic<size_t>> roundRobinIdex_;

    //健康检查相关
    cmlib::TimerId healthCheckTimerId_;
    std::chrono::seconds healthCheckInterval_{5};
    
    mutable std::mutex mutex_;
};
#endif