#include "ConnectionPool.h"
#include "PooledConnection.h"
#include "client.h"
#include "net/Log/Logger.h"

#include "net/EventLoop.h"
#include "net/TimerId.h"
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <random>
#include <utility>

ConnectionPool::ConnectionPool(cmlib::EventLoop* loop, 
                               const cmlib::InetAddress& serverAddr, 
                               size_t poolSize, 
                               LoadBalanceStrategy strategy) 
                               : loop_(loop),
                                 serverAddr_(serverAddr),
                                 connNum_(poolSize),
                                 strategy_(strategy)
{
    for(size_t i = 0; i<poolSize; ++i)
    {
        auto conn = std::make_unique<PooledConnection>(loop,serverAddr,i);
        conn->Connect();
        connections_.push_back(std::move(conn));
    }
    startHealthCheck();
}

ConnectionPool::~ConnectionPool()
{
    stopHealthCheck();
    for(auto& ky : connections_)
    {
        ky->cancelAllPending();
        ky->Disconnect();
    }
}

void ConnectionPool::startHealthCheck()
{
    healthCheckTimerId_ = loop_->runAfter(healthCheckInterval_.count(), [this] { doHealthCheck(); });
}

void ConnectionPool::stopHealthCheck()
{
    if(healthCheckTimerId_.valid())
    {
        loop_->cancel(healthCheckTimerId_);
        healthCheckTimerId_ = TimerId{};
    }
}

PooledConnection& ConnectionPool::acquire()
{
    switch (strategy_) {
        case LoadBalanceStrategy::RoundRobin :
            return acquireRoundRobin();
        case LoadBalanceStrategy::LeastConnection : 
            return acquireLeastConnection();
        case LoadBalanceStrategy::Random : 
            return acquireRandom();
        default:
            return acquireRoundRobin();
    }
}


//负载均衡策略对应函数
PooledConnection& ConnectionPool::acquireRoundRobin()
{
    size_t n = size();
    size_t start = rrIndex_.fetch_add(1,std::memory_order_relaxed) % n;

    for(size_t i=0; i<n; ++i)
    {
        size_t index = (start+i) % n;
        auto& conn = connections_[index];

        if(conn->isHealthy()) 
            return *conn;
    }

    throw RpcConnectionException("No Healthy Connection avaiable!");
}

PooledConnection& ConnectionPool::acquireLeastConnection()
{
    PooledConnection* bestConnPtr = nullptr;
    size_t min = std::numeric_limits<size_t>::max();

    for(auto& conn : connections_)
    {
        if(!conn->isHealthy()) continue;
        int active = conn->activeRequest();
        if(active<min)
        {
            min = active;
            bestConnPtr = conn.get();
        }
    }
    if(!bestConnPtr) 
        throw RpcConnectionException("No Healthy Connection avaiable!");

    return *bestConnPtr;
}

PooledConnection& ConnectionPool::acquireRandom()
{
    thread_local std::mt19937 rng(std::random_device{}());

    size_t n = connections_.size();
    
    std::uniform_int_distribution<size_t> dist(0,n-1);

    for(size_t attemp = 0; attemp<10; attemp++)
    {
        size_t idex = dist(rng);

        auto& conn = connections_[idex];
        if(conn->isHealthy()) return *conn;
    }
    throw RpcConnectionException("No Healthy Connection avaiable!");
}

void ConnectionPool::doHealthCheck()
{
    for(auto& conn : connections_)
    {
        //检查是否处于连接状态
        if(!conn->isConnected())
        {
            //断开则标记非健康且重连
            conn->markUnHealthy();

            loop_->runInLoop([&conn = *conn]{conn.Connect();});

            LOG_INFO<< "ConnectionPool["<<conn->getIndex() << "]: Unhealthy, reconnecting...."; 
        }
    }
}