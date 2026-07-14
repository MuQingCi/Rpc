#include "ConnectionPool.h"
#include "PooledConnection.h"
#include "net/Log/Logger.h"

#include "net/EventLoop.h"
#include "net/TimerId.h"
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
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
        auto conn = std::make_shared<PooledConnection>(loop,serverAddr,i);
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
    healthCheckTimerId_ = loop_->runEvery(healthCheckInterval_.count(), [this] { doHealthCheck(); });
}

void ConnectionPool::stopHealthCheck()
{
    if(healthCheckTimerId_.valid())
    {
        loop_->cancel(healthCheckTimerId_);
        healthCheckTimerId_ = TimerId{};
    }
}

ConnectionPool::Borrowed ConnectionPool::acquire()
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
ConnectionPool::Borrowed ConnectionPool::acquireRoundRobin()
{
    std::unique_lock<std::mutex> lock(mutex_);

    size_t n = size();
    size_t start = rrIndex_.fetch_add(1,std::memory_order_relaxed) % n;

    for(size_t i=0; i<n; ++i)
    {
        size_t index = (start+i) % n;
        auto& conn = connections_[index];

        if(conn->isHealthy() && conn->state() == ConnState::IDLE) 
        {
            conn->setState(ConnState::BUSY);
            return Borrowed(conn);
        }
    }

    throw RpcConnectionException("No Healthy Connection avaiable!");
}

ConnectionPool::Borrowed ConnectionPool::acquireLeastConnection()
{
    std::unique_lock<std::mutex> lock(mutex_);

    ConnPtr bestConnPtr = nullptr;
    size_t min = std::numeric_limits<size_t>::max();

    for(auto& conn : connections_)
    {
        if(!conn->isHealthy() || conn->state() == ConnState::BUSY) continue;
        size_t active = conn->activeRequest();
        if(active<min)
        {
            min = active;
            bestConnPtr = conn;
        }
    }
    if(!bestConnPtr) 
        throw RpcConnectionException("No Healthy Connection avaiable!");

    bestConnPtr->setState(ConnState::BUSY);

    return Borrowed(bestConnPtr);
}

ConnectionPool::Borrowed ConnectionPool::acquireRandom()
{
    std::unique_lock<std::mutex> lock(mutex_);

    thread_local std::mt19937 rng(std::random_device{}());

    size_t n = connections_.size();
    
    std::uniform_int_distribution<size_t> dist(0,n-1);

    for(size_t attemp = 0; attemp<10; attemp++)
    {
        size_t idex = dist(rng);

        auto& conn = connections_[idex];
        if(conn->isHealthy() && conn->state() == ConnState::IDLE) 
        {
            conn->setState(ConnState::BUSY);
            return Borrowed(conn);
        }
    }
    throw RpcConnectionException("No Healthy Connection avaiable!");
}

void ConnectionPool::doHealthCheck()
{
    decltype(connections_) toReconnect;
    {
        std::unique_lock<std::mutex> lock(mutex_);

        for(auto& conn : connections_)
        {
            //检查是否处于连接状态
            if((conn->state() == ConnState::IDLE || conn->state() == ConnState::UNHEALTHY) && !conn->isConnected())
            {
                //断开则标记非健康
                conn->markUnHealthy();
                toReconnect.push_back(conn);
            }
        }
    }
    
    for(auto& conn : toReconnect)
    {
        loop_->runInLoop([conn]{ conn->Connect(); });
        LOG_INFO<< "ConnectionPool["<<conn->getIndex() << "]: Unhealthy, reconnecting...."; 
    }
}