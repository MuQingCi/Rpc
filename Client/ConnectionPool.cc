#include "ConnectionPool.h"
#include "PooledConnection.h"
#include "Service/Endpoint.h"
#include "net/InetAddress.h"
#include "net/Log/Logger.h"

#include "net/EventLoop.h"
#include "net/TimerId.h"
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

//无服务分发
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

//服务分发版
ConnectionPool::ConnectionPool(cmlib::EventLoop* loop, 
                               const cmlib::InetAddress& serverAddr, 
                               size_t poolSize, 
                               LoadBalanceStrategy strategy,
                               size_t connPerServer)
                             : loop_(loop),
                                serverAddr_(serverAddr),
                                connNum_(poolSize),
                                strategy_(strategy),
                               connPerServer_(connPerServer)
{
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
        healthCheckTimerId_ = cmlib::TimerId{};
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

ConnectionPool::Borrowed ConnectionPool::acquire(const ServerKey& key)
{
    switch (strategy_) {
        case LoadBalanceStrategy::RoundRobin :
            return acquireRoundRobin(key);
        case LoadBalanceStrategy::LeastConnection : 
            return acquireLeastConnection(key);
        case LoadBalanceStrategy::Random : 
            return acquireRandom(key);
        default:
            return acquireRoundRobin(key);
    }
}

void ConnectionPool::updateEndpoints(const std::vector<Endpoint>& epVec)
{
    std::unique_lock<std::mutex> lock(mutex_);
    
    //将新的端点的ServerKey添加进集合中
    std::set<std::string> newEndpoints;
    for(auto& ep : epVec)
        newEndpoints.insert(makeServerKey(ep));

    //1：将新旧端点列表转化为易于比较的集合
    //将现有的旧ServerKey添加进集合中
    std::set<std::string> oldEndpoints;
    for(auto& [ky,_] : servers_)
        oldEndpoints.insert(ky);

    //2.找出已经被删除的ServerKey,关闭其对应的group中的连接
    for(auto& key : oldEndpoints)
    {
        if(newEndpoints.find(key) == newEndpoints.end())
        {
            auto& group = servers_[key];
            for(auto& conn : group.connections)
                conn->Disconnect();
            servers_.erase(key);
            if(strategy_ == LoadBalanceStrategy::RoundRobin)
                roundRobinIdex_.erase(key);
        }
    }

    //3.找出新的ServerKey并建立连接
    for(auto& ep : epVec)
    {
        ServerKey key = makeServerKey(ep);
        if(oldEndpoints.find(key) == oldEndpoints.end())
        {
            ServerConnGroup group;
            group.endpoint = ep;
            for(size_t i = 0; i<connPerServer_; ++i)
            {
                auto conn = std::make_shared<PooledConnection>(loop_,cmlib::InetAddress(ep.host,ep.port,false), i);
                conn->Connect();
                group.connections.push_back(std::move(conn));
            }
            servers_[key] = std::move(group);
            if(strategy_ == LoadBalanceStrategy::RoundRobin)
                roundRobinIdex_[key].store(0,std::memory_order_release);
        }
    }

    //4.更新现有端点快照
    currentEndpoints_ = epVec;
}

//----------私有成员函数----------

std::string ConnectionPool::makeServerKey(const Endpoint& ep)
{
    return ep.host + ":" + std::to_string(ep.port);
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

//服务发现版
ConnectionPool::Borrowed ConnectionPool::acquireRoundRobin(const ServerKey& key)
{
    std::unique_lock<std::mutex> lock(mutex_);
    //1.先获取Serverkey对应的连接容器
    auto& connVec = servers_[key].connections;
    size_t n = connVec.size();
    size_t start = roundRobinIdex_[key].fetch_add(1,std::memory_order_relaxed) % n;

    //遍历容器找到第一个健康且Idle的连接
    for(size_t i=0; i<n; ++i)
    {
        size_t index = (start+i) % n;
        auto& conn = connVec[index];

        if(conn->isHealthy() && conn->state() == ConnState::IDLE) 
        {
            conn->setState(ConnState::BUSY);
            return Borrowed(conn);
        }
    }

    throw RpcConnectionException("No Healthy Connection avaiable!");
}

ConnectionPool::Borrowed ConnectionPool::acquireLeastConnection(const ServerKey& key)
{
    std::unique_lock<std::mutex> lock(mutex_);

    ConnPtr bestConnPtr = nullptr;
    size_t min = std::numeric_limits<size_t>::max();

    auto& group = servers_[key];

    for(auto& conn : group.connections)
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
        throw RpcConnectionException("No Healthy Connection avaiable In Serveice: !");

    bestConnPtr->setState(ConnState::BUSY);

    return Borrowed(bestConnPtr);
}

ConnectionPool::Borrowed ConnectionPool::acquireRandom(const ServerKey& key)
{
    std::unique_lock<std::mutex> lock(mutex_);

    thread_local std::mt19937 rng(std::random_device{}());

    size_t n = servers_[key].connections.size();
    
    std::uniform_int_distribution<size_t> dist(0,n-1);

    for(size_t attemp = 0; attemp<10; attemp++)
    {
        size_t idex = dist(rng);

        auto& conn = servers_[key].connections[idex];
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