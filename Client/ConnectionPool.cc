#include "ConnectionPool.h"
#include "PooledConnection.h"
#include "Rpc_exceptions.h"
#include "Service/Endpoint.h"
#include "net/InetAddress.h"
#include "net/Log/Logger.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <random>
#include <set>
#include <utility>

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

//服务分发版(service暂不使用)
ConnectionPool::ConnectionPool(cmlib::EventLoop* loop,
                               LoadBalanceStrategy strategy,
                               size_t connPerServer)
                             : loop_(loop),
                               strategy_(strategy),
                               connPerServer_(connPerServer)
{
}

ConnectionPool::~ConnectionPool()
{
    stopHealthCheck();

    
    auto addToPending = [&](ConnPtr conn) 
    {
        // 避免重复设置回调（如果已经设置过，这里覆盖掉旧的也不会有问题，但最好只设置一次）
        conn->setOnDisconnected([this, conn] 
        {
            std::unique_lock<std::mutex> lock(mutex_);
            auto it = std::find(pendingClose_.begin(), pendingClose_.end(), conn);
            if (it != pendingClose_.end()) {
                pendingClose_.erase(it);
            }
            if (pendingClose_.empty()) {
                closeCv_.notify_all();
            }
        });
        pendingClose_.push_back(conn);
        conn->Disconnect();
    };

    {
        std::unique_lock<std::mutex> lock(mutex_);

        for(auto& ky : connections_)
        {
            ky->cancelAllPending();
            // ky->Disconnect();
            addToPending(ky);
        }

        connections_.clear();

        // 关闭动态池所有连接
        for (auto& [svc, entry] : servers_) {
            for (auto& group : entry.groups) 
            {
                for (auto& conn : group.connections) 
                    // conn->Disconnect();
                    addToPending(conn);
            }
    }
    servers_.clear();
    }
    

    std::unique_lock<std::mutex> lock(mutex_);
    closeCv_.wait(lock,[this]{return pendingClose_.empty();});
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

ConnectionPool::Borrowed ConnectionPool::acquire(const ServerName& name)
{
    std::unique_lock<std::mutex> lock(mutex_);

    auto it = servers_.find(name);
    if(it == servers_.end() || it->second.groups.empty())
        throw RpcConnectionException("Service not available: " + name);
    
    auto& entry = it->second;
    size_t groupIdx = entry.endpointRrIndex->fetch_add(1,std::memory_order_relaxed) % entry.groups.size();
    auto& group = entry.groups[groupIdx];

    switch (strategy_) {
        case LoadBalanceStrategy::RoundRobin :
            return acquireRoundRobin(group);
        case LoadBalanceStrategy::LeastConnection : 
            return acquireLeastConnection(group);
        case LoadBalanceStrategy::Random : 
            return acquireRandom(group);
        default:
            return acquireRoundRobin(group);
    }
}

// ========== 动态更新端点 ==========
//updateEndpoints 会立刻断开已移除端点的所有连接，正在处理中的请求将失败。
void ConnectionPool::updateEndpoints(const std::vector<Endpoint>& epVec)
{
    std::unique_lock<std::mutex> lock(mutex_);

    //1. 按服务名分组新端点
    std::map<std::string, std::vector<Endpoint>> svcMap;
    for(auto& ep : epVec) 
        svcMap[ep.service].push_back(ep);

    //2.移除已不存在的服务
    //servers_: ServerName->ServiceEntry
    for(auto it = servers_.begin(); it!= servers_.end();)
    {
        if(svcMap.find(it->first) == svcMap.end())
        {
            // 关闭该服务所有连接
            //ServiceEntry: std::vector<ServerConnGroup>, endpointRrIndex{0}m,,std::atomic<size_t> endpointRrIndex{0};
            for(auto& g : it->second.groups)
                for(auto& conn : g.connections)
                {
                    LOG_INFO<<"Disconnect a Connection from "<<g.endpoint.host<<":" << g.endpoint.port;
                    //-----------------
                    conn->setOnDisconnected([this,conn]{
                        std::unique_lock<std::mutex> lock(mutex_);
                        auto it = std::find(pendingClose_.begin(),pendingClose_.end(), conn);
                        if(it != pendingClose_.end()) pendingClose_.erase(it);
                        if(pendingClose_.empty()) closeCv_.notify_all();
                    });
                    pendingClose_.push_back(conn);
                    //-----------------------
                    conn->Disconnect(); //异步关闭
                }
                    
            it = servers_.erase(it);
        }else ++it;
    }

    //3.增量更新每个服务
    for(auto& [svcn,newEps] : svcMap)
    {
        if(ignoreServices_.count(svcn)) continue;
        auto it = servers_.find(svcn);
        //新服务
        if(it == servers_.end())
        {
            ServiceEntry svcEntry;
            for(auto& ep : newEps)
            {
                ServerConnGroup group;
                group.endpoint = ep;
                
                for(size_t i =0; i<connPerServer_; ++i)
                {
                    auto conn = std::make_shared<PooledConnection>(loop_,cmlib::InetAddress(ep.host,ep.port,false),i);
                    conn->Connect();
                    group.connections.push_back(std::move(conn));
                }
                svcEntry.groups.push_back(std::move(group));
            }
            servers_[svcn] = std::move(svcEntry);
        }
        else
        {
            // 已有服务：增量更新端点
            auto& entry = it->second;

            //构造旧的ServiceKey的集合
            std::set<std::string> oldKeys;
            for(auto& group : entry.groups)
            {
                oldKeys.insert(makeServerKey(group.endpoint));
            }

            //构造新的ServiceKey的集合
            std::set<std::string> newKeys;
            for(auto& ep : newEps)
            {
                newKeys.insert(makeServerKey(ep));
            }

            //移除已经不存在的端点
            /**
             * @brief 1.先遍历it对应ServiceEntry中的groups
                      2.在每次遍历中根据groups中的endpoint生成对应ServiceKey并在newKey中查找
                      3.若找不到则说明端点已经被移除
             * 
             */
            auto& groups = it->second.groups;
            for(size_t i=0;i<groups.size();)
            {
                auto keyName = makeServerKey(groups[i].endpoint);

                if(newKeys.find(keyName) == newKeys.end())
                {
                    //移除则断开连接
                    for(auto& conn : groups[i].connections)
                    {
                        conn->setOnDisconnected([this,conn]{
                            std::unique_lock<std::mutex> lock(mutex_);
                            auto it = std::find(pendingClose_.begin(),pendingClose_.end(),conn);
                            if(it!=pendingClose_.end()) pendingClose_.erase(it);
                            if(pendingClose_.empty()) closeCv_.notify_all();
                        });
                        pendingClose_.push_back(conn);
                        conn->Disconnect();
                    }
                        
                    groups.erase(groups.begin() + i);
                }else {
                    ++i;
                }
            }
            //添加新端点
            for(auto& ep : newEps)
            {
                auto key = makeServerKey(ep);
                if(oldKeys.find(key) == oldKeys.end())
                {
                    ServerConnGroup group;
                    group.endpoint = ep;

                    for(size_t i=0; i<connPerServer_;++i)
                    {
                        auto conn = std::make_shared<PooledConnection>(loop_,cmlib::InetAddress(ep.host,ep.port,false),i);
                        conn->Connect();
                        group.connections.push_back(std::move(conn));
                    }
                    it->second.groups.push_back(std::move(group));
                }
            }
        }
    }

    //4.保存端点快照
    currentEndpoints_ = epVec;

    if(!healthCheckTimerId_.valid())
        startHealthCheck();
}

void ConnectionPool::updateServiceEndpoints(const std::string& serviceName, const std::vector<Endpoint>& epVec) {
    std::unique_lock<std::mutex> lock(mutex_);

    auto it = servers_.find(serviceName);
    if (epVec.empty()) {
        // 如果端点列表为空，移除整个服务
        if (it != servers_.end()) {
            for (auto& group : it->second.groups) {
                for (auto& conn : group.connections) {
                    conn->setOnDisconnected([this, conn] {
                        std::unique_lock<std::mutex> lock(mutex_);
                        auto it = std::find(pendingClose_.begin(), pendingClose_.end(), conn);
                        if (it != pendingClose_.end()) pendingClose_.erase(it);
                        if (pendingClose_.empty()) closeCv_.notify_all();
                    });
                    pendingClose_.push_back(conn);
                    conn->Disconnect();
                }
            }
            servers_.erase(it);
        }
        return;
    }

    // 构造新端点 key 集合
    std::set<std::string> newKeys;
    for (auto& ep : epVec) newKeys.insert(makeServerKey(ep));

    if (it == servers_.end()) {
        // 新服务：创建 ServiceEntry
        ServiceEntry entry;
        for (auto& ep : epVec) {
            ServerConnGroup group;
            group.endpoint = ep;
            for (size_t i = 0; i < connPerServer_; ++i) {
                auto conn = std::make_shared<PooledConnection>(loop_, cmlib::InetAddress(ep.host, ep.port, false), i);
                conn->Connect();
                group.connections.push_back(std::move(conn));
            }
            entry.groups.push_back(std::move(group));
        }
        servers_[serviceName] = std::move(entry);
    } else {
        // 已有服务：增量更新端点
        auto& entry = it->second;

        // 移除旧端点
        auto& groups = entry.groups;
        for (size_t i = 0; i < groups.size(); ) {
            auto key = makeServerKey(groups[i].endpoint);
            if (newKeys.find(key) == newKeys.end()) {
                for (auto& conn : groups[i].connections) {
                    conn->setOnDisconnected([this, conn] {
                        std::unique_lock<std::mutex> lock(mutex_);
                        auto it = std::find(pendingClose_.begin(), pendingClose_.end(), conn);
                        if (it != pendingClose_.end()) pendingClose_.erase(it);
                        if (pendingClose_.empty()) closeCv_.notify_all();
                    });
                    pendingClose_.push_back(conn);
                    conn->Disconnect();
                }
                groups.erase(groups.begin() + i);
            } else {
                ++i;
            }
        }

        // 添加新端点
        for (auto& ep : epVec) {
            auto key = makeServerKey(ep);
            bool exists = std::any_of(groups.begin(), groups.end(), [&](const ServerConnGroup& g) {
                return makeServerKey(g.endpoint) == key;
            });
            if (!exists) {
                ServerConnGroup group;
                group.endpoint = ep;
                for (size_t i = 0; i < connPerServer_; ++i) {
                    auto conn = std::make_shared<PooledConnection>(loop_, cmlib::InetAddress(ep.host, ep.port, false), i);
                    conn->Connect();
                    group.connections.push_back(std::move(conn));
                }
                groups.push_back(std::move(group));
            }
        }
    }
}

void ConnectionPool::removeService(const std::string serviceName)
{
    std::unique_lock<std::mutex> lock(mutex_);
    
    auto it = servers_.find(serviceName);
    if(it != servers_.end())
    {
        for(auto& g : it->second.groups)
            for(auto&conn : g.connections)
            {
                conn->setOnDisconnected([this, conn] {
                    std::unique_lock<std::mutex> lock(mutex_);
                    auto it = std::find(pendingClose_.begin(), pendingClose_.end(), conn);
                    if (it != pendingClose_.end()) {
                        pendingClose_.erase(it);
                    }
                    if (pendingClose_.empty()) {
                        closeCv_.notify_all();
                    }
                });
                pendingClose_.push_back(conn);
                conn->Disconnect();
            }
        servers_.erase(it);
    }
}

void ConnectionPool::ignoreService(const std::string& serviceName)
{
	std::unique_lock<std::mutex> lock(mutex_);
	ignoreServices_.insert(serviceName);
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
ConnectionPool::Borrowed ConnectionPool::acquireRoundRobin(ServerConnGroup& group)
{
    //1.先获取Serverkey对应的连接容器
    size_t n = group.connections.size();
    size_t start = group.connRrIndex->fetch_add(1,std::memory_order_relaxed) % n;

    //遍历容器找到第一个健康且Idle的连接
    for(size_t i=0; i<n; ++i)
    {
        size_t index = (start+i) % n;
        auto& conn = group.connections[index];

        if(conn->isHealthy() && conn->state() == ConnState::IDLE) 
        {
            conn->setState(ConnState::BUSY);
            return Borrowed(conn);
        }
    }

    throw RpcConnectionException("No Healthy Connection avaiable!");
}

ConnectionPool::Borrowed ConnectionPool::acquireLeastConnection(ServerConnGroup& group)
{
    ConnPtr bestConnPtr = nullptr;
    size_t minActive = std::numeric_limits<size_t>::max();

    for(auto& conn : group.connections)
    {
        if(!conn->isHealthy() || conn->state() == ConnState::BUSY) continue;
        size_t active = conn->activeRequest();
        if(active<minActive)
        {
            minActive = active;
            bestConnPtr = conn;
        }
    }
    if(!bestConnPtr) 
        throw RpcConnectionException("No Healthy Connection avaiable In Serveice: !");

    bestConnPtr->setState(ConnState::BUSY);

    return Borrowed(bestConnPtr);
}

ConnectionPool::Borrowed ConnectionPool::acquireRandom(ServerConnGroup& group)
{
    thread_local std::mt19937 rng(std::random_device{}());

    size_t n = group.connections.size();
    
    std::uniform_int_distribution<size_t> dist(0,n-1);

    for(size_t attemp = 0; attemp<10; attemp++)
    {
        size_t idex = dist(rng);

        auto& conn = group.connections[idex];
        if(conn->isHealthy() && conn->state() == ConnState::IDLE) 
        {
            conn->setState(ConnState::BUSY);
            return Borrowed(conn);
        }
    }
    throw RpcConnectionException("No Healthy Connection avaiable!");
}

//健康检查 isConnected() 与状态设置存在窗口：doHealthCheck 检测到 !isConnected()后设置 UNHEALTHY，但 onConnection 回调可能随后将状态恢复为 IDLE，导致重复 Connect()
void ConnectionPool::doHealthCheck()
{
    decltype(connections_) toReconnect;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto checkAndMark = [&](const ConnPtr& conn) {
            //检查是否处于连接状态
            if((conn->state() == ConnState::IDLE || conn->state() == ConnState::UNHEALTHY) && !conn->isConnected())
            {
                //断开则标记非健康
                conn->markUnHealthy();
                toReconnect.push_back(conn);
            }
        };

        //静态池
        for(auto& conn : connections_)
        {
            checkAndMark(conn);
        }

        //动态池
        for(auto& [svcn, entry] : servers_)
        {
            for(auto& group : entry.groups)
            {
                for (auto& conn : group.connections) 
                {
                    checkAndMark(conn);
                }
            }
        }
    }
    
    for(auto& conn : toReconnect)
    {
        loop_->runInLoop([conn]{ conn->Connect(); });
        LOG_INFO<< "ConnectionPool["<<conn->getIndex() << "]: Unhealthy, reconnecting...."; 
    }
}
