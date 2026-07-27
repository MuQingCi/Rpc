#include "../include/Service/FileConfigRegister.h"
#include "Service/Endpoint.h"
#include "net/Log/Logger.h"
#include "net/TimerId.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>
#include <yaml-cpp/emitter.h>
#include <yaml-cpp/emittermanip.h>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>

namespace fs = std::filesystem;

FileConfigRegister::FileConfigRegister(cmlib::EventLoop* loop,
                                       const std::string& path, 
                                       double pollInterval,
                                       RegistryMode mode) 
                                       : loop_(loop),
                                         configDir_(path),
                                         pollIntervalSec_(pollInterval),
                                         mode_(mode)
{
    if(mode == RegistryMode::Server || mode == RegistryMode::Both)
    {
        std::error_code ec;
        fs::create_directories(path,ec);
        if(ec)
            LOG_ERROR<<"Failed to create config directory: "<<path << " - " << ec.message();
    }

    if(mode == RegistryMode::Client || mode == RegistryMode::Both)
    {
        loadAllEndPoints();
    }
}

FileConfigRegister::~FileConfigRegister()
{
    // 取消定时器需要在 IO 线程完成，但对象可能正在析构。
    // 使用 shared_from_this 延长生命周期，并投递到 IO 线程执行
    auto self = shared_from_this();

    loop_->runInLoop([self] {
        std::lock_guard<std::mutex> lock(self->mutex_);
        if (self->pollerTimerId_.valid()) {
            self->loop_->cancel(self->pollerTimerId_);
            self->pollerTimerId_ = cmlib::TimerId{};
        }
    });
}

void FileConfigRegister::registerService(const std::string& serviceName, const Endpoint& endpoint)
{
    auto self = shared_from_this();
    auto ep = endpoint;
    loop_->runInLoop([self,serviceName, ep]{
        self->writeEndpointFile(serviceName, ep);
    });
}

void FileConfigRegister::deregisterService(const std::string& serviceName, const Endpoint& endpoint)
{
    auto self = shared_from_this();
    auto ep = endpoint;
    loop_->runInLoop([self,serviceName, ep]{
        self->removeEndpointFile(serviceName, ep);
    });
}

void FileConfigRegister::subscribe(const std::string& serviceName, EndpointListCallback callback)
{
    if(mode_ == RegistryMode::Server)
    {
        LOG_ERROR << "subscribe() not allowed in Server mode";
        return;
    }

    std::vector<Endpoint> currentEndpoints;
    EndpointListCallback cbCopy;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 清理待取消的订阅
        for (const auto& name : pendingUnsubscribe_) {
            callbacks_.erase(name);
        }
        pendingUnsubscribe_.clear();

        callbacks_[serviceName].push_back(std::move(callback));
        cbCopy = callbacks_[serviceName].back();

        // 获取当前端点快照
        auto it = endpoints_.find(serviceName);
        if (it != endpoints_.end()) {
            currentEndpoints = it->second;
        }

        // 启动轮询定时器（如果尚未启动）
        if (!pollerTimerId_.valid()) {
            auto self = shared_from_this();
            pollerTimerId_ = loop_->runEvery(pollIntervalSec_, [self] {
                self->loadAllEndPoints();
            });
        }
    }

    cbCopy(currentEndpoints);
}

void FileConfigRegister::unsubscribe(const std::string& serviceName)
{
    //采用延迟取消消除竞争
    //真正的清理将在下一次 loadAllEndPoints 或 subscribe 中进行
    std::lock_guard<std::mutex> lock(mutex_);
    pendingUnsubscribe_.insert(serviceName);
}

void FileConfigRegister::loadAllEndPoints()
{
    std::map<std::string, std::vector<Endpoint>> newEndpoints;

    std::error_code ec;
   //判断路径所指是否存在且为目录
    if(!fs::exists(configDir_,ec) || !fs::is_directory(configDir_, ec))
        return;

    //遍历目录中的文件
    for(auto& entry : fs::directory_iterator(configDir_,ec))
    {
        if(ec) break;

        //若当前文件不为普通文件或者其后缀不为.yaml则跳过
        if(!entry.is_regular_file() ||entry.path().extension() != ".yaml") 
            continue;

        //解析yaml文件
        auto eps = parseFile(entry.path().string());
        for(auto& ep : eps)
        {
            newEndpoints[ep.service].push_back(std::move(ep));
        }
    }

    // 2. 持锁更新状态，并收集需要通知的服务
    std::vector<std::pair<std::string, std::vector<Endpoint>>> toNotify;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        clearPendingUnsubscribe();
        //比较新旧并通知所有已订阅的服务
        for(auto& [svc, cbList] : callbacks_)
        {
            auto oldIt = endpoints_.find(svc);
            auto newIt = newEndpoints.find(svc);
            bool changed = false;

            //服务已被移除
            if(oldIt != endpoints_.end() && newIt == newEndpoints.end())
            {
                changed = true;
            }
            //新增服务
            else if(oldIt == endpoints_.end() && newIt != newEndpoints.end())
            {
                changed = true;
            }
            //比较各个端点是否发生变化
            else if (oldIt != endpoints_.end() && newIt != newEndpoints.end()) 
            {
                //目前只判断大小和容器是否相等
                if(oldIt->second.size() != newIt->second.size() || oldIt->second != newIt->second)
                {
                    changed = true;
                }
            }

            //若发生改变则调用回调
            if(changed)
            {
                std::vector<Endpoint> notifyList;
                if(newIt != newEndpoints.end())
                    notifyList = newIt->second;
                toNotify.emplace_back(svc, std::move(notifyList));
            }
        }

        endpoints_ = std::move(newEndpoints);

        if (callbacks_.empty() && pollerTimerId_.valid()) {
            loop_->cancel(pollerTimerId_);
            pollerTimerId_ = cmlib::TimerId{};
        }
    }

    // 3. 在锁外通知所有变化的服务
    for (auto& [svc, list] : toNotify) {
        // 需要重新获取回调列表，因为通知时不能再持锁
        // 注意：这里需要再次加锁读取 callbacks_，但必须复制后解锁。
        std::vector<EndpointListCallback> cbs;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = callbacks_.find(svc);
            if (it != callbacks_.end()) {
                cbs = it->second;  // 复制整个 vector
            }
        }
        for (auto& cb : cbs) {
            cb(list);
        }
    }
}

/**
 * @brief 解析单个yaml文件,被loadAllEndPoints函数引用
 * 
 * @param filePath 
 * @return std::vector<EndPoint> ----其中存储的是单个yaml文件中的所有端点(1或n)
 */
std::vector<Endpoint> FileConfigRegister::parseFile(const std::string& filePath)
{
    std::vector<Endpoint> endpoints;
    try
    {
        YAML::Node node = YAML::LoadFile(filePath);
        //判断node是单个对象还是数组
        if(node.IsSequence()) //数组
        {
            for(const auto& item : node)
            {
                Endpoint ep;
                ep.service = item["service"].as<std::string>();
                ep.host = item["host"].as<std::string>();
                ep.port = item["port"].as<uint16_t>();
                ep.weight = item["weight"] ? item["weight"].as<uint16_t>() : 1;

                if(item["metadata"])
                {
                    ep.metadata = item["metadata"].as<std::map<std::string, std::string>>();
                }
                endpoints.push_back(std::move(ep));
            }
        }
        else if(node.IsMap())//单个对象
        {
            Endpoint ep;
            ep.service = node["service"].as<std::string>();
            ep.host = node["host"].as<std::string>();
            ep.port = node["port"].as<uint16_t>();
            ep.weight = node["weight"] ? node["weight"].as<uint16_t>() : 1;

            if(node["metadata"])
            {
                ep.metadata = node["metadata"].as<std::map<std::string, std::string>>();
            }
            endpoints.push_back(std::move(ep));
        }
        else 
        {
            LOG_WARNING<<"Invalid yaml struct in: " <<filePath;
        }
    }//catch
    //捕获yaml异常
    catch(const YAML::Exception& e)
    {
        LOG_ERROR<<"Parse YAML failed: " << filePath <<" - " << e.what();
    }
    //捕获其他异常
    catch(const std::exception& e)
    {
        LOG_ERROR<<"Unexpected error reading " << filePath << ":" <<e.what();
    }
    return endpoints;
}

// ===== 写端点文件（在 IO 线程执行） =====
void FileConfigRegister::writeEndpointFile(const std::string& service, const Endpoint& ep)
{
    // 1.文件 I/O（锁外）
    //获取文件名
    std::string fileName = makeFileName(service, ep);
    //拼接文件名路径
    fs::path filePath = fs::path(configDir_) / fileName;

    //实例化并填写yaml
    YAML::Emitter out;
    out<<YAML::BeginMap;
    out<<YAML::Key << "service" << YAML::Value << service;
    out<<YAML::Key << "host" << YAML::Value << ep.host;
    out<<YAML::Key << "port" << YAML::Value << ep.port;
    out<<YAML::Key << "weight" << YAML::Value << ep.weight;
    if(!ep.metadata.empty())
    {
        out<<YAML::Key << "metadata" << YAML::Value << ep.metadata;
    }
    out<<YAML::EndMap;

    {
        //将其写入文件
        std::ofstream fout(filePath);
        if(!fout)
        {
            LOG_ERROR<<"Cannot write endpoint file: " << filePath;
            return;
        }
        fout<<out.c_str();
        fout.close();
    }

    LOG_INFO<<"Registered endpoint: " << filePath;

    // 2. 更新内存状态（锁内）
    std::vector<Endpoint> notifyList;
    std::vector<EndpointListCallback> cbs;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 更新 endpoints_
        auto& vec = endpoints_[service];

        //查找该服务节点是否存在（去重检查）,存在就更新
        auto it = std::find_if(vec.begin(), vec.end(), [&ep](const Endpoint& e) { return e == ep; });
        if (it != vec.end()) {
            it->weight = ep.weight;
            it->metadata = ep.metadata;
        } else {
            vec.push_back(ep);
        }

        // 清理 pending 并获取回调
        clearPendingUnsubscribe();

        auto cbIt = callbacks_.find(service);
        if (cbIt != callbacks_.end()) {
            cbs = cbIt->second;
            notifyList = endpoints_[service];  // 副本
        }
    }

    // 3. 锁外通知
    for (auto& cb : cbs) {
        cb(notifyList);
    }
}

/**
 * @brief 用于移除整个yaml文件(此函数只使用于一个yaml只包含一个Endpoint的情况)
    实现思路:
    1.先删除对应yaml文件
    2.在互斥锁区中删除对应endpoints_中目标service对应的vector中的目标Endpoint(拷贝对应的端点数组和回调数组)
    3.在互斥锁区外通知所有订阅了这个service的订阅者
 * 
 * @param service 
 * @param ep 
 */
void FileConfigRegister::removeEndpointFile(const std::string& service, const Endpoint& ep)
{
    // 1. 文件 I/O（锁外）
    std::string fileName = makeFileName(service, ep);
    fs::path filePath = fs::path(configDir_) / fileName;
    std::error_code ec;
    fs::remove(filePath, ec);
    if (ec) {
        LOG_ERROR << "Failed to remove endpoint file: " << filePath << " - " << ec.message();
        return;
    }
    LOG_INFO << "Deregistered Endpoint file: " << filePath;

    // 2. 内存更新（锁内）
    std::vector<Endpoint> notifyList;
    std::vector<EndpointListCallback> cbs;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = endpoints_.find(service);
        if (it != endpoints_.end()) {
            auto& vec = it->second;
            vec.erase(std::remove_if(vec.begin(), vec.end(),
                                     [&ep](const Endpoint& e) { return e == ep; }),
                      vec.end());
            if (vec.empty()) endpoints_.erase(it);
        }

        // 清理 pending
        clearPendingUnsubscribe();

        auto cbIt = callbacks_.find(service);
        if (cbIt != callbacks_.end()) {
            cbs = cbIt->second;
            if (endpoints_.count(service)) notifyList = endpoints_[service];
        }
    }

    // 3. 锁外通知
    for (auto& cb : cbs) {
        cb(notifyList);
    }
}

std::string FileConfigRegister::makeFileName(const std::string& service, const Endpoint& ep)
{
    return service + "_" + ep.host + "_" + std::to_string(ep.port) + ".yaml";
}

void FileConfigRegister::clearPendingUnsubscribe()
{
    for(const auto&kv : pendingUnsubscribe_)
    {
        callbacks_.erase(kv);
    }
    pendingUnsubscribe_.clear();

    //此时如果无剩余回调则取消轮询定时器
    if(callbacks_.empty() && pollerTimerId_.valid())
    {
        loop_->cancel(pollerTimerId_);
        pollerTimerId_ = cmlib::TimerId{};
        // poolStarted_.store(false,std::memory_order_release);
    }
}