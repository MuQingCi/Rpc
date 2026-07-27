#ifndef CLEARMOON_RPC_FILECONFIGREGISTER_H
#define CLEARMOON_RPC_FILECONFIGREGISTER_H

#include "Service/Endpoint.h"
#include "Service/IsServiceDiscovery.h"
#include "Service/IsServiceRegister.h"
#include "net/TimerId.h"
#include "net/EventLoop.h"

#include "yaml-cpp/yaml.h"
#include "Registry.h"

// #include <atomic>
#include <map>
#include <mutex>
#include <set>
#include <memory>
#include <string>
#include <sys/types.h>
#include <vector>

namespace cmlib = clearmoon::net;

enum RegistryMode
{
    Server,
    Client,
    Both
};

class FileConfigRegister : public isServiceRegister, 
                           public isServiceDiscovery,
                           public std::enable_shared_from_this<FileConfigRegister>
{
public:
    FileConfigRegister(cmlib::EventLoop* loop,
                       const std::string& path, 
                       double pollInterval,
                       RegistryMode mode = RegistryMode::Both);
    ~FileConfigRegister() override;

    //服务端调用函数
    //注册服务--内部调用writeEndpointFile
    virtual void registerService(const std::string& serviceName, const Endpoint& endpoint) override;
    //取消注册的服务--内部调用removeEndpointFile
    virtual void deregisterService(const std::string& serviceName, const Endpoint& endpoint) override;

    virtual void subscribe(const std::string& serviceName, EndpointListCallback callback) override;
    virtual void unsubscribe(const std::string& serviceName) override;
private:
    //扫描整个配置目录
    void loadAllEndPoints();

    //解析单个yaml文件，返回其中的端点列表(yaml文件中可含一个/多个端点)
    std::vector<Endpoint> parseFile(const std::string& filePath);

    void writeEndpointFile(const std::string& service, const Endpoint& ep);
    void removeEndpointFile(const std::string& service, const Endpoint& ep);

    //生成文件名
    std::string makeFileName(const std::string& service, const Endpoint& ep);

    void clearPendingUnsubscribe();

    //配置文件目录
    std::string configDir_;
    cmlib::EventLoop* loop_;   //IO线程
    double pollIntervalSec_;   //轮询间隔---用于Client
    
    RegistryMode mode_;        //C/S标志位

    //ServiceName -> 端点列表
    std::map<std::string, std::vector<Endpoint>> endpoints_;

    //ServiceName -> 回调函数列表
    //EndpointListCallback = std::function<void(std::vector<Endpoint>&)>
    std::map<std::string, std::vector<EndpointListCallback>> callbacks_;

    std::set<std::string> pendingUnsubscribe_;

    cmlib::TimerId pollerTimerId_;

    mutable std::mutex mutex_;
};

#endif //CLEARMOON_RPC_FILECONFIGREGISTER_H