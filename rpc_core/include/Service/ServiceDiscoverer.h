#ifndef CLEARMOON_RPC_SERVICEDISCOVERER_H
#define CLEARMOON_RPC_SERVICEDISCOVERER_H

#include "IsServiceDiscovery.h"
#include "Service/Endpoint.h"
#include "net/EventLoop.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace cmlib = clearmoon::net;

class ServiceDiscoverer : std::enable_shared_from_this<ServiceDiscoverer>
{
public:
using EndpointChangeCallback = std::function<void(std::vector<Endpoint>)>;

    ServiceDiscoverer(cmlib::EventLoop* loop, std::string& serviceName, std::shared_ptr<isServiceDiscovery> discover);

    ~ServiceDiscoverer();

    void setEndpointChangeCallback(EndpointChangeCallback cb) { callback_ = std::move(cb); }

    void start();
    void stop();

private:
    //用于discovery_的第二个参数(即服务端点中的订阅回调，其中调用callback_)
    void onEndpointListChanged(const std::vector<Endpoint>& epVec);

    cmlib::EventLoop* loop_;
    std::string serviceName_;

    std::shared_ptr<isServiceDiscovery> discovery_;
    //一般被其他设置且被onEndpointListChanged调用
    EndpointChangeCallback callback_;

    bool started_;
};

#endif