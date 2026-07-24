#include "Service/ServiceDiscoverer.h"
#include <cassert>
#include <utility>

ServiceDiscoverer::ServiceDiscoverer(cmlib::EventLoop* loop, 
                                     std::string& serviceName, 
                                     std::shared_ptr<isServiceDiscovery> discover) 
                                    : loop_(loop),
                                      serviceName_(serviceName),
                                      discovery_(std::move(discover)) 
{
}

ServiceDiscoverer::~ServiceDiscoverer()
{
    stop();
}

void ServiceDiscoverer::start()
{
    //确保在IO线程
    loop_->assertInLoopThread();

    if(started_) return;
    started_ = true;

    auto self = shared_from_this();

    discovery_->subscribe(serviceName_, [self](const std::vector<Endpoint>& epVec) { self->onEndpointListChanged(epVec); });
}

void ServiceDiscoverer::stop()
{
    loop_->assertInLoopThread();
    if(!started_) return;
    started_ = false;
    callback_ = nullptr;

    discovery_->unsubscribe(serviceName_);
}

void ServiceDiscoverer::onEndpointListChanged(const std::vector<Endpoint>& epVec)
{
    //如果运行该回调时，其他线程执行了stop则跳过
    if(!started_ || !callback_) return;

    //上一行已经确保callback为非空指针了
    auto cb = callback_;
    cb(epVec);
}