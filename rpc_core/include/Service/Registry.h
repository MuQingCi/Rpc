#ifndef CLEARMOON_RPC_REGISTRY_H
#define CLEARMOON_RPC_REGISTRY_H

#include "./Endpoint.h"

#include <functional>
#include <string>
#include <vector>

class Registry
{
public: 
    virtual ~Registry() = default;

    virtual void registerService(const std::string& serviceName, const EndPoint& endpoint) = 0;
    virtual void deregisterService(const std::string& serviceName, const EndPoint& endpoint) = 0;

    virtual void subscribe(const std::string& serviceName, std::function<void(std::vector<EndPoint>)> callback) = 0;
    virtual void unsubscribe(const std::string& serviceName) = 0;
};

#endif //CLEARMOON_RPC_REGISTRY_H