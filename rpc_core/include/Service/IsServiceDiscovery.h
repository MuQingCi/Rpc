#ifndef CLEARMOON_RPC_ISSERVICEDISCOVERY_H
#define CLEARMOON_RPC_ISSERVICEDISCOVERY_H

#include "Service/Endpoint.h"
#include <functional>
#include <string>
#include <vector>

class isServiceDiscovery
{
public:
using EndpointListCallback = std::function<void(const std::vector<Endpoint>&)>;

    virtual ~isServiceDiscovery() = default;

    virtual void subscribe(const std::string& serviceName, EndpointListCallback callback) = 0;
    virtual void unsubscribe(const std::string& serviceName) = 0;

    virtual void shutdown() = 0;
};

#endif //CLEARMOON_RPC_ISSERVICEDISCOVERY_H