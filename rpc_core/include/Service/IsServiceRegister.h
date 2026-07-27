#ifndef CLEARMOON_RPC_ISSERVICEREGISTER_H
#define CLEARMOON_RPC_ISSERVICEREGISTER_H

#include "Service/Endpoint.h"
#include <string>
class isServiceRegister
{
public:
    virtual ~isServiceRegister() = default;

    virtual void registerService(const std::string& serviceName, const Endpoint& ep) = 0;
    virtual void deregisterService(const std::string& serviceName, const Endpoint& ep) = 0;

    virtual void shutdown() = 0;
};

#endif // CLEARMOON_RPC_ISSERVICEREGISTER_H