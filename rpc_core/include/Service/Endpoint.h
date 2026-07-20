#ifndef CLEARMOON_RPC_ENDPOINT_H
#define CLEARMOON_RPC_ENDPOINT_H

#include <cstdint>
#include <map>
#include <string>

struct Endpoint
{
    std::string service;
    std::string host;
    uint16_t port;
    uint16_t weight;

    std::map<std::string, std::string> metadata;
    
    std::string address() const { return host + ":" + std::to_string(port); }

    bool operator ==(const Endpoint& other) const
    {
        return service == other.service && host == other.host && port == other.port && weight == other.weight && metadata == other.metadata;
    }

    bool operator !=(const Endpoint& other) const{
        return !(*this == other);
    }
};

#endif //CLEARMOON_RPC_ENDPOINT_H