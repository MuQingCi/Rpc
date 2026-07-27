#ifndef CLEARMOON_RPC_RPCFILTER_H
#define CLEARMOON_RPC_RPCFILTER_H

#include "net/Log/Logger.h"

#include <map>
#include <string>

using RpcContext = std::map<std::string, std::string>;

struct Header;
struct RPC_Meta;

class RpcFilter
{
public:
    ~RpcFilter() = default;
    virtual void before(const Header& header, const RPC_Meta& meta, const std::string& body, RpcContext& ctx) = 0;

    virtual void after(const Header& header, const RPC_Meta& meta, const std::string& body, const RpcContext& ctx) = 0;
};

#endif //CLEARMOON_RPC_RPCFILTER_H