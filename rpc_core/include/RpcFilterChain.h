#ifndef CLEARMOON_RPC_RPCFILTERCHAIN_H
#define CLEARMOON_RPC_RPCFILTERCHAIN_H

#include "./RpcFilter.h"
#include <memory>
#include <vector>

class RpcFilterChain
{
public:
    void addFilter(std::shared_ptr<RpcFilter> filter);
    void removeFilter(std::shared_ptr<RpcFilter> filter);

    //顺序执行所有过滤器
    void executeBefore(const Header& header, const RPC_Meta& meta, const std::string& body, RpcContext& ctx)
    {
        for(auto& f : filters_)
            f->before(header, meta, body, ctx);
    }

    //逆序执行所有过滤器
    void executeAfter(const Header& header, const RPC_Meta& meta, const std::string& body, const RpcContext& ctx)
    {
        for(auto it = filters_.rbegin(); it!= filters_.rend(); ++it)
        {
            (*it)->after(header, meta, body, ctx);
        }
    }

private:
    std::vector<std::shared_ptr<RpcFilter>> filters_;
};

#endif //CLEARMOON_RPC_RPCFILTERCHAIN_H