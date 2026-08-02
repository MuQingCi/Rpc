#ifndef CLEARMOON_RPC_FILTER_RPCFILTERCHAIN_H
#define CLEARMOON_RPC_FILTER_RPCFILTERCHAIN_H

#include "./RpcFilter.h"
#include <memory>
#include <vector>

//过滤器链
class RpcFilterChain
{
public:
    void addFilter(std::shared_ptr<RpcFilter> filter);
    void removeFilter(std::shared_ptr<RpcFilter> filter);

    //顺序执行所有过滤器
    bool executeBefore(const Header& header, RPC_Meta& meta, const std::string& body, RpcContext& ctx)
    {
        for(auto& f : filters_)
            if(!f->before(header, meta, body, ctx))
                return false;
        return true;
    }

    //逆序执行所有过滤器
    void executeAfter(const Header& header, const RPC_Meta& meta, const RpcContext& ctx)
    {
        for(auto it = filters_.rbegin(); it!= filters_.rend(); ++it)
        {
            (*it)->after(header, meta, ctx);
        }
    }

private:
    std::vector<std::shared_ptr<RpcFilter>> filters_;
};

#endif //CLEARMOON_RPC_RPCFILTERCHAIN_H