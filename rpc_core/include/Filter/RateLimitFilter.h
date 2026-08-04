#ifndef CLEARMOON_RPC_FILTER_RATELIMITFILTER_H
#define CLEARMOON_RPC_FILTER_RATELIMITFILTER_H

#include "RpcFilter.h"
#include "TokenBucket.h"
#include "net/Log/Logger.h"

#include <memory>

//流量控制过滤器
class RateLimitFilter : public RpcFilter
{
public:
    RateLimitFilter(std::shared_ptr<TokenBucket> tokenBucket) 
                  : tokenBucket_(tokenBucket)
    {
        LOG_DEBUG<<"Add RateLimitFilter";
    }

    bool before(const Header& header, RPC_Meta& meta, const std::string& body, RpcContext& ctx) override
    {
        if (!tokenBucket_->consume()) {
            ctx["block_reason"] = "rate_limited";
            return false;//拦截
        }
        return true;
    }

    void after(const Header& header, const RPC_Meta& meta, const RpcContext& ctx) override
    {}
private:
    std::shared_ptr<TokenBucket> tokenBucket_;
};

#endif