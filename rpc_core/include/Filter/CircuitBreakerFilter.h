#ifndef CLEARMOON_RPC_FILTER_CIRCUITBREAKERFILTER_H
#define CLEARMOON_RPC_FILTER_CIRCUITBREAKERFILTER_H

#include "RpcFilter.h"
#include "../ToolFunc.h"
#include "net/Log/Logger.h"

#include <chrono>
#include <cstddef>
#include <mutex>

//熔断过滤器
class CircuitBreakerFilter : public RpcFilter
{
public:
    CircuitBreakerFilter(size_t threshold, std::chrono::seconds window)
                        :threshold_(threshold),
                         total_(0),
                         failures_(0),
                         window_(window),
                         lastReset_(std::chrono::steady_clock::now())
    {
        LOG_INFO<<"Add CircuitBreakFilter, threshold: " << threshold << ",window: " << window.count();
    }

    bool before(const Header& header, const RPC_Meta& meta, const std::string& body, RpcContext& ctx) override
    {
        auto now = std::chrono::steady_clock::now();
        if(now - lastReset_ > window_)
        {
            total_ = 0;
            failures_ = 0;
            lastReset_ = now;
        }
        if(total_ > 10 && failures_ * 100 / total_ > threshold_){
            //开启熔断
            ctx["block_reason"] = "circuitBreaker_open";
            return false; //拒绝通过
        }
        
        return true;
    }

    void after(const Header& header, const RPC_Meta& meta, const RpcContext& ctx) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ++total_;
        if(meta.err_code != 0)
            failures_++;
    };

private:
    size_t threshold_;
    size_t total_;
    size_t failures_;
    std::chrono::seconds window_;   //一次熔断窗口
    std::chrono::steady_clock::time_point lastReset_;

    std::mutex mutex_;
};

#endif