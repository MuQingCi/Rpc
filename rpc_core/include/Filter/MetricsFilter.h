#ifndef CLEARMOON_RPC_FILTER_METRICSFILTER_H
#define CLEARMOON_RPC_FILTER_METRICSFILTER_H

#include "MetricsCollector.h"
#include "RpcFilter.h"
#include "../ToolFunc.h"

#include <chrono>
#include <memory>
#include <string>

//Metrics埋点过滤器
class MetricsFilter : public RpcFilter
{
public:
    bool before(const Header& header, RPC_Meta& meta, const std::string& body, RpcContext& ctx) override
    {
        auto now = std::chrono::steady_clock::now();

        ctx["metrics_start"] = std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());
        return true;
    }

    void after(const Header& header, const RPC_Meta& meta, const RpcContext& ctx) override
    {
        auto it = ctx.find("metrics_start");
        if(it == ctx.end()) return;

        auto start_us = std::stoull(it->second);
        auto now = std::chrono::steady_clock::now();

        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() - start_us;

        double second = elapsed / 1e6;

        auto& metrics = MetricsCollector::getInstance();

        metrics.inCounter("rpc_server_requests_total");

        metrics.observeLatency("rpc_server_request_duration_seconds", second);

        if(meta.err_code == 0)
            metrics.inCounter("rpc_server_requests_success_total");
        else
        {
            metrics.inCounter("rpc_server_requests_error_total");
            metrics.inCounter("rpc_server_requests_error_code" + std::to_string(meta.err_code));
        }

        //其他统计
    }
};

#endif