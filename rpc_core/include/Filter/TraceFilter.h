#ifndef CLEARMOON_RPC_FILTER_TRACEFILTER_H
#define CLEARMOON_RPC_FILTER_TRACEFILTER_H

#include "RpcFilter.h"
#include "../ToolFunc.h"
#include "net/Log/Logger.h"

#include <chrono>
#include <cstdint>
#include <string>

//分布式ID追踪过滤器
class TraceFilter : public RpcFilter
{
public:
    TraceFilter() { LOG_INFO<< "Add TraceFilter"; }
    //记录对端元数据(meta)中的traceID以进行追踪,对端未设置traceID则本地生成一个
    bool before(const Header& header, const RPC_Meta& meta, const std::string& body, RpcContext& ctx) override
    {
        uint64_t tranceID = 0;
        if(meta.traceID == 0)
        {
            tranceID = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        }
        else
            tranceID = meta.traceID;

        ctx["trace_id"] = std::to_string(tranceID);
        
        return true;
    }

    //记录对应traceID的请求已处理完成
    void after(const Header& header, const RPC_Meta& meta, const RpcContext& ctx) override
    {
        auto it = ctx.find("trace_id");

        if(it != ctx.end())
            LOG_INFO<<"Request completed, trace_id = " << it->second <<", method id = " << meta.method_id << ", seq = " << meta.seq;
    }

};

#endif