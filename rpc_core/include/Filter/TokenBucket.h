#ifndef CLEARMOON_RPC_FILTER_TOKENBUCKET_H
#define CLEARMOON_RPC_FILTER_TOKENBUCKET_H

#include "net/Log/Logger.h"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <mutex>


//令牌桶
class TokenBucket
{
public:
    TokenBucket(double rate, size_t capacity) : rate_(rate), capacity_(capacity), token_(capacity),last_(std::chrono::steady_clock::now()) 
    {
        LOG_INFO<<"Create a TokenBucket, rate: " << rate << ", capacity:" << capacity;
    }

    bool consume(){
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration<double>(now - last_).count();

        std::unique_lock<std::mutex> lock(mutex_);
        token_ = std::min(static_cast<double>(capacity_), token_ + elapsed * rate_);
        if(token_ >= 1.0)
        {
            --token_;
            return true;
        }
        return false;
    }

private:
    double rate_;       //生成令牌的速率(令牌/秒)
    size_t capacity_;   //令牌桶的最大容量
    double token_;  //当前桶中的令牌数

    std::chrono::steady_clock::time_point last_;//上次更新令牌桶的时间

    std::mutex mutex_;
};

#endif