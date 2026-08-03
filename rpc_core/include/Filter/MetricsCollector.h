#ifndef CLEARMOON_RPC_FILTER_METRICSCOLLECTOR_H
#define CLEARMOON_RPC_FILTER_METRICSCOLLECTOR_H

// #include "absl/container/flat_hash_map.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <string>
#include <map>
#include <unordered_map>
#include <utility>
class MetricsCollector
{
public:
    static MetricsCollector& getInstance()
    {
        static MetricsCollector instacnce;
        return instacnce;
    }

    void inCounter(const std::string& name, uint64_t delta = 1)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        counter_[name].fetch_add(delta, std::memory_order_relaxed);
    }

    void observeLatency(const std::string&name, double seconds)
    {
        static constexpr std::array<double, 8> buckets = { 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0 };

        auto it = std::lower_bound(buckets.begin(), buckets.end(), seconds);
        size_t idx = std::distance(buckets.begin(), it);

        std::unique_lock<std::mutex> lock(mutex_);

        histogramBuckets_[name][idx].fetch_add(1,std::memory_order_relaxed);

        auto sumIt = histogramSum_.find(name);
        if(sumIt != histogramSum_.end())
        {
            auto expected = sumIt->second.first.load(std::memory_order_relaxed);
            auto desired =  expected + seconds;

            //CAS自旋更新
            while(!sumIt->second.first.compare_exchange_weak(expected,desired,std::memory_order_relaxed, std::memory_order_relaxed))
            {
                desired = expected + seconds;
            }
            // histogramSum_[name].first.fetch_add(seconds,std::memory_order_relaxed);
            sumIt->second.second.fetch_add(1,std::memory_order_relaxed);
        }else {
            auto& sumPair = histogramSum_[name];
            sumPair.first.store(seconds, std::memory_order_relaxed);
            sumPair.second.store(1, std::memory_order_relaxed);
        }
    }

    std::map<std::string, uint64_t> getCounters() const
    {
        std::map<std::string,uint64_t> result;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            for(auto& [k,v] : counter_)
                result[k] = v.load();
        }
        
        return result;

    }

private:
    MetricsCollector() = default;
    // std::map<std::string, std::atomic<uint64_t>> counter_;
    std::unordered_map<std::string, std::atomic<uint64_t>> counter_;

    //name->(0,1,....,8)共9个桶，分别存储延迟为(-∞,0.001),(0.001,0.005)....(1.0,5.0),(5.0,+∞)区间中的个数
    // std::map<std::string, std::array<std::atomic<uint64_t>, 9>> histogramBuckets_;
    std::unordered_map<std::string, std::array<std::atomic<uint64_t>, 9>> histogramBuckets_;

    //name->(LantencySum, numberSum)
    // std::map<std::string, std::pair<std::atomic<double>, std::atomic<uint64_t>>> histogramSum_;
    std::unordered_map<std::string, std::pair<std::atomic<double>, std::atomic<uint64_t>>> histogramSum_;

    mutable std::mutex mutex_;
};

#endif