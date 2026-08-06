#ifndef CLEARMOON_RPC_CLIENTCONFIG_H
#define CLEARMOON_RPC_CLIENTCONFIG_H

#include "ConnectionPool.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

///=============================================================================
/// @brief 客户端运行配置（从 yaml 文件加载，替代硬编码）
///=============================================================================
struct ClientConfig
{
    // ---- 注册中心（服务发现） ----
    std::string registryType = "etcd";          ///< etcd / file
    std::string etcdUrl      = "http://127.0.0.1:2379";
    std::string configFileDir = "./config";     ///< file 模式配置目录
    double      pollInterval  = 1.0;            ///< file 模式轮询间隔（秒）

    // ---- 连接池 ----
    size_t               connPerServer       = 4;                    ///< 每个端点连接数
    LoadBalanceStrategy  strategy            = LoadBalanceStrategy::RoundRobin;
    std::chrono::seconds healthCheckInterval{5};                     ///< 健康检查间隔

    // ---- 订阅的服务 ----
    std::vector<std::string> subscribeServices;

    // ---- RPC 调用默认超时 ----
    std::chrono::milliseconds callTimeout{5000};

    // ---- 过滤器 ----
    bool trace = true;
    bool metrics = true;
    double rateLimit = 10;                          ///< 令牌/秒
    size_t bucketCapacity = 200;                    ///< 桶容量
    uint32_t breakerErrorRate = 50;                 ///< 熔断失误率阈值(%)
    std::chrono::seconds breakerWindow{10};         ///< 熔断窗口更新间隔
};

/// 字符串 -> 负载均衡策略
inline LoadBalanceStrategy parseLoadBalanceStrategy(const std::string& s)
{
    if (s == "least_connection") return LoadBalanceStrategy::LeastConnection;
    if (s == "random")           return LoadBalanceStrategy::Random;
    return LoadBalanceStrategy::RoundRobin;
}

/// 从 yaml 文件加载客户端配置
inline ClientConfig loadClientConfig(const std::string& path)
{
    ClientConfig cfg;
    YAML::Node root = YAML::LoadFile(path);
    const YAML::Node& client = root["client"];
    if (!client) return cfg;

    // 注册中心
    if (const YAML::Node& reg = client["registry"])
    {
        if (reg["type"])         cfg.registryType   = reg["type"].as<std::string>();
        if (reg["etcd_url"])     cfg.etcdUrl        = reg["etcd_url"].as<std::string>();
        if (reg["file_path"])    cfg.configFileDir  = reg["file_path"].as<std::string>();
        if (reg["poll_interval"]) cfg.pollInterval  = reg["poll_interval"].as<double>();
    }
    // 连接池
    if (client["conn_per_server"])       cfg.connPerServer       = client["conn_per_server"].as<size_t>();
    if (client["load_balance"])          cfg.strategy            = parseLoadBalanceStrategy(client["load_balance"].as<std::string>());
    if (client["health_check_interval"]) cfg.healthCheckInterval = std::chrono::seconds(client["health_check_interval"].as<int>());
    if (client["call_timeout_ms"])       cfg.callTimeout         = std::chrono::milliseconds(client["call_timeout_ms"].as<int64_t>());

    // 订阅的服务
    if (const YAML::Node& subs = client["subscribe"])
    {
        for (const auto& s : subs)
            cfg.subscribeServices.push_back(s.as<std::string>());
    }

    // 过滤器
    if (const YAML::Node& filters = root["filters"])
    {
        if (filters["trace"])   cfg.trace   = filters["trace"].as<bool>();
        if (filters["metrics"]) cfg.metrics = filters["metrics"].as<bool>();

        if (const YAML::Node& rl = filters["rate_limit"])
        {
            if (rl["rate"])     cfg.rateLimit       = rl["rate"].as<double>();
            if (rl["capacity"]) cfg.bucketCapacity  = rl["capacity"].as<size_t>();
        }
        if (const YAML::Node& cb = filters["circuit_breaker"])
        {
            if (cb["error_rate"])     cfg.breakerErrorRate = cb["error_rate"].as<uint32_t>();
            if (cb["window_seconds"]) cfg.breakerWindow    = std::chrono::seconds(cb["window_seconds"].as<int>());
        }
    }
    return cfg;
}

#endif // CLEARMOON_RPC_CLIENTCONFIG_H
