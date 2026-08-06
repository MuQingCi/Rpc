#ifndef CLEARMOON_RPC_SERVERCONFIG_H
#define CLEARMOON_RPC_SERVERCONFIG_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

///=============================================================================
/// @brief 服务端运行配置（从 yaml 文件加载，替代硬编码）
///=============================================================================
struct ServerConfig
{
    // ---- 监听地址 ----
    std::string host  = "127.0.0.1";
    uint16_t    port  = 12345;
    size_t      threadPoolSize = 8;     ///< 业务线程池大小
    uint16_t    serviceWeight   = 1;    ///< 注册端点权重

    // ---- 注册中心 ----
    std::string registryType    = "etcd";   ///< etcd / file
    std::string etcdUrl         = "http://127.0.0.1:2379";
    int         ttl             = 10;       ///< Lease TTL（秒）
    int         keepaliveInterval = 3;      ///< 续约间隔（秒）
    std::string filePath        = "./config"; ///< file 模式配置目录
    double      pollInterval    = 5.0;      ///< file 模式轮询间隔（秒）

    // ---- 注册的服务名 ----
    std::vector<std::string> services;

    // ---- 过滤器 ----
    bool trace = true;
    bool metrics = true;
    double rateLimit = 100;                     ///< 令牌/秒
    size_t bucketCapacity = 200;                ///< 桶容量
    uint32_t breakerErrorRate = 60;             ///< 熔断失误率阈值(%)
    std::chrono::seconds breakerWindow{10};     ///< 熔断窗口更新间隔
};

/// 从 yaml 文件加载服务端配置
inline ServerConfig loadServerConfig(const std::string& path)
{
    ServerConfig cfg;
    YAML::Node root = YAML::LoadFile(path);
    const YAML::Node& server = root["server"];
    if (!server) return cfg;

    // 监听地址
    if (server["host"])              cfg.host            = server["host"].as<std::string>();
    if (server["port"])              cfg.port            = server["port"].as<uint16_t>();
    if (server["thread_pool_size"])  cfg.threadPoolSize  = server["thread_pool_size"].as<size_t>();
    if (server["service_weight"])    cfg.serviceWeight   = server["service_weight"].as<uint16_t>();

    // 注册中心
    if (const YAML::Node& reg = server["registry"])
    {
        if (reg["type"])               cfg.registryType       = reg["type"].as<std::string>();
        if (reg["etcd_url"])           cfg.etcdUrl            = reg["etcd_url"].as<std::string>();
        if (reg["ttl"])                cfg.ttl                = reg["ttl"].as<int>();
        if (reg["keepalive_interval"]) cfg.keepaliveInterval  = reg["keepalive_interval"].as<int>();
        if (reg["file_path"])          cfg.filePath           = reg["file_path"].as<std::string>();
        if (reg["poll_interval"])      cfg.pollInterval       = reg["poll_interval"].as<double>();
    }

    // 注册的服务名
    if (const YAML::Node& svcs = server["services"])
    {
        for (const auto& s : svcs)
            cfg.services.push_back(s.as<std::string>());
    }

    // 过滤器
    if (const YAML::Node& filters = root["filters"])
    {
        if (filters["trace"])   cfg.trace   = filters["trace"].as<bool>();
        if (filters["metrics"]) cfg.metrics = filters["metrics"].as<bool>();

        if (const YAML::Node& rl = filters["rate_limit"])
        {
            if (rl["rate"])     cfg.rateLimit      = rl["rate"].as<double>();
            if (rl["capacity"]) cfg.bucketCapacity = rl["capacity"].as<size_t>();
        }
        if (const YAML::Node& cb = filters["circuit_breaker"])
        {
            if (cb["error_rate"])     cfg.breakerErrorRate = cb["error_rate"].as<uint32_t>();
            if (cb["window_seconds"]) cfg.breakerWindow    = std::chrono::seconds(cb["window_seconds"].as<int>());
        }
    }
    return cfg;
}

#endif // CLEARMOON_RPC_SERVERCONFIG_H
