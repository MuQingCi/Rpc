#ifndef CLEARMOON_RPC_ETCDDISCOVERY_H
#define CLEARMOON_RPC_ETCDDISCOVERY_H

#include "Service/Endpoint.h"
#include "Service/IsServiceDiscovery.h"
#include "net/EventLoop.h"

#include <etcd/Watcher.hpp>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace etcd {
class Client;
}  // namespace etcd

namespace cmlib = clearmoon::net;

///=============================================================================
/// @brief 基于 etcd 的服务发现实现
///
/// 对每个已订阅的服务：
///   1. 通过 ls(prefix) 获取当前端点列表，立即回调通知
///   2. 启动 watch(prefix, recursive) 监听变更
///   3. 变更发生时重新 ls 获取最新端点列表并回调
///
/// Key 前缀：/clearmoon/services/{serviceName}/
///=============================================================================
class EtcdDiscovery : public isServiceDiscovery,
                      public std::enable_shared_from_this<EtcdDiscovery> {
public:
    /// @param loop     IO 线程
    /// @param etcdUrl  etcd 服务器地址
    explicit EtcdDiscovery(cmlib::EventLoop* loop, const std::string& etcdUrl);

    ~EtcdDiscovery() override;

    // ---- isServiceDiscovery 接口 ----
    void subscribe(const std::string& serviceName,
                  EndpointListCallback callback) override;
    void unsubscribe(const std::string& serviceName) override;
    void shutdown() override;

private:
    /// 生成服务前缀 key：/clearmoon/services/{serviceName}/
    static std::string makeServicePrefix(const std::string& serviceName);

    /// 从 etcd ls 响应的 key-value 中解析 Endpoint 列表
    static std::vector<Endpoint> parseEndpoints(
        const std::string& serviceName,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& values);

    /// 从 JSON 字符串反序列化 Endpoint
    static Endpoint jsonToEndpoint(const std::string& json);

    /// 拉取服务端点并通知订阅者
    void fetchAndNotify(const std::string& serviceName);

    /// 启动对指定服务前缀的 watch 循环
    void startWatch(const std::string& serviceName);

    cmlib::EventLoop* loop_;
    std::string etcdUrl_;
    std::unique_ptr<etcd::Client> client_;

    /// 每个服务的订阅信息
    struct Subscription {
      std::vector<EndpointListCallback> callbacks;
      bool watching = false;  // 是否已启动 watch
      shared_ptr<etcd::Watcher> watcher;
    };
    std::map<std::string, Subscription> subscriptions_;

    // 待取消的订阅（延迟清理，类似 FileConfigRegister 的模式）
    std::set<std::string> pendingUnsubscribe_;

    /// 记录每个服务最后接收到的端点列表，用于 watch 触发的 diff
    std::map<std::string, std::vector<Endpoint>> lastEndpoints_;

    bool shutdown_ = false;
    mutable std::mutex mutex_;
  };

#endif  // CLEARMOON_RPC_ETCDDISCOVERY_H