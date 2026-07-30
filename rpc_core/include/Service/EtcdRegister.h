#ifndef CLEARMOON_RPC_ETCDREGISTER_H
#define CLEARMOON_RPC_ETCDREGISTER_H

#include "Service/Endpoint.h"
#include "Service/IsServiceRegister.h"
#include "net/EventLoop.h"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace etcd {
class Client;
class KeepAlive;
}  // namespace etcd

namespace cmlib = clearmoon::net;

///=============================================================================
/// @brief 基于 etcd 的服务注册实现
///
/// 每个注册的服务端点会：
///   1. 创建一个 etcd Lease（TTL 可配，默认 10s）
///   2. 将端点信息以 JSON 格式写入 /clearmoon/services/{service}/{host}:{port}
///   3. 通过 KeepAlive 自动续约 Lease
///   4. deregister / shutdown 时撤销 Lease，key 自动被 etcd 清理
///
/// 进程异常退出时，Lease 在 TTL 后自动过期，etcd 自动清理 key。
///=============================================================================
class EtcdRegister : public isServiceRegister,
                     public std::enable_shared_from_this<EtcdRegister> {
public:
    /// @param loop        IO 线程（所有 etcd 操作在 IO 线程执行）
    /// @param etcdUrl     etcd 服务器地址，如 "http://127.0.0.1:2379"
    /// @param ttl         Lease TTL（秒），默认 10
    /// @param keepaliveInterval  续约间隔（秒），默认 3（一般取 TTL/3）
    EtcdRegister(cmlib::EventLoop* loop, const std::string& etcdUrl,
                int ttl = 10, int keepaliveInterval = 3);

    ~EtcdRegister() override;

    // ---- isServiceRegister 接口 ----
    void registerService(const std::string& serviceName,
                        const Endpoint& ep) override;
    void deregisterService(const std::string& serviceName,
                            const Endpoint& ep) override;
    void shutdown() override;

private:
    /// 生成 etcd key：/clearmoon/services/{serviceName}/{host}:{port}
    static std::string makeEtcdKey(const std::string& serviceName,
                                  const Endpoint& ep);
    /// 序列化 Endpoint 为 JSON
    static std::string endpointToJson(const Endpoint& ep);

    cmlib::EventLoop* loop_;
    std::string etcdUrl_;
    int ttl_;
    int keepaliveInterval_;

    // 每个注册的端点持有一个 KeepAlive 对象（自动续约 Lease）
    // key = "{serviceName}/{host}:{port}"
    struct LeaseEntry {
      std::shared_ptr<etcd::KeepAlive> keepAlive;
      int64_t leaseId = 0;
    };
    std::map<std::string, LeaseEntry> leases_;

    std::unique_ptr<etcd::Client> client_;

    bool shutdown_ = false;
    mutable std::mutex mutex_;
};

#endif  // CLEARMOON_RPC_ETCDREGISTER_H