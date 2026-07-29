#include "../include/Service/EtcdRegister.h"

#include "Service/Endpoint.h"
#include "net/Log/Logger.h"

#include <etcd/Client.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/Response.hpp>

#include <mutex>
#include <string>
#include <utility>

EtcdRegister::EtcdRegister(cmlib::EventLoop* loop, const std::string& etcdUrl,
                           int ttl, int keepaliveInterval)
    : loop_(loop),
      etcdUrl_(etcdUrl),
      ttl_(ttl),
      keepaliveInterval_(keepaliveInterval) {
  client_ = std::make_unique<etcd::Client>(etcdUrl);
  LOG_INFO << "EtcdRegister: connected to " << etcdUrl;
}

EtcdRegister::~EtcdRegister() {
  if (!shutdown_) {
    shutdown();
  }
}

void EtcdRegister::registerService(const std::string& serviceName,
                                   const Endpoint& ep) {
  if (shutdown_) {
    LOG_ERROR << "EtcdRegister::registerService called after shutdown";
    return;
  }

  auto self = shared_from_this();
  auto svc = serviceName;
  auto endpoint = ep;
  loop_->runInLoop([self, svc, endpoint]() {
    std::string key = makeEtcdKey(svc, endpoint);
    std::string value = endpointToJson(endpoint);

    // 检查是否已注册（去重）
    {
      std::lock_guard<std::mutex> lock(self->mutex_);
      if (self->leases_.count(key)) {
        LOG_WARNING << "EtcdRegister: service already registered: " << key;
        return;
      }
    }

    try {
      // 1. 创建 Lease + KeepAlive（library 自动续约）
      //TODO可使用异步
      auto keepAlive = self->client_->leasekeepalive(self->ttl_).get();
      int64_t leaseId = keepAlive->Lease();

      // 2. 将端点写入 etcd，绑定 Lease
      self->client_->put(key, value, leaseId).get();

      // 3. 记录 Lease 信息
      {
        std::lock_guard<std::mutex> lock(self->mutex_);
        LeaseEntry entry;
        entry.keepAlive = keepAlive;
        entry.leaseId = leaseId;
        self->leases_[key] = std::move(entry);
      }

      LOG_INFO << "EtcdRegister: registered " << key << " (leaseId=" << leaseId
               << ", ttl=" << self->ttl_ << "s)";
    } catch (const std::exception& e) {
      LOG_ERROR << "EtcdRegister::registerService failed for " << key << ": "
                << e.what();
    }
  });
}

void EtcdRegister::deregisterService(const std::string& serviceName,
                                     const Endpoint& ep) {
  if (shutdown_) return;

  auto self = shared_from_this();
  auto svc = serviceName;
  auto endpoint = ep;
  loop_->runInLoop([self, svc, endpoint]() {
    std::string key = makeEtcdKey(svc, endpoint);

    LeaseEntry entry;
    {
      std::lock_guard<std::mutex> lock(self->mutex_);
      auto it = self->leases_.find(key);
      if (it == self->leases_.end()) {
        LOG_WARNING << "EtcdRegister: service not registered: " << key;
        return;
      }
      entry = std::move(it->second);
      self->leases_.erase(it);
    }

    try {
      // 1. 从 etcd 删除 key
      self->client_->rm(key).get();

      // 2. 取消 KeepAlive（自动撤销 Lease）
      entry.keepAlive->Cancel();

      LOG_INFO << "EtcdRegister: deregistered " << key;
    } catch (const std::exception& e) {
      LOG_ERROR << "EtcdRegister::deregisterService failed for " << key << ": "
                << e.what();
    }
  });
}

void EtcdRegister::shutdown() {
  auto self = shared_from_this();
  loop_->runInLoop([self]() {
    std::lock_guard<std::mutex> lock(self->mutex_);
    if (self->shutdown_) return;
    self->shutdown_ = true;

    LOG_INFO << "EtcdRegister: shutting down, cancelling "
             << self->leases_.size() << " leases";

    for (auto& [key, entry] : self->leases_) {
      try {
        entry.keepAlive->Cancel();
        LOG_INFO << "EtcdRegister: cancelled lease for " << key;
      } catch (const std::exception& e) {
        LOG_ERROR << "EtcdRegister: failed to cancel lease for " << key << ": "
                  << e.what();
      }
    }
    self->leases_.clear();
  });
}

// ===== 静态工具函数 =====

std::string EtcdRegister::makeEtcdKey(const std::string& serviceName,
                                      const Endpoint& ep) {
  return "/clearmoon/services/" + serviceName + "/" + ep.host + ":" +
         std::to_string(ep.port);
}

std::string EtcdRegister::endpointToJson(const Endpoint& ep) {
  std::string json;
  json += "{\"host\":\"";
  json += ep.host;
  json += "\",\"port\":";
  json += std::to_string(ep.port);
  json += ",\"weight\":";
  json += std::to_string(ep.weight);
  json += ",\"metadata\":{";
  bool first = true;
  for (const auto& [k, v] : ep.metadata) {
    if (!first) json += ",";
    first = false;
    json += "\"" + k + "\":\"" + v + "\"";
  }
  json += "}}";
  return json;
}