#include "../include/Service/EtcdDiscovery.h"

#include "Service/Endpoint.h"
#include "net/Log/Logger.h"

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <etcd/Value.hpp>
#include <etcd/Watcher.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <utility>
#include <vector>
#include <nlohmann/json.hpp>
// 辅助函数：将 etcd::Values 转为 vector<string>
namespace {
std::vector<std::string> rawValues(const etcd::Values& vals) {
	std::vector<std::string> result;
	result.reserve(vals.size());
	for (const auto& v : vals) {
		result.push_back(v.as_string());
	}
	return result;
}
}  // namespace

EtcdDiscovery::EtcdDiscovery(cmlib::EventLoop* loop,
                             const std::string& etcdUrl)
    : loop_(loop), etcdUrl_(etcdUrl) {
	client_ = std::make_unique<etcd::Client>(etcdUrl);
	LOG_INFO << "EtcdDiscovery: connected to " << etcdUrl;
}

EtcdDiscovery::~EtcdDiscovery() {
	if (!shutdown_) {
		shutdown();
	}
}

void EtcdDiscovery::subscribe(const std::string& serviceName,
                              EndpointListCallback callback) {
	if (shutdown_) {
		LOG_ERROR << "EtcdDiscovery::subscribe called after shutdown";
		return;
	}

	bool needFetch = false;

	{
		std::lock_guard<std::mutex> lock(mutex_);

		// 清理待取消订阅
		for (const auto& name : pendingUnsubscribe_) {
		subscriptions_.erase(name);
		lastEndpoints_.erase(name);
		}
		pendingUnsubscribe_.clear();

		auto& sub = subscriptions_[serviceName];
		sub.callbacks.push_back(std::move(callback));

		if (!sub.watching) {
		sub.watching = true;
		needFetch = true;
		}
	}

	if (needFetch) {
		// 1. 立即拉取当前端点列表
		fetchAndNotify(serviceName);
		// 2. 启动 watch 监听变更
		startWatch(serviceName);
	}
}

void EtcdDiscovery::unsubscribe(const std::string& serviceName) {
	// 延迟取消，避免与正在执行的 watch 回调竞争
	std::lock_guard<std::mutex> lock(mutex_);
	pendingUnsubscribe_.insert(serviceName);
}

void EtcdDiscovery::shutdown() {
	auto self = shared_from_this();
	loop_->runInLoop([self]() {
		std::lock_guard<std::mutex> lock(self->mutex_);
		if (self->shutdown_) return;
		self->shutdown_ = true;

		LOG_INFO << "EtcdDiscovery: shutting down, clearing "
				<< self->subscriptions_.size() << " subscriptions";
		self->subscriptions_.clear();
		self->lastEndpoints_.clear();
		self->pendingUnsubscribe_.clear();
	});
}

// ===== 私有方法 =====

std::string EtcdDiscovery::makeServicePrefix(const std::string& serviceName) {
  	return "/clearmoon/services/" + serviceName + "/";
}

void EtcdDiscovery::fetchAndNotify(const std::string& serviceName) {
	auto self = shared_from_this();
	auto svc = serviceName;
	loop_->runInLoop([self, svc]() {
		std::string prefix = makeServicePrefix(svc);

    try {
    	auto resp = self->client_->ls(prefix).get();

      	if (!resp.is_ok()) {
        LOG_ERROR << "EtcdDiscovery: ls failed for " << prefix << ": " << resp.error_message();
        return;
    	}

		// 解析端点列表
		auto endpoints =
			parseEndpoints(svc, resp.keys(), rawValues(resp.values()));

		// 检查是否与上次相同，避免无意义通知
		bool changed = false;
		{
			std::lock_guard<std::mutex> lock(self->mutex_);

			// 如果服务已被取消订阅，跳过
			if (self->pendingUnsubscribe_.count(svc) ||
				!self->subscriptions_.count(svc)) {
				return;
			}

			auto& last = self->lastEndpoints_[svc];
			if (last.size() != endpoints.size() || last != endpoints) {
				last = endpoints;
				changed = true;
			}
    	}

		if (changed) {
			// 获取回调副本并在锁外通知
			std::vector<EndpointListCallback> cbs;
			{
				std::lock_guard<std::mutex> lock(self->mutex_);
				auto it = self->subscriptions_.find(svc);
				if (it != self->subscriptions_.end()) {
					cbs = it->second.callbacks;
				}
			}
			for (auto& cb : cbs) {
				cb(endpoints);
			}
			LOG_INFO << "EtcdDiscovery: endpoint changed for " << svc << " (" << endpoints.size() << " endpoints)";
		}
    } 
	catch (const std::exception& e) {
			LOG_ERROR << "EtcdDiscovery::fetchAndNotify failed for " << svc << ": " << e.what();
		}
  	});
}

void EtcdDiscovery::startWatch(const std::string& serviceName) {
	auto self = shared_from_this();
	auto svc = serviceName;
	std::string prefix = makeServicePrefix(svc);

	try {
		// Watcher 拥有自己的内部线程，在独立线程中接收 watch 事件
		auto watcher = std::make_shared<etcd::Watcher>(
			*client_, prefix,
			[self, svc](etcd::Response resp) 
			{
				// watch 回调在 Watcher 的内部线程中执行
				if (!resp.is_ok()) {
					LOG_ERROR << "EtcdDiscovery: watch error for " << svc << ": "
							<< resp.error_message();
					return;
				}

				// 检查是否有实际的 key-value 变更事件
				bool hasChange = false;
				for (const auto& event : resp.events()) {
					if (event.has_kv()) {
					hasChange = true;
					break;
					}
				}

				if (hasChange) {
					// 端点发生变化，重新拉取全量列表
					self->fetchAndNotify(svc);
				}
			},
			true);  // recursive = true，监听前缀下所有 key 的变化

		// Watcher 运行在其内部线程中，通过 client_ 的析构间接终止
		// unsubscribe 通过 pendingUnsubscribe_ 标记让回调忽略该服务的通知
		std::unique_lock<std::mutex> lock(mutex_);
		auto it = subscriptions_.find(svc);
		if(it == subscriptions_.end())
		{
			// 服务已被取消订阅，watcher 会在析构时自动停止
            LOG_INFO << "EtcdDiscovery: service " << svc << " already unsubscribed, discarding watcher";
            return;
		}

		auto& sub = subscriptions_.find(serviceName)->second;
		sub.watcher = watcher;
		// (void)watcher;

	} catch (const std::exception& e) {
		LOG_ERROR << "EtcdDiscovery::startWatch failed for " << svc << ": "
				<< e.what();
	}
}

// ===== 静态工具函数 =====

std::vector<Endpoint>
EtcdDiscovery::parseEndpoints(const std::string& serviceName,
                              const std::vector<std::string>& keys,
                              const std::vector<std::string>& values) {
	std::vector<Endpoint> endpoints;
	endpoints.reserve(values.size());

	for (size_t i = 0; i < values.size(); ++i) {
		try {
		Endpoint ep = jsonToEndpoint(values[i]);
		ep.service = serviceName;
		endpoints.push_back(std::move(ep));
		} catch (const std::exception& e) {
		LOG_WARNING << "EtcdDiscovery: failed to parse endpoint at key "
					<< (i < keys.size() ? keys[i] : "<unknown>") << ": "
					<< e.what();
		}
	}

	return endpoints;
}

// 简单的手写 JSON 解析器，不引入额外的 JSON 库依赖
Endpoint EtcdDiscovery::jsonToEndpoint(const std::string& json) {
	nlohmann::json j = nlohmann::json::parse(json);
	Endpoint ep;
	ep.service = j.value("service","");
	ep.host = j.value("host","");
	ep.port = j.value("port",0);
	ep.weight = j.value("weight",1);
	if(j.contains("metadata"))
	{
		ep.metadata = j["metadata"].get<std::map<std::string, std::string>>();
	}
	return ep;
}