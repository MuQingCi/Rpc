#include "Client.h"
#include "ConnectionPool.h"
#include "Service/Endpoint.h"

#include <memory>
#include <mutex>
#include <utility>
#include <vector>


using namespace clearmoon;
using namespace clearmoon::net;

RPCClient::RPCClient(EventLoop* loop, const InetAddress& serverAddr)
				   : loop_(loop),
				     connPool_(std::make_unique<ConnectionPool>(loop,serverAddr,4,LoadBalanceStrategy::RoundRobin)),
				     dynamic_(false) {}

RPCClient::RPCClient(cmlib::EventLoop* loop,
                     size_t connPerServer,
                     std::shared_ptr<isServiceDiscovery> discovery
                    )
                   : loop_(loop),
                     connPool_(std::make_unique<ConnectionPool>(loop,
                           LoadBalanceStrategy::RoundRobin,
                                connPerServer)),
                     discovery_(std::move(discovery)),
                     dynamic_(true)
{
}

void RPCClient::subscribe(const std::string& serviceName)
{
  	if(!dynamic_ || !discovery_) return;

	{
		std::unique_lock<std::mutex> lock(mutex_);
		if(subscribedServices_.count(serviceName)) return;
		subscribedServices_.insert(serviceName);
	}
	
  	auto self = shared_from_this();
	std::weak_ptr<RPCClient> weakSelf = self;
  	discovery_->subscribe(serviceName, [weakSelf,serviceName](const std::vector<Endpoint>& epVec){
		auto self = weakSelf.lock();
		if(self)
    		self->connPool_->updateServiceEndpoints(serviceName,epVec);
  	});
}

void RPCClient::unsubscribe(const std::string& serviceName)
{
	if(!dynamic_ || !discovery_) return;

	{
		std::unique_lock<std::mutex> lock(mutex_);
		if(!subscribedServices_.count(serviceName)) return;
		subscribedServices_.erase(serviceName);
	}

	discovery_->unsubscribe(serviceName);
	connPool_->ignoreService(serviceName);
	connPool_->removeService(serviceName);
}

// ============================================================================
// 连接回调
// ============================================================================
// void RPCClient::onConnection(const TcpConnectionPtr& conn)
// {
//     if (conn->connected())
//     {
//         LOG_INFO << "RPCClient connected to " << conn->getPeerAddr().toIpPort();
//         conn_ = conn;
//         retryCount_ = 0;   // 连接成功，重置重试计数
//         notifyConnectionState(true);
//     }
//     else
//     {
//         LOG_WARNING << "RPCClient disconnected from " << conn->getPeerAddr().toIpPort();
//         conn_.reset();
//         notifyConnectionState(false);

//         cancelAllPending();

//         // 触发重连（如果已启用）
//         if (reconnectEnabled_)
//         {
//             scheduleReconnect();
//         }
//     }
// }

// ============================================================================
// 消息回调
// ============================================================================
// void RPCClient::onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm)
// {
//     (void)conn;
//     (void)tm;
//     LOG_INFO << "RPCClient received message";

//     uint32_t minLen = sizeof(Header) + sizeof(RPC_Meta);

//     //处理消息
//     while (buff->readableBytes() >= minLen)
//     {
//         Header header{};
//         RPC_Meta meta{};
//         std::string body;

//         if (!decode(buff, header, meta, body))
//             break;

//         if (header.Flags == 3) // ACK 确认
//         {
//             if (conn_)
//                 conn_->ackReceived(meta.seq);
//             continue;
//         }

//         if (header.Flags == 1) // 响应
//         {
//             std::function<void(const std::string&)> callback;

//             // 1. 临界区：取出回调并标记 completed
//             {
//                 std::unique_lock<std::mutex> lock(mutex_);
//                 auto it = pending_.find(meta.seq);
//                 if (it != pending_.end())
//                 {
//                     // 标记完成，onTimeout 见到 completed 会跳过
//                     it->second.completed = true;

//                     // 取消超时定时器
//                     if (it->second.timerId.valid())
//                     {
//                         loop_->cancel(it->second.timerId);
//                     }

//                     callback = std::move(it->second.callback);
//                     pending_.erase(it);
//                 }
//             }

//             // 2. 临界区外执行回调（不持锁，不阻塞其他请求）
//             if (callback)
//             {
//                 try {
//                     callback(body);
//                 } catch (std::exception& e) {
//                     LOG_ERROR<< "RPCClient MessageHandleCallback error: " << e.what();
//                 }
//             }
//             else
//             {
//                 LOG_WARNING << "Received response for unknown/cancelled seq=" << meta.seq;
//             }
//         }
//     }
// }

// ============================================================================
// 注册待处理请求
// ============================================================================
// uint64_t RPCClient::registerPendingRaw(
//     std::function<void(const std::string&)> callback,
//     std::chrono::milliseconds timeout)
// {
//     std::unique_lock<std::mutex> lock(mutex_);

//     if(!connected()) 
//     {
//         throw RpcConnectionException("connection lost during registration");
//     }

//     uint64_t seq = nextSeq_++;

//     PendingContext ctx;
//     ctx.seq      = seq;
//     ctx.callback = std::move(callback);
//     ctx.completed = false;

//     // 在持有锁的情况下注册超时定时器
//     // 注意：runAfter 是线程安全的（通过 EventLoop::runInLoop 调度到 IO 线程）
//     ctx.timerId = loop_->runAfter(
//         timeout.count() / 1000.0,
//         [this, seq]() { onTimeout(seq); });

//     pending_[seq] = std::move(ctx);
//     return seq;
// }

// ============================================================================
// 超时处理
// ============================================================================
//
// 设计说明（双路径清理）：
//   1. Call() 中 fut.wait_for() 超时后立即调用 removePending() 并抛异常
//   2. 定时器回调 onTimeout() 作为辅助清理，处理 removePending() 未覆盖的情况
// 两条路径谁先获得 mutex_ 谁就完成清理，另一条发现 entry 已不存在则直接返回，
// 不会出现二次清理或 promise 悬空的问题。
//
// void RPCClient::onTimeout(uint64_t seq)
// {
//     std::function<void(const std::string&)> callback;

//     {
//         std::unique_lock<std::mutex> lock(mutex_);
//         auto it = pending_.find(seq);

//         // 响应已到达，或已被 removePending() 清理 → 跳过
//         if (it == pending_.end() || it->second.completed)
//             return;

//         // 取出 callback 并在锁外释放（避免持锁调用 promise）
//         callback = std::move(it->second.callback);
//         pending_.erase(it);
//     }

//     // ⚠️ 注意：此处不调用 callback()。
//     // 因为调用者在 Call() 中通过 fut.wait_for() 已检测到超时并抛出
//     // RpcTimeoutException，promise 未被 set_value/set_exception 会在
//     // 析构时自动触发 broken_promise，而 fut 已被放弃（从未 get），
//     // 所以不会产生悬空阻塞。onTimeout 仅负责清理 pending_ 表。
//     // (void)callback;

//     if(callback)
//     {
//         callback(std::string());
//     }
// }

// ============================================================================
// 移除 pending 请求（被 Call 超时路径调用）
// ============================================================================
// void RPCClient::removePending(uint64_t seq)
// {
//     std::function<void(const std::string&)> callback;

//     {
//         std::unique_lock<std::mutex> lock(mutex_);
//         auto it = pending_.find(seq);
//         if (it != pending_.end())
//         {
//             // 取消定时器
//             if (it->second.timerId.valid())
//             {
//                 loop_->cancel(it->second.timerId);
//             }
//             callback = std::move(it->second.callback);
//             pending_.erase(it);
//         }
//     }
// }

// ============================================================================
// 取消所有 pending 请求（析构 / stop / 连接失败 时调用）
// ============================================================================
// void RPCClient::cancelAllPending()
// {
//     decltype(pending_) pendingCopy;

//     {
//         std::unique_lock<std::mutex> lock(mutex_);

//         // 取消所有超时定时器
//         for (auto& kv : pending_)
//         {
//             if (kv.second.timerId.valid())
//             {
//                 loop_->cancel(kv.second.timerId);
//             }
//         }

//         // 将 pending_ 整表交换到局部副本，在锁外处理回调
//         pendingCopy.swap(pending_);
//     }

//     // 锁外执行所有回调：传入空字符串使 promise 反序列化失败，
//     // 从而 fut.get() 抛出异常，避免用户线程永久阻塞
//     for (auto& kv : pendingCopy)
//     {
//         if (kv.second.callback)
//         {
//             kv.second.callback(std::string());
//         }
//     }
// }

// ============================================================================
// 重连逻辑（指数退避 + 随机抖动）
// ============================================================================
// void RPCClient::scheduleReconnect()
// {
//     if (!reconnectEnabled_)
//         return;

//     if (reconnectConfig_.maxRetries > 0 &&
//         retryCount_ >= reconnectConfig_.maxRetries)
//     {
//         LOG_ERROR << "RPCClient max reconnection retries reached (" << retryCount_ << ")";
//         return;
//     }

//     // 计算退避时间：baseBackoffMs * 2^retryCount
//     uint32_t backoff = reconnectConfig_.baseBackoffMs *
//                        (1u << std::min(retryCount_, 10u)); // 限制指数增长
//     backoff = std::min(backoff, reconnectConfig_.maxBackoffMs);

//     // 添加随机抖动 [backoff * (1 - jitter), backoff * (1 + jitter)]
//     if (reconnectConfig_.jitterFactor > 0.0)
//     {
//         std::mt19937 rng(static_cast<unsigned>(
//             std::chrono::steady_clock::now().time_since_epoch().count()));
//         double jitterRange = backoff * reconnectConfig_.jitterFactor;
//         std::uniform_real_distribution<double> dist(-jitterRange, jitterRange);
//         backoff = static_cast<uint32_t>(backoff + dist(rng));
//     }

//     LOG_INFO << "RPCClient will reconnect in " << backoff << "ms (retry="
//              << retryCount_ << ")";

//     retryCount_++;

//     if(reconnectTimerId_.valid())
//     {
//         loop_->cancel(reconnectTimerId_);
//         reconnectTimerId_ = TimerId{};
//     }

//     reconnectTimerId_ = loop_->runAfter(backoff / 1000.0, [this]() { doReconnect(); });
// }

// void RPCClient::doReconnect()
// {
//     if (!reconnectEnabled_)
//         return;

//     LOG_INFO << "RPCClient attempting reconnect...";
//     tcpClient_.connect();
// }

// ============================================================================
// 连接状态通知
// ============================================================================
// void RPCClient::notifyConnectionState(bool isConnected)
// {
//     if (connectionStateCallback_)
//     {
//         try
//         {
//             connectionStateCallback_(isConnected);
//         }
//         catch (const std::exception& e)
//         {
//             LOG_ERROR << "RPCClient connectionStateCallback threw: " << e.what();
//         }
//     }
// }