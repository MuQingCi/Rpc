#ifndef CLEARMOON_RPC_CLIENT_H
#define CLEARMOON_RPC_CLIENT_H

#include "ConnectionPool.h"
#include "Service/IsServiceDiscovery.h"
#include "Service/ServiceDiscoverer.h"
#include "Task.h"
#include "Message.pb.h"

#include "net/EventLoop.h"
#include "net/InetAddress.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>

namespace cmlib =  clearmoon::net;

///=============================================================================
/// @brief RPC客户端重连策略配置
///=============================================================================
// struct ReconnectConfig
// {
//     uint32_t maxRetries    = 5;           ///< 最大重试次数（0 表示不重连）
//     uint32_t baseBackoffMs = 500;         ///< 初始退避时间（毫秒）
//     uint32_t maxBackoffMs  = 30 * 1000;   ///< 最大退避时间（30秒）
//     double   jitterFactor  = 0.2;         ///< 随机抖动因子（0~0.5 之间）
// };

#include "Rpc_exceptions.h"

///=============================================================================
/// @class RPCClient
/// @brief 支持超时控制与自动重连的 RPC 客户端
///=============================================================================
class RPCClient : public std::enable_shared_from_this<RPCClient>
{
public:
    RPCClient(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr);

    RPCClient(cmlib::EventLoop* loop, size_t connPerServer,std::shared_ptr<isServiceDiscovery> discovery);

    ~RPCClient()
    {
        if(dynamic_ && discovery_)
        {
            decltype(subscribedServices_) copy;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                copy.swap(subscribedServices_);
            }

            for(const auto& svc : copy)
            {
                discovery_->unsubscribe(svc);
                connPool_->removeService(svc);
            }   
        }
        discovery_->shutdown();
        connPool_->stopHealthCheck();
    }

    void subscribe(const std::string& serviceName);
    void unsubscribe(const std::string& serviceName);

    //异步调用 --c++20的协程版本
    template<typename Request, typename Response>
    Task<Response> CallAsync(Request& req, std::chrono::milliseconds timeout, uint32_t method_id);

    //异步调用 --c++20的协程版本--动态服务版
    template<typename Request, typename Response>
    Task<Response> CallAsync(const std::string& serviceName,
                             Request& req, 
                             std::chrono::milliseconds timeout, 
                             uint32_t method_id);
private:
    cmlib::EventLoop*       loop_;            // 缓存 EventLoop 指针

    //连接池
    std::unique_ptr<ConnectionPool> connPool_;

    //动态服务
    std::shared_ptr<isServiceDiscovery> discovery_;
    std::set<std::string> subscribedServices_;
    bool dynamic_;

    mutable std::mutex mutex_;
};

//=============================================================================
// 内联 / 模板函数实现
//=============================================================================
// template<typename Request, typename Response>
// Response RPCClient::Call(Request& req)
// {
//     // 默认超时 5 秒
//     return Call<Request, Response>(req, std::chrono::seconds(5));
// }

// template<typename Request, typename Response>
// Response RPCClient::Call(Request& req, std::chrono::milliseconds timeout)
// {
//     // 检查连接状态
//     if (!connected())
//     {
//         throw RpcConnectionException("not connected to server");
//     }

//     std::promise<Response> prom;
//     std::future<Response>  fut = prom.get_future();

//     uint64_t seq = registerPending<Response>(std::move(prom), timeout);

//     // 编码并发送请求
//     RPC_Meta meta{};
//     meta.seq       = seq;
//     meta.method_id = getMethodId<Request>();
//     meta.timeout   = static_cast<uint32_t>(timeout.count());
//     meta.err_code  = 0;

//     Buffer sendBuff;
//     encode(&sendBuff, 0, 1, meta, req);

//     try {
//         conn_->send(&sendBuff);
//     } catch (...) {
//         removePending(seq);
//         throw RpcConnectionException("send faild");
//     }
    

//     // 等待结果或超时
//     auto status = fut.wait_for(timeout);
//     if (status == std::future_status::timeout)
//     {
//         // 超时：清理 pending 记录，定时器回调会在 onTimeout 中处理
//         removePending(seq);
//         throw RpcTimeoutException(seq, static_cast<uint32_t>(timeout.count()));
//     }

//     // 如果 promise 被设置了异常（反序列化失败），get() 会重新抛出
//     try {
//         return fut.get();
//     } catch (std::exception& e) {
//         throw RpcTimeoutException(seq, static_cast<uint32_t>(timeout.count()));
//     }
// }

// template<typename Response>
// uint64_t RPCClient::registerPending(std::promise<Response>&& prom,
//                                     std::chrono::milliseconds timeout)
// {
//     // 用 shared_ptr 包装 promise，使得 lambda 可复制（std::function 要求）
//     auto sp = std::make_shared<std::promise<Response>>(std::move(prom));

//     // 包装为 std::function 兼容的签名
//     auto callback = [sp](const std::string& body) mutable
//     {
//         if (body.empty())
//         {
//             // 连接断开 / 取消时传入空 body → 设异常唤醒等待线程
//             sp->set_exception(
//                 std::make_exception_ptr(std::runtime_error("RPC connection closed / cancelled")));
//             return;
//         }

//         Response res;
//         if (res.ParseFromString(body))
//         {
//             sp->set_value(std::move(res));
//         }
//         else
//         {
//             sp->set_exception(
//                 std::make_exception_ptr(std::runtime_error("RPC response parse failed")));
//         }
//     };

//     return registerPendingRaw(std::move(callback), timeout);
// }

template <typename Request, typename Response>
Task<Response> RPCClient::CallAsync(Request& req, 
                                    std::chrono::milliseconds timeout, 
                                    uint32_t method_id)
{
    auto conn = connPool_->acquire();
    uint64_t seq = 0;
    auto awaiter = std::make_shared<RpcAwaiter<Response>>(conn->getLoop(), seq, timeout);

    conn->sendRequest(req, awaiter, timeout, method_id);

    co_return co_await *awaiter;
}

//异步调用 --c++20的协程版本--动态服务版
template<typename Request, typename Response>
Task<Response> RPCClient::CallAsync(const std::string& serviceName,
                            Request& req, 
                            std::chrono::milliseconds timeout, 
                            uint32_t method_id)
{
    auto conn = connPool_->acquire(serviceName);
    uint64_t seq = 0;
    auto awaiter = std::make_shared<RpcAwaiter<Response>>(conn->getLoop(), seq, timeout);

    conn->sendRequest(req, awaiter, timeout, method_id);

    co_return co_await *awaiter;
}

#endif // CLEARMOON_RPC_CLIENT_H