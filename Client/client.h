#ifndef CLEARMOON_RPC_CLIENT_H
#define CLEARMOON_RPC_CLIENT_H

#include "message.pb.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "net/TcpClient.h"
#include "net/TcpConnection.h"
#include "net/TimerId.h"
#include "toolFunc.h"

#include <chrono>
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

///=============================================================================
/// @brief RPC客户端重连策略配置
///=============================================================================
struct ReconnectConfig
{
    uint32_t maxRetries    = 5;           ///< 最大重试次数（0 表示不重连）
    uint32_t baseBackoffMs = 500;         ///< 初始退避时间（毫秒）
    uint32_t maxBackoffMs  = 30 * 1000;   ///< 最大退避时间（30秒）
    double   jitterFactor  = 0.2;         ///< 随机抖动因子（0~0.5 之间）
};

///=============================================================================
/// @brief RPC调用超时异常
///=============================================================================
class RpcTimeoutException : public std::runtime_error
{
public:
    explicit RpcTimeoutException(uint64_t seq, uint32_t timeoutMs)
        : std::runtime_error("RPC call timeout, seq: " + std::to_string(seq)
                             + ", timeout: " + std::to_string(timeoutMs) + "ms")
        , seq_(seq)
        , timeoutMs_(timeoutMs)
    {}
    uint64_t seq()      const noexcept { return seq_; }
    uint32_t timeoutMs() const noexcept { return timeoutMs_; }
private:
    uint64_t seq_;
    uint32_t timeoutMs_;
};

///=============================================================================
/// @brief RPC连接异常（网络断开 / 重连失败）
///=============================================================================
class RpcConnectionException : public std::runtime_error
{
public:
    explicit RpcConnectionException(const std::string& msg)
        : std::runtime_error("RPC connection error: " + msg)
    {}
};

///=============================================================================
/// @class RPCClient
/// @brief 支持超时控制与自动重连的 RPC 客户端
///=============================================================================
class RPCClient
{
public:
    RPCClient(EventLoop* loop, const InetAddress& serverAddr);

    ~RPCClient()
    {
        reconnectEnabled_ = false;

        if(reconnectTimerId_.valid())
        {
            loop_->cancel(reconnectTimerId_);
            reconnectTimerId_ = TimerId{};
        }

        cancelAllPending();
        tcpClient_.disconnect();
    }

    // ========== 连接 / 断开 ==========
    void start();
    void stop();
    bool connected() const { return conn_ && conn_->connected(); }

    // ========== 回调设置 ==========
    using ConnectionStateCallback = std::function<void(bool connected)>;
    void setConnectionStateCallback(ConnectionStateCallback cb)
    { connectionStateCallback_ = std::move(cb); }

    // ========== 重连策略配置 ==========
    void setReconnectConfig(const ReconnectConfig& config);
    void enableReconnect(bool on) { reconnectEnabled_ = on; }
    bool isReconnectEnabled() const { return reconnectEnabled_; }
    void resetRetryCount() { retryCount_ = 0; }

    // ========== 消息回调 ==========
    void onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm);

    // ========== 连接回调 ==========
    void onConnection(const TcpConnectionPtr& conn);

    // ========== 同步调用（无超时） ==========
    template<typename Request, typename Response>
    Response Call(Request& req);

    // ========== 同步调用（带超时） ==========
    template<typename Request, typename Response>
    Response Call(Request& req, std::chrono::milliseconds timeout);

private:
    // ========== 内部数据结构 ==========
    /// 单个待处理请求的上下文
    struct PendingContext
    {
        std::function<void(const std::string&)> callback;
        TimerId timerId;
        uint64_t seq;
        bool     completed = false;
    };

    // ========== 内部函数 ==========
    //注册模板函数
    template<typename Response>
    uint64_t registerPending(std::promise<Response>&& prom, std::chrono::milliseconds timeout);

    //通用注册函数
    uint64_t registerPendingRaw(std::function<void(const std::string&)> callback,
                                std::chrono::milliseconds timeout);

    void onTimeout(uint64_t seq);
    void removePending(uint64_t seq);
    void cancelAllPending();

    // ========== 重连逻辑 ==========
    void scheduleReconnect();
    void doReconnect();

    // ========== 连接状态通知 ==========
    void notifyConnectionState(bool isConnected);

    // ========== 成员变量 ==========

    // --- 底层网络 ---
    TcpClient       tcpClient_;
    TcpConnectionPtr conn_;
    EventLoop*       loop_;            // 缓存 EventLoop 指针

    // --- RPC 请求序列号 ---
    uint64_t nextSeq_{1};

    // pending_ 中的回调不应持有锁；移除回调到临界区之外执行
    std::map<uint64_t, PendingContext> pending_;
    mutable std::mutex                 mutex_;

    // --- 超时定时器 ID 通过 PendingContext 管理，无需独立 map ---

    // --- 重连状态 ---
    bool     reconnectEnabled_{true};
    uint32_t retryCount_{0};
    ReconnectConfig reconnectConfig_;

    TimerId reconnectTimerId_;

    // --- 连接通知回调 ---
    ConnectionStateCallback connectionStateCallback_;
};

//=============================================================================
// 内联 / 模板函数实现
//=============================================================================

template<typename Request, typename Response>
Response RPCClient::Call(Request& req)
{
    // 默认超时 5 秒
    return Call<Request, Response>(req, std::chrono::seconds(5));
}

template<typename Request, typename Response>
Response RPCClient::Call(Request& req, std::chrono::milliseconds timeout)
{
    // 检查连接状态
    if (!connected())
    {
        throw RpcConnectionException("not connected to server");
    }

    std::promise<Response> prom;
    std::future<Response>  fut = prom.get_future();

    uint64_t seq = registerPending<Response>(std::move(prom), timeout);

    // 编码并发送请求
    RPC_Meta meta{};
    meta.seq       = seq;
    meta.method_id = getMethodId<Request>();
    meta.timeout   = static_cast<uint32_t>(timeout.count());
    meta.err_code  = 0;

    Buffer sendBuff;
    encode(&sendBuff, 0, 1, meta, req);

    try {
        conn_->send(&sendBuff);
    } catch (...) {
        removePending(seq);
        throw RpcConnectionException("send faild");
    }
    

    // 等待结果或超时
    auto status = fut.wait_for(timeout);
    if (status == std::future_status::timeout)
    {
        // 超时：清理 pending 记录，定时器回调会在 onTimeout 中处理
        removePending(seq);
        throw RpcTimeoutException(seq, static_cast<uint32_t>(timeout.count()));
    }

    // 如果 promise 被设置了异常（反序列化失败），get() 会重新抛出
    try {
        return fut.get();
    } catch (std::exception& e) {
        throw RpcTimeoutException(seq, static_cast<uint32_t>(timeout.count()));
    }
}

template<typename Response>
uint64_t RPCClient::registerPending(std::promise<Response>&& prom,
                                    std::chrono::milliseconds timeout)
{
    // 用 shared_ptr 包装 promise，使得 lambda 可复制（std::function 要求）
    auto sp = std::make_shared<std::promise<Response>>(std::move(prom));

    // 包装为 std::function 兼容的签名
    auto callback = [sp](const std::string& body) mutable
    {
        if (body.empty())
        {
            // 连接断开 / 取消时传入空 body → 设异常唤醒等待线程
            sp->set_exception(
                std::make_exception_ptr(std::runtime_error("RPC connection closed / cancelled")));
            return;
        }

        Response res;
        if (res.ParseFromString(body))
        {
            sp->set_value(std::move(res));
        }
        else
        {
            sp->set_exception(
                std::make_exception_ptr(std::runtime_error("RPC response parse failed")));
        }
    };

    return registerPendingRaw(std::move(callback), timeout);
}

#endif // CLEARMOON_RPC_CLIENT_H