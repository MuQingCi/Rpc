#include "PooledConnection.h"
#include "net/TimerId.h"
#include "net/Log/Logger.h"

#include <atomic>
#include <mutex>
#include <utility>

PooledConnection::PooledConnection(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr, size_t Index) : loop_(loop),tcpClient_(loop,serverAddr),
index_(Index)
{
    tcpClient_.setConnectionCallback([this](cmlib::TcpConnectionPtr conn){ onConnection(std::move(conn)); });
    tcpClient_.setMessageCallback([this] (const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm) { onMessage(conn, buff, tm); });
}

PooledConnection::~PooledConnection()
{
    destroyed_.store(true, std::memory_order_release);

    cancelAllPending();

    // 清除回调，防止 TcpConnection 在 IO 线程回调到已析构的 this
    tcpClient_.setConnectionCallback(nullptr);
    tcpClient_.setMessageCallback(nullptr);

    Disconnect();
}


void PooledConnection::removePending(uint64_t seq)
{
    std::unique_lock<std::mutex> lock(mutex_);
    
    auto it = pending_.find(seq);
    if(it == pending_.end())
        return;
    auto& ctx = it->second;
    if(ctx.timerId.valid())
    {
        loop_->cancel(ctx.timerId);
        ctx.timerId = TimerId{};
    }

    it->second.cancel();
    pending_.erase(it);
}
void PooledConnection::cancelAllPending()
{
    decltype(pending_) pendingCopy;

    {
        std::unique_lock<std::mutex> lock(mutex_);

        for(auto& kv : pending_)
        {
            if(kv.second.timerId.valid())
            {
                loop_->cancel(kv.second.timerId);
            }
        }
        pendingCopy.swap(pending_);
    }

    for(auto& kv : pendingCopy)
    {
        kv.second.cancel();
    }
}

void PooledConnection::onMessage(const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm)
{
    (void)conn; (void)tm;

    if (destroyed_.load(std::memory_order_acquire))
        return;

    LOG_INFO << "RPCClient: " << index_ <<" received message";

    uint32_t minLen = sizeof(Header) + sizeof(RPC_Meta);

    //处理消息
    while (buff->readableBytes() >= minLen)
    {
        Header header{};
        RPC_Meta meta{};
        std::string body;

        if (!decode(buff, header, meta, body))
            break;

        if (header.Flags == 3) // ACK 确认
        {
            if (conn_)
                conn_->ackReceived(meta.seq);
            continue;
        }

        if (header.Flags == 1) // 响应
        {
            std::function<void(const std::string&)> callback;

            // 1. 临界区：取出回调并标记 completed
            {
                std::unique_lock<std::mutex> lock(mutex_);
                auto it = pending_.find(meta.seq);
                if (it != pending_.end())
                {
                    it->second.completed = true;
                    if(it->second.timerId.valid())
                    {
                        loop_->cancel(it->second.timerId);
                        it->second.timerId = TimerId{};
                    }

                    callback = std::move(it->second.onResponse);
                    pending_.erase(it);

                    activerequests_.fetch_sub(1,std::memory_order_relaxed);
                }
            }

            // 2. 临界区外执行回调（不持锁，不阻塞其他请求）
            if (callback)
            {
                try {
                    callback(body);
                } catch (std::exception& e) {
                    LOG_ERROR<< "RPCClient MessageHandleCallback error: " << e.what();
                }
            }
            else
            {
                LOG_WARNING << "Received response for unknown/cancelled seq=" << meta.seq;
            }
        }
    }
}


void PooledConnection::onConnection(const cmlib::TcpConnectionPtr& conn)
{
    if (destroyed_.load(std::memory_order_acquire))
        return;

    if(conn->connected()){
        conn_ = conn;
        setState(ConnState::IDLE);
    }
    else {
        cancelAllPending();
        conn_.reset();
        setState(ConnState::UNHEALTHY);
    }
}


void PooledConnection::onTimeout(uint64_t seq)
{
    std::unique_lock<std::mutex> lock(mutex_);

    auto it = pending_.find(seq);
    if(it == pending_.end()) return;

    auto& ctx = it->second;

    if(ctx.completed) return;
    ctx.completed = true;

    auto cancel = std::move(ctx.cancel);

    pending_.erase(it);
    lock.unlock();
    
    if(cancel) cancel();
}
