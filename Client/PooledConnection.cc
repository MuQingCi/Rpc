#include "PooledConnection.h"
#include "net/TimerId.h"
#include "net/Log/Logger.h"

#include <utility>

PooledConnection::PooledConnection(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr, size_t Index) : loop_(loop),tcpClient_(loop,serverAddr),
index_(Index)
{
    tcpClient_.setConnectionCallback([this](cmlib::TcpConnectionPtr conn){ onConnection(std::move(conn)); });
    tcpClient_.setMessageCallback([this] (const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm) { onMessage(conn, buff, tm); });
}


void PooledConnection::removePending(uint64_t seq)
{
    auto it = pending_.find(seq);
    if(it == pending_.end())
        return;
    auto ctx = it->second;
    if(ctx.timerId.valid())
    {
        loop_->cancel(ctx.timerId);
        ctx.timerId = TimerId{};
    }

    //TODO 处理目标定时器的awaiter
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
        //TODO 处理awaiter;
    }
}

void PooledConnection::onMessage(const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm)
{
    (void)conn;
    (void)tm;
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
                    // 标记完成，onTimeout 见到 completed 会跳过
                    it->second.completed = true;

                    // 取消超时定时器
                    if (it->second.timerId.valid())
                    {
                        loop_->cancel(it->second.timerId);
                    }

                    callback = std::move(it->second.callback);
                    pending_.erase(it);
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

}
