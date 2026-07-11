#include "PooledConnection.h"

#include <utility>

PooledConnection::PooledConnection(cmlib::EventLoop* loop, const cmlib::InetAddress& serverAddr, size_t Index) : loop_(loop),tcpClient_(loop,serverAddr),
index_(Index)
{
    tcpClient_.setConnectionCallback([this](cmlib::TcpConnectionPtr conn){ onConnection(std::move(conn)); });
    tcpClient_.setMessageCallback([this] (const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm) { onMessage(conn, buff, tm); });
}


void PooledConnection::removePending(uint64_t seq)
{

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
