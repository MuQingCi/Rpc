#include "Connector.h"
#include "Log/Logger.h"

#include <cassert>
#include <cerrno>
#include <cstring>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace clearmoon;
using namespace clearmoon::net;

Connector::Connector(EventLoop* loop, const InetAddress& serverAddr)
    : loop_(loop), serverAddr_(serverAddr), state_(kDisconnected)
{
}

Connector::~Connector()
{
    assert(!channel_ || channel_->isNonEvent());
}

void Connector::start()
{
    loop_->runInLoop([this] { startInLoop(); });
}

void Connector::stop()
{
    loop_->runInLoop([this] {
        if (state_ == kConnecting)
        {
            setState(kDisconnected);
            if (channel_) 
            {
                channel_->remove();
                channel_.reset();
            }
            sock_ = Socket(); // 释放旧的 socket
        }
    });
}

void Connector::startInLoop()
{
    loop_->assertInLoopThread();
    connect();
}

void Connector::connect()
{
    sock_ = Socket(AF_INET, SOCK_STREAM, 0);
    sock_.setNonBlocking(true);
    if (!sock_.valid())
    {
        LOG_ERROR << "Connector::connect - failed to create socket";
        return;
    }

    int ret = ::connect(sock_.Fd(), serverAddr_.getAddress(), serverAddr_.getSockLen());
    if (ret == 0)
    {
        // 连接立即可用（如 Unix 域套接字或本机 loopback）
        setState(kConnected);
        InetAddress localAddr = sock_.getLocalAddr();
        if (newConnCallback_)
            newConnCallback_(std::move(sock_), std::move(localAddr));
    }
    else if (errno == EINPROGRESS)
    {
        // 异步连接中
        setState(kConnecting);
        channel_ = std::make_unique<Channel>(loop_, sock_.Fd());
        channel_->setWriteCallback([this] { handleWrite(); });
        channel_->setErrorCallback([this] { handleError(); });
        channel_->enableWriting();
    }
    else
    {
        // 连接失败
        LOG_ERROR << "Connector::connect failed, errno=" << errno << " " << strerror(errno);
        sock_ = Socket();
    }
}

void Connector::handleWrite()
{
    loop_->assertInLoopThread();
    if (state_ != kConnecting) return;

    // 检查连接是否成功：通过 getsockopt SO_ERROR
    int err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(sock_.Fd(), SOL_SOCKET, SO_ERROR, &err, &len) < 0)
        err = errno;

    if (err != 0)
    {
        LOG_ERROR << "Connector::handleWrite - connect failed: " << strerror(err);
        channel_->remove();
        channel_.reset();
        sock_ = Socket();
        setState(kDisconnected);
        return;
    }

    setState(kConnected);
    channel_->remove();
    channel_.reset();

    InetAddress localAddr = sock_.getLocalAddr();
    if (newConnCallback_)
        newConnCallback_(std::move(sock_), std::move(localAddr));
}

void Connector::handleError()
{
    LOG_ERROR << "Connector::handleError";
    if (state_ == kConnecting)
    {
        channel_->remove();
        channel_.reset();
        sock_ = Socket();
        setState(kDisconnected);
    }
}

void Connector::retry()
{
    // 预留：重试逻辑，可加入指数退避
}