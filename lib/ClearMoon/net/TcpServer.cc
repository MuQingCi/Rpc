#include "TcpServer.h"
#include "EventLoop.h"
#include "TcpConnection.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <sys/socket.h>

using namespace clearmoon;
using namespace clearmoon::net;

TcpServer::TcpServer(EventLoop* loop, const ThreadPoolInitCallback& cb, const InetAddress& listenAddr) 
                    : baseloop_(loop), 
                      eventThreadPool_(cb, "ClearMoon"), 
                      acceptor_(loop, listenAddr),
                      threadName_("ClearMoon"),
                      started_(false),
                      nextConnId_(1)
{
    acceptor_.setConnectionCallback([this] (Socket sock, InetAddress peerAddress) { newConnection(std::move(sock), std::move(peerAddress)); });
}

TcpServer::~TcpServer()
{
    stop();
}

void TcpServer::start()
{
    if (started_) return;

    //依次启动io线程池与Acceptor监听器
    eventThreadPool_.start();
    acceptor_.listen();

    started_ = true;
}

void TcpServer::stop()
{
    if(!started_) return;

    started_ = false;

    acceptor_.close();
    auto conns = connections_;
    for(auto& kv : conns)
        kv.second->forceClose();

    // Do not clear `connections_` here to avoid destroying TcpConnection objects
    // in the main thread. Each TcpConnection will be destroyed in its IO loop
    // via `removeConnectionInLoop`/`connectDestroyed` to ensure channel/poller
    // operations happen in the correct thread.
}


/**
 * @brief 用于Acceptor处理新连接的回调函数
    1.先从io线程池中以轮询的方法取一个io线程
    2.初始化新连接的名称
    3.初始化连接对应的TcpConnectionPtr
    4.将其添加到对应的<string,TcpConnectionPtr>Map中
 * 
 * @param socket 
 * @param peerAddress 
 */
void TcpServer::newConnection(Socket socket, InetAddress peerAddress)
{
    baseloop_->assertInLoopThread();

    EventLoop* ioLoop = eventThreadPool_.getNextLoop();


    std::string name = threadName_ + "#" + std::to_string(nextConnId_++);

    InetAddress local = socket.getLocalAddr();

    auto connPtr = std::make_shared<TcpConnection>(ioLoop, name, std::move(socket), local, peerAddress);
    connections_[name] = connPtr;

    //设置connPtr相关回调
    connPtr->setConnectionCallback(connectionCallback_);
    connPtr->setCloseCallback([this](const TcpConnectionPtr& c){ removeConnection(c); });
    connPtr->setMessageCallback(messageCallback_);
    connPtr->setWriteCompleteCallback(writeCompleteCallback_);

    ioLoop->runInLoop([connPtr]{ connPtr->connectEstablelished(); });
}

void TcpServer::removeConnection(const TcpConnectionPtr& conn)
{
    baseloop_->runInLoop([this, conn]{ removeConnectionInLoop(conn); });
}

void TcpServer::removeConnectionInLoop(const TcpConnectionPtr& conn)
{
    baseloop_->assertInLoopThread();
    size_t n = connections_.erase(conn->name());
    if(n != 1) return;
    EventLoop* ioloop = conn->getLoop();
    ioloop->runInLoop([conn]{ conn->connectDestroyed(); });
}