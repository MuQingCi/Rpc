#include "TcpClient.h"

using namespace clearmoon;
using namespace clearmoon::net;

TcpClient::TcpClient(EventLoop* loop, const InetAddress& serverAddr)
    : loop_(loop),
      serverAddr_(serverAddr),
      connector_(std::make_shared<Connector>(loop, serverAddr))
{
    connector_->setNewConnectionCallback(
        [this](Socket socket, InetAddress localAddr) {
            newConnection(std::move(socket), std::move(localAddr));
        });
}

TcpClient::~TcpClient()
{
    if (connection_)
    {
        connection_->setCloseCallback([](const TcpConnectionPtr&) {});
        connection_->forceClose();
    }
}

void TcpClient::connect()
{
    connector_->start();
}

void TcpClient::disconnect()
{
    connector_->stop();
    if (connection_)
    {
        connection_->shutdown();
        // connection_.reset();   // 防止 ~TcpClient() 重复 forceClose()
    }
}

void TcpClient::newConnection(Socket socket, InetAddress localAddr)
{
    loop_->assertInLoopThread();

    static std::atomic<int> connId{1};
    std::string connName = "TcpClient-" + std::to_string(connId++);

    // localAddr = 本机绑定地址(由Connector通过getsockname获取)
    // serverAddr_ = 对端服务端地址 = TcpConnection的peerAddr
    auto conn = std::make_shared<TcpConnection>(
        loop_, connName, std::move(socket), localAddr, serverAddr_);

    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);
    conn->setCloseCallback([this](const TcpConnectionPtr& c) { removeConnection(c); });

    connection_ = conn;
    conn->connectEstablelished();
}

void TcpClient::removeConnection(const TcpConnectionPtr& conn)
{
    loop_->assertInLoopThread();
    if (connection_ == conn)
    {
        // 延后到本轮事件批处理结束后再析构连接：
        // Channel::handleEvent 可能在同一批事件中先触发 error 分支(handleClose)再触发
        // write 分支；若连接在 handleClose 内即析构，后续分支将访问悬垂的 Channel。
        loop_->queueInLoop([this, conn] {
            if (connection_ == conn)
            {
                connection_.reset();
                conn->connectDestroyed();

                if(closeCallback_) closeCallback_(conn);
            }
        });
    }
}