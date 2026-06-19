#ifndef CLEARMOON_NET_TCPCLIENT_H
#define CLEARMOON_NET_TCPCLIENT_H

#include "../base/noncopy.h"
#include "Callbacks.h"
#include "Connector.h"
#include "InetAddress.h"
#include "TcpConnection.h"

#include <memory>

namespace clearmoon
{
namespace net
{

class TcpClient : public noncopyable
{
public:
    TcpClient(EventLoop* loop, const InetAddress& serverAddr);
    ~TcpClient();

    void connect();
    void disconnect();

    void setConnectionCallback(const ConnectionCallback& cb) { connectionCallback_ = cb; }
    void setMessageCallback(const MessageCallback& cb) { messageCallback_ = cb; }

    TcpConnectionPtr connection() const { return connection_; }
    EventLoop* getLoop() const { return loop_; }

    bool connected() const { return connection_ && connection_->connected(); }

private:
    void newConnection(Socket socket, InetAddress localAddr);
    void removeConnection(const TcpConnectionPtr& conn);

    EventLoop* loop_;
    InetAddress serverAddr_;
    std::shared_ptr<Connector> connector_;
    TcpConnectionPtr connection_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
};

} // namespace net
} // namespace clearmoon

#endif