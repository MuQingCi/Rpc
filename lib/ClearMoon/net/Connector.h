#ifndef CLEARMOON_NET_CONNECTOR_H
#define CLEARMOON_NET_CONNECTOR_H

#include "../base/noncopy.h"
#include "Channel.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Socket.h"
#include "net/TimerId.h"

#include <functional>
#include <memory>

namespace clearmoon
{
namespace net
{

using NewConnectionCallback = std::function<void(Socket, InetAddress)>;

class Connector : public noncopyable,
                  public std::enable_shared_from_this<Connector>
{
public:
    Connector(EventLoop* loop, const InetAddress& serverAddr);
    ~Connector();

    void setNewConnectionCallback(const NewConnectionCallback& cb) { newConnCallback_ = cb; }

    void start();   // 发起连接
    void stop();    // 取消连接（尚未完成时）

private:
    void startInLoop();
    void connect();
    void handleWrite();   // 连接结果到达（EPOLLOUT）
    void handleError();   // 连接失败
    
    //超时处理函数
    void handleTimeout();

    void retry();         // 重试（可选）
    void setState(int s) { state_ = s; }

    enum States { kDisconnected, kConnecting, kConnected };

    EventLoop* loop_;
    InetAddress serverAddr_;
    int state_;
    std::unique_ptr<Channel> channel_;
    Socket sock_;
    NewConnectionCallback newConnCallback_;

    //连接超时定时器
    TimerId connectTimeoutId_;

    double connectTimeout_ = 1; //1s
};

} // namespace net
} // namespace clearmoon

#endif