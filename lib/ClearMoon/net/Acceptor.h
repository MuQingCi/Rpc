#ifndef CLEARMOON_NET_ACCEPTOR_H
#define CLEARMOON_NET_ACCEPTOR_H

#include "../base/noncopy.h"
#include "InetAddress.h"
#include "Socket.h"
#include "EventLoop.h"
#include "Channel.h"

#include <functional>

namespace clearmoon 
{
namespace net 
{

class Acceptor : public noncopyable
{
public:
using newConnectionCallback = std::function<void(Socket, InetAddress)>;

    Acceptor(EventLoop* loop, const InetAddress& addr);
    ~Acceptor();

    void setConnectionCallback(const newConnectionCallback& cb) { connCallback_ = cb; }
    //由TcpServer调用
    void listen();

    void close();
private:
    //传给channel的回调
    void handleRead();

    EventLoop* loop_;
    Socket listenSock_;

    Channel acceptChannel_;

    newConnectionCallback connCallback_;
    bool listening_ = false;
};

}//net
}//clearmoon

#endif