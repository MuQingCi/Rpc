#include "Acceptor.h"
#include "InetAddress.h"
#include "Socket.h"
#include <cerrno>
#include <sys/socket.h>

using namespace clearmoon;
using namespace clearmoon::net;

Acceptor::Acceptor(EventLoop* loop, const InetAddress& addr) : loop_(loop), listenSock_(AF_INET), acceptChannel_(loop, listenSock_.Fd())
{
    listenSock_.setReuseAddress(true);
    listenSock_.setNonBlocking(true);
    listenSock_.bind(addr);
    acceptChannel_.setReadCallback([this]{ handleRead(); });
}

Acceptor::~Acceptor()
{
    acceptChannel_.disableAll();
}

void Acceptor::listen()
{
    if(listening_) 
        return;
    listenSock_.listen();
    acceptChannel_.enableReading();
    listening_ = true;
}

void Acceptor::handleRead()
{
    while (true) //循环接受新连接
    {
        InetAddress peerAddr;
        Socket conn = listenSock_.accept(&peerAddr);
        if(conn.valid())
        {
            if(connCallback_)
                connCallback_(std::move(conn), std::move(peerAddr));
        }
        
        else {
            int savedErrno = errno;
            if(savedErrno == EAGAIN || savedErrno == EWOULDBLOCK)
            {
                break;
            }
        }
    }
    
}

void Acceptor::close()
{
    if(!listening_) return;
    loop_->runInLoop([this] {
        acceptChannel_.disableAll();
        listening_ = false;
     });
    
}
