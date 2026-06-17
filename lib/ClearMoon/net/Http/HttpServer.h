#ifndef CLEARMOON_NET_HTTP_HTTPSERVER_H
#define CLEARMOON_NET_HTTP_HTTPSERVER_H

#include "HttpContext.h"
#include "HttpRequest.h"
#include "HttpResponse.h"
#include "../Log/Logger.h"

#include "../Callbacks.h"
#include "../TcpConnection.h"
#include "../TcpServer.h"

#include <functional>
#include <map>
#include <string>

namespace clearmoon 
{
namespace net 
{

class HttpServer : public noncopyable
{
public:
using HttpCallback = std::function<void(const TcpConnectionPtr&, HttpRequest&, HttpResponse&)>;

    explicit HttpServer(TcpServer* server)
        : server_(server)
    {
        server_->setConnectionCallback([this](const TcpConnectionPtr& conn) {
            onConnection(conn);
        });

        server_->setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
            onMessage(conn, buf, ts);
        });
    }

    ~HttpServer() = default;

    void setHttpCallback(const HttpCallback& cb)
    {
        cb_ = cb;
    }

private:
    void onConnection(const TcpConnectionPtr& conn);

    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp receiveTime);

    TcpServer* server_;
    std::map<std::string, HttpContext> context_;
    HttpCallback cb_;
};

}
}

#endif