// HttpServer is implemented inline in HttpServer.h
// This file exists only to satisfy the build system.
#include "HttpServer.h"

using namespace clearmoon;
using namespace clearmoon::net;

void HttpServer::onConnection(const TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        // 连接建立，初始化 HttpContext
        context_[conn->name()] = HttpContext();
    }
    else
    {
        // 连接断开，清理 HttpContext
        context_.erase(conn->name());
    }
}

void HttpServer::onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp receiveTime)
{
    auto it = context_.find(conn->name());
    if (it == context_.end())
    {
        // 不应该发生，但安全处理
        conn->forceClose();
        return;
    }

    HttpContext& context = it->second;
    HttpRequest request;
    HttpResponse response;

    // 默认 Connection: keep-alive
    response.setHeader("Connection", "keep-alive");

    if (context.parse(buf, &request, &response))
    {
        // HTTP 请求解析完成，回调用户逻辑
        if (cb_)
        {
            cb_(conn, request, response);
        }

        // ========== 文件模式：先发头部，再用 sendfile 发送文件体 ==========
        if (response.isFileMode())
        {
            Buffer headerBuf;
            response.serializeHeaders(&headerBuf);
            conn->send(headerBuf.peek(), headerBuf.readableBytes());
            conn->sendFile(response.getFilePath());
        }
        else
        {
            // 普通模式：将完整响应序列化后发送
            Buffer outputBuf;
            response.appendToBuffer(&outputBuf);
            conn->send(outputBuf.peek(), outputBuf.readableBytes());
        }

        // 重置 context 准备解析下一个请求
        context.reset();

        // 如果要求关闭连接
        if (response.getCloseConnection())
        {
            conn->forceClose();
        }
    }
    // 否则等待更多数据
}