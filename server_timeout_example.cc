/**
 * server_timeout_example.cc
 *
 * 示例：为 RPC Server 的新连接注册超时回调定时器
 *
 * 演示内容：
 *   1. 空闲连接超时（Idle Timeout）
 *      - 连接建立后 30 秒内无数据则自动断开
 *      - 每次收到数据重置超时定时器
 *   2. 关键 API
 *      - EventLoop::runAfter()   — 延迟执行回调

#include "rpc_server.h"
#include "message.pb.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Log/Logger.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"

#include <map>
#include <memory>
#include <mutex>

// ─────────────────────────────────────────────────────────────
// 方案 A：扩展 RPCServer（增加连接回调支持）
//
// RPCServer 内部的 tcpServer_ 是私有成员，外部无法直接设置
// 连接回调。以下展示如何在 RPCServer 中增加 setConnectionCallback()
// 接口供外部使用。
//
// 建议修改 rpc_server.h，增加：
//   void setConnectionCallback(ConnectionCallback cb) {
//       tcpServer_.setConnectionCallback(std::move(cb));
//   }
// ─────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────
// 方案 B：直接使用 TcpServer（不经过 RPCServer 封装）
//
//   这种方式可以完全控制连接、消息等所有回调。
// ─────────────────────────────────────────────────────────────

struct PeerContext {
    clearmoon::net::TimerId idleTimerId;  // 空闲超时定时器 ID
    std::string peerAddr;                  // 对端地址
};

void exampleWithTcpServer() {
    EventLoop loop;

    // 1. 创建 TcpServer
    InetAddress listenAddr("127.0.0.1", 1234, false);
    TcpServer tcpServer(&loop, TcpServer::ThreadPoolInitCallback(), listenAddr);

    std::map<std::string, PeerContext> contexts;
    std::mutex ctxMutex;

    // 2. 设置连接回调 —— 新连接到来时触发
    tcpServer.setConnectionCallback([&](const TcpConnectionPtr& conn) {
        if (conn->connected()) {
            // ========== 新连接建立 ==========
            std::string peer = conn->getPeerAddr().toIpPort();
            LOG_INFO << "新连接建立: " << peer;

            // 在连接所属的 IO 线程上注册空闲超时定时器
            EventLoop* ioLoop = conn->getLoop();
            TimerId tid = ioLoop->runAfter(30.0,  // 30 秒空闲超时
                [conn, peer]() {
                    if (conn->connected()) {
                        LOG_WARNING << "空闲超时，断开连接: " << peer;
                        conn->forceClose();
                    }
                }
            );

            // 保存上下文
            {
                std::lock_guard<std::mutex> lock(ctxMutex);
                contexts[conn->name()] = {tid, peer};
            }
        } else {
            // ========== 连接断开 ==========
            LOG_INFO << "连接断开: " << conn->getPeerAddr().toIpPort();

            {
                std::lock_guard<std::mutex> lock(ctxMutex);
                contexts.erase(conn->name());
            }
        }
    });

 *      - EventLoop::cancel()     — 取消定时器
 *      - TcpConnection::getLoop()— 获取连接所属的 EventLoop
 *      - TcpConnection::forceClose() — 强制关闭连接
 */
