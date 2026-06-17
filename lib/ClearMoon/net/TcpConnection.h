#ifndef CLEARMOON_NET_TCPCONNECTION_H
#define CLEARMOON_NET_TCPCONNECTION_H

#include "../base/noncopy.h"
#include "Callbacks.h"
#include "Channel.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Socket.h"

#include <cstddef>
#include <memory>
#include <string>
#include <sys/types.h>

namespace clearmoon 
{
namespace net 
{
class TcpConnection : public noncopyable,
                      public std::enable_shared_from_this<TcpConnection>
{
public:
    TcpConnection(EventLoop* loop, std::string name, Socket socket, InetAddress& localAddr, InetAddress& peerAddr);
    ~TcpConnection();

    //TcpServer调用
    void connectEstablelished();
    void connectDestroyed();
   

    void shutdown();   //优雅关闭
    void forceClose(); //强制关闭

    //设置回调函数
    void setConnectionCallback(const ConnectionCallback& cb) { connectionCallback_ = cb; }
    void setMessageCallback(const MessageCallback& cb) { messageCallback_ = cb; }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) { writeCompleteCallback_ = cb; }
    void setCloseCallback(const CloseCallback& cb) { closeCallback_ = cb; }

    //获取本类成员变量
    const std::string name() const { return name_; }
    EventLoop* getLoop() const { return loop_; }
    const InetAddress& getLocalAddr() const { return localAddr_; }
    const InetAddress& getPeerAddr() const { return peerAddr_; }

    bool connected() const { return state_ == kConnected; }

    void send(const std::string& message);
    void send(const void*data, size_t len);
    void sendInLoop(const void*data, size_t len);

    // ========== 文件发送接口 ==========
    /**
     * @brief 使用 sendfile 零拷贝发送文件
     * @param filePath 文件路径
     */
    void sendFile(const std::string& filePath);
    void sendFileInLoop(const std::string& filePath);

private:
    enum StateE{
        kConnecting,
        kConnected,
        kDisConnecting,
        kDisConnected
    };

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    void shutdownInLoop();
    void forceCloseInLoop();

    void setState(StateE s) { state_ = s; }
    EventLoop* loop_;
    Channel channel_;
    Socket socket_;
    InetAddress localAddr_;
    InetAddress peerAddr_;

    std::string name_;
    StateE state_{kConnecting};

    Buffer writeBuffer_;
    Buffer readBuffer_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    CloseCallback closeCallback_;

    // ========== 文件发送状态 ==========
    int fileFd_ = -1;            // 当前要发送的文件描述符
    off_t fileSentOffset_ = 0;   // 已发送的字节偏移
    off_t fileTotalSize_ = 0;    // 文件总大小
    bool sendingFile_ = false;   // 是否正在发送文件

};
}
}

#endif