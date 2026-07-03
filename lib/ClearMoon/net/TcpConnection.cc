#include "TcpConnection.h"
#include "Log/Logger.h"
#include "net/TimerId.h"

#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <random>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>

using namespace clearmoon;
using namespace clearmoon::net;

const uint32_t TcpConnection::RetransmitEntry::kBaseTimeout = 1;    //初始超时重传时间为1000ms

const uint32_t TcpConnection::RetransmitEntry::kMaxTimeout = 10;    //最大延迟为10s

thread_local std::mt19937 rng(std::random_device{}());

namespace clearmoon 
{
namespace net 
{
//指数退避计时函数
//随机抖动版本
std::chrono::milliseconds caculate_backoff_jitter(
    uint32_t base_delay_ms,
    uint32_t retries,
    uint32_t max_delay_ms,
    std::mt19937& rng
)
{
    uint64_t cap_ms = base_delay_ms;

    for(int i=0; i<retries;i++)
    {
        if(cap_ms > max_delay_ms / 2)
        {
            cap_ms = max_delay_ms;
            break;
        }
        cap_ms *=2;
    }

    cap_ms = std::min(cap_ms, static_cast<uint64_t>(max_delay_ms));

    std::uniform_int_distribution<uint64_t> dist(0, cap_ms);
    auto jitter_ms = dist(rng);

    return std::chrono::milliseconds(jitter_ms);
}
}
}



TcpConnection::TcpConnection(EventLoop* loop, std::string name, 
                            Socket socket, InetAddress& localAddr, 
                            InetAddress& peerAddr) 
                            : loop_(loop), 
                              channel_(loop,socket.Fd()),
                              name_(name), 
                              socket_(std::move(socket)), 
                              localAddr_(std::move(localAddr)), 
                              peerAddr_(std::move(peerAddr))
                              
{
    channel_.setReadCallback([this]{ handleRead(); });
    channel_.setWriteCallback([this] { handleWrite(); });
    channel_.setErrorCallback([this] { handleError(); });
    socket_.setNonBlocking(true);
}

TcpConnection::~TcpConnection()
{
    assert(state_ == kDisConnected);
    if(state_ != kDisConnected)
        forceClose();

    if(idleTimerId_.valid())
    {
        loop_->cancel(idleTimerId_);
        idleTimerId_ = TimerId();
    }
    
    if(!pendingRetrans_.empty())
    {
        for(auto&kv : pendingRetrans_)
        {
            if(kv.second.timerId.valid())
            {
                loop_->cancel(kv.second.timerId);
            }
        }
        pendingRetrans_.clear();
    }
}

void TcpConnection::connectEstablelished()
{
    loop_->assertInLoopThread();
    assert(state_ == kConnecting);

    channel_.enableReading();
    setState(kConnected);

    //连接建立时启动IdleTimer
    resetIdleTimer();

    if(connectionCallback_) 
        connectionCallback_(shared_from_this());
}

void TcpConnection::connectDestroyed()
{
    loop_->assertInLoopThread();

    if(state_ == kConnected)
    {
        setState(kDisConnected);
    }

    //把空闲清理定时器置空
    if(idleTimerId_.valid())
    {
        loop_->cancel(idleTimerId_);
        idleTimerId_ = TimerId();
    }
    
    if(!pendingRetrans_.empty())
    {
        for(auto&kv : pendingRetrans_)
        {
            if(kv.second.timerId.valid())
            {
                loop_->cancel(kv.second.timerId);
            }
        }
        pendingRetrans_.clear();
    }

    // 不需要先 disableAll()，channel_.remove() 内部会完成 epoll 摘除
    channel_.remove();
}


void TcpConnection::shutdown()
{
    if(state_ == kConnected)
    {
        setState(kDisConnecting);
        loop_->runInLoop([this] { shutdownInLoop(); });
    }
}

void TcpConnection::forceClose()
{
    if(state_ == kConnected || state_ == kDisConnecting)
    {
        setState(kDisConnecting);
        loop_->runInLoop([this]{ forceCloseInLoop(); });
    }
}

void TcpConnection::send(Buffer* buff)
{
    send(buff->peek(), buff->readableBytes());
}

void TcpConnection::send(const std::string& message)
{
    send(message.data(), message.size());
}
void TcpConnection::send(const void* data, size_t len)
{
    if(state_ != kConnected) return;

    if (loop_->isInThread()) {
        sendInLoop(data, len);
    } else {
        // 为保证跨线程发送时数据有效性，拷贝数据到 shared_ptr<string>
        auto buf = std::make_shared<std::string>(static_cast<const char*>(data), len);
        loop_->runInLoop([this, buf]{ sendInLoop(buf->data(), buf->size()); });
    }
}

//可靠传输（支持超时重传)
void TcpConnection::sendWithRetransmit(const void*data, size_t len, uint64_t seq)
{
    //构建名为SendFunc的lambda函数
    auto SendFunc = [this, data = std::string(static_cast<const char*>(data), len), seq] { 
        //构造超时重传上下文
        RetransmitEntry entry;
        entry.seq = seq;
        entry.data = data;
        entry.retries = 0;
        send(entry.data.data(), entry.data.size());

        //填充待处理重传map并启动超时重传定时器
        pendingRetrans_[seq] = std::move(entry);
        resetRetransmitTimer(pendingRetrans_[seq]);
    };

    //确保SendFunc位于io线程中被执行
    if(loop_->isInThread())
    {
        SendFunc();
    }else {
        loop_->runInLoop(std::move(SendFunc));
    }
}

void TcpConnection::sendWithRetransmit(Buffer* buffer, uint64_t seq)
{
    auto SendFunc = [this, buffer, seq]
    {
        //该函数一定位于io线程中运行
        RetransmitEntry entry;
        entry.seq = seq;
        entry.data = std::string(buffer->peek(), buffer->readableBytes());
        entry.retries = 0;

        sendInLoop(buffer->peek(), buffer->readableBytes());

        pendingRetrans_[seq] = std::move(entry);
        resetRetransmitTimer(pendingRetrans_[seq]);
    };

    //确保SendFunc位于io线程中被执行
    if(loop_->isInThread())
    {
        SendFunc();
    }else {
        loop_->runInLoop(std::move(SendFunc));
    }
}

void TcpConnection::ackReceived(uint64_t seq)
{
    if(loop_->isInThread())
    {
        auto it = pendingRetrans_.find(seq);
        if(it != pendingRetrans_.end())
        {
            if(it->second.timerId.valid())
            {
                loop_->cancel(it->second.timerId);
            }
            pendingRetrans_.erase(seq);
        }
    }
    else {
        loop_->runInLoop([this, seq]{
            auto it = pendingRetrans_.find(seq);
            if(it != pendingRetrans_.end())
            {
                if(it->second.timerId.valid())
                {
                    loop_->cancel(it->second.timerId);
                }
                pendingRetrans_.erase(seq);
            }
        });

    }
}


// ========== 文件发送接口 ==========
void TcpConnection::sendFile(const std::string& filePath)
{
    if(state_ != kConnected) return;

    if(loop_->isInThread())
    {
        sendFileInLoop(filePath);
    }
    else
    {
        loop_->runInLoop([this, filePath] { sendFileInLoop(filePath); });
    }
}


//---------------private---------------
void TcpConnection::sendInLoop(const void* data, size_t len)
{
    loop_->assertInLoopThread();
    if(state_ == kDisConnected) return;

    ssize_t n = 0;
    if(!channel_.isWriting() && writeBuffer_.readableBytes() == 0)
    {
        n = ::write(channel_.getFd(), data, len);
        if(n >= 0)
        {
            if(static_cast<size_t>(n) == len)
            {
                if(writeCompleteCallback_) writeCompleteCallback_(shared_from_this());
                return;
            }
        }
        else
        {
            n = 0;
            if(errno != EAGAIN)
            {
                //TODO 错误处理
            }
        }
    }

    if(static_cast<size_t>(n) < len)
    {
        writeBuffer_.append(static_cast<const char*>(data) + n, len - n);
        if(!channel_.isWriting())
        {
            channel_.enableWriting();
        }
    }

    if(state_ == kDisConnecting && writeBuffer_.readableBytes() == 0)
        shutdownInLoop();
}

void TcpConnection::sendFileInLoop(const std::string& filePath)
{
    loop_->assertInLoopThread();
    if(state_ == kDisConnected) return;

    // 打开文件
    int fd = ::open(filePath.c_str(), O_RDONLY);
    if(fd < 0)
    {
        LOG_INFO<< "Can't open the file!";
        // 文件无法打开，强制关闭连接
        forceCloseInLoop();
        return;
    }

    LOG_INFO << "Open The File!";

    // 获取文件大小
    struct stat st;
    if(::fstat(fd, &st) < 0)
    {
        ::close(fd);
        forceCloseInLoop();
        return;
    }

    // 设置文件发送状态
    fileFd_ = fd;
    fileSentOffset_ = 0;
    fileTotalSize_ = st.st_size;
    sendingFile_ = true;

    // 启用 EPOLLOUT 事件，handleWrite 中会用 sendfile 发送
    channel_.enableWriting();
}



void TcpConnection::handleRead()
{
    loop_->assertInLoopThread();

    int savedErrno = 0;
    ssize_t n = readBuffer_.readFd(channel_.getFd(), &savedErrno);

    Timestamp t = Timestamp::now();
    if(n > 0 )
    {
        //重置空闲处理定时器
        resetIdleTimer();

        if(messageCallback_) messageCallback_(shared_from_this(), &readBuffer_, t);
    }
    else if(n == 0)
    {
        handleClose();
    }
    else {
        if(savedErrno != EAGAIN)
        //TODO 处理错误
        handleError();
    }
        
}

void TcpConnection::handleWrite()
{
    loop_->assertInLoopThread();

    if(!channel_.isWriting()) return;

    //重置空闲处理定时器
    resetIdleTimer();

    // ========== 先排空 writeBuffer_（比如文件模式下响应头可能还未发送完）==========
    if(writeBuffer_.readableBytes() > 0)
    {
        int savedErrno = 0;
        ssize_t n = writeBuffer_.WriteFd(channel_.getFd(), &savedErrno);
        if(n > 0)
        {
            if(writeBuffer_.readableBytes() == 0 && !sendingFile_)
                channel_.disableWriting();
            if(writeCompleteCallback_)
                writeCompleteCallback_(shared_from_this());
        }
        else {
            if(savedErrno != EAGAIN)
                handleError();
        }
        // 如果 writeBuffer_ 还有数据未发完，等待下次 EPOLLOUT
        if(writeBuffer_.readableBytes() > 0)
            return;
    }

    // ========== 文件发送模式（sendfile 零拷贝）==========
    if(sendingFile_ && fileFd_ >= 0)
    {
        const size_t chunkSize = 64 * 1024; // 每次最多 64KB
        ssize_t sent = ::sendfile(channel_.getFd(), fileFd_, &fileSentOffset_, chunkSize);

        if(sent > 0)
        {
            if(fileSentOffset_ >= fileTotalSize_)
            {
                // 文件发送完毕
                ::close(fileFd_);
                fileFd_ = -1;
                sendingFile_ = false;
                channel_.disableWriting();

                LOG_INFO<<"Send the file Sucesslly";

                if(writeCompleteCallback_)
                    writeCompleteCallback_(shared_from_this());
            }
            // 否则继续等待下一次 EPOLLOUT 事件
        }
        else if(sent < 0)
        {
            int err = errno;
            if(err != EAGAIN)
            {
                LOG_ERROR<< "Error: Send the file";
                // 发送出错
                ::close(fileFd_);
                fileFd_ = -1;
                sendingFile_ = false;
                channel_.disableWriting();
                handleError();
            }
            // EAGAIN 仅等待下一次重试
        }

        return;
    }

    // ========== 普通 buffer 发送模式（仅在 writeBuffer_ 非空且非文件模式时进入）==========
    if(writeBuffer_.readableBytes() > 0)
    {
        int savedErrno = 0;
        ssize_t n = writeBuffer_.WriteFd(channel_.getFd(), &savedErrno);

        if(n > 0)
        {
            if(writeBuffer_.readableBytes() == 0)
                channel_.disableWriting();
            if(writeCompleteCallback_)
                writeCompleteCallback_(shared_from_this());
            if(state_ == kDisConnecting)
                shutdownInLoop();
        }
        else {
            if(savedErrno != EAGAIN)
            //TODO 处理错误
            handleError();
        }
    }
}

void TcpConnection::handleClose()
{
    LOG_INFO << "Handle Close";
    
    loop_->assertInLoopThread();
    
    // 防重入：如果已经断开，不再重复处理
    if(state_ == kDisConnected)
        return;
    
    assert(state_ == kConnected || state_ == kDisConnecting);
    setState(kDisConnected);
    // channel_.remove() 内部会调用 loop_->removeChannel() 完成 epoll 移除
    // 不需要先调用 disableAll()，避免重复 epoll_ctl DEL 导致的断言失败
    channel_.remove();

    // 清理文件发送状态
    if(fileFd_ >= 0)
    {
        ::close(fileFd_);
        fileFd_ = -1;
    }
    fileSentOffset_ = 0;
    fileTotalSize_ = 0;
    sendingFile_ = false;

    if(idleTimerId_.valid())
    {
        loop_->cancel(idleTimerId_);
        idleTimerId_ = TimerId();
    }
    
    if(!pendingRetrans_.empty())
    {
        for(auto&kv : pendingRetrans_)
        {
            if(kv.second.timerId.valid())
            {
                loop_->cancel(kv.second.timerId);
            }
        }
        pendingRetrans_.clear();
    }

    if(closeCallback_)
        closeCallback_(shared_from_this());
}

void TcpConnection::handleError()
{
    //TODO 日志记录错误
    handleClose();
}

void TcpConnection::shutdownInLoop()
{
    loop_->assertInLoopThread();
    if(!channel_.isWriting())
        socket_.shutdownWrite();
}

void TcpConnection::forceCloseInLoop()
{
    loop_->assertInLoopThread();
    if(state_ == kConnected || state_ == kDisConnecting)
        handleClose();
}

void TcpConnection::resetRetransmitTimer(RetransmitEntry& entry)
{
    if(entry.timerId.valid())
    {
        loop_->cancel(entry.timerId);
        entry.timerId = TimerId();
    }

    //指数退避计算时间
    auto timeout = caculate_backoff_jitter(RetransmitEntry::kBaseTimeout, entry.retries, RetransmitEntry::kMaxTimeout, rng);

    entry.timerId = loop_->runAfter(timeout.count() / 1000.0, [this, seq = entry.seq]{
        onRetransmitTimeout(seq);
    });
}

void TcpConnection::onRetransmitTimeout(uint64_t seq)
{
    loop_->assertInLoopThread();
    auto it = pendingRetrans_.find(seq);
    if(it == pendingRetrans_.end()) return;

    auto &entry = it->second;

    if(entry.retries >= RetransmitEntry::kMaxRetries) 
    {
        LOG_WARNING<< "超时重传超出最大重传数, seq = "<<seq;
        pendingRetrans_.erase(it);
        forceCloseInLoop();
        return;
    }
    
    sendInLoop(entry.data.data(), entry.data.size());
    entry.retries++;
    resetRetransmitTimer(entry);
}


void TcpConnection::resetIdleTimer()
{
    //若空闲处理定时器已经存在则先cancel再重新添加
    if(idleTimerId_.valid())
    {
        loop_->cancel(idleTimerId_);
        idleTimerId_ = TimerId();
    }

    idleTimerId_ = loop_->runAfter(kTimeoutSeconds_, [this] { onIdleTimeout(); });
}

void TcpConnection::onIdleTimeout()
{ 
    loop_->assertInLoopThread();
    // shutdown();
    forceCloseInLoop();
}