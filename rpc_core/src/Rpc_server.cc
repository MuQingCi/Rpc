#include "../include/Rpc_server.h"
#include "Service/Endpoint.h"
#include "TaskThreadPool.h"
#include "Message.pb.h"
#include "ToolFunc.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/Log/Logger.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"


#include <arpa/inet.h>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <utility>
#include <ifaddrs.h>

RPCServer::RPCServer(cmlib::EventLoop* loop, cmlib::InetAddress& listenAddr) : tcpServer_(loop, cmlib::TcpServer::ThreadPoolInitCallback(), listenAddr), taskThreadPool_(std::make_unique<TaskThreadPool>()),
listenAddr_(listenAddr) //任务线程池的线程数默认为8
{
    tcpServer_.setMessageCallback([this](const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm) { onMessage(conn, buff, tm); });

    taskThreadPool_->start();
}

RPCServer::RPCServer(cmlib::EventLoop* loop, 
                     cmlib::InetAddress& listenAddr,
                     std::shared_ptr<isServiceRegister> registry,
                     const std::string& serviceName) 
                    : tcpServer_(loop, 
                        cmlib::TcpServer::ThreadPoolInitCallback(), 
                        listenAddr),
                       taskThreadPool_(std::make_unique<TaskThreadPool>()),
                       listenAddr_(listenAddr),
                       registry_(std::move(registry))
{
    if(!serviceName.empty()) serviceNames_.insert(serviceName);
    tcpServer_.setMessageCallback([this](const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm) { onMessage(conn, buff, tm); });
    taskThreadPool_->start();
}

RPCServer::RPCServer(cmlib::EventLoop* loop, 
                     cmlib::InetAddress& listenAddr,
                     std::shared_ptr<isServiceRegister> registry)
                    :tcpServer_(loop, cmlib::TcpServer::ThreadPoolInitCallback(), listenAddr),
                    taskThreadPool_(std::make_unique<TaskThreadPool>()),
                    listenAddr_(listenAddr),
                    registry_(std::move(registry))
{
    tcpServer_.setMessageCallback([this](const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm) { onMessage(conn, buff, tm); });
    taskThreadPool_->start();
}

RPCServer::~RPCServer()
{
    stop();
}

void RPCServer::onMessage(const cmlib::TcpConnectionPtr& conn, cmlib::Buffer* buff, cmlib::Timestamp tm)
{   

    uint32_t minLen = sizeof(Header) + sizeof(RPC_Meta);
    // while(decode(buff, header, body))
    while(buff->readableBytes() >= minLen)
    {
        Header header;
        RPC_Meta meta;
        std::string body;
        
        if(!decode(buff, header, meta, body)) break;

        if (header.Flags == static_cast<uint8_t>(Flag::kAck)) // ACK 确认
        {
            if (conn)
                conn->ackReceived(meta.seq);
            continue;
        }

        if(header.Flags == static_cast<uint8_t>(Flag::kRequest))   //请求
        {
            if(meta.retransmit == 1)
            {
                auto metaCopy = meta;
                metaCopy.retransmit = 0;
                cmlib::Buffer ackBuff;
                encode(&ackBuff, Flag::kAck, kVersion, metaCopy, nullptr);
                conn->send(&ackBuff);
            }

            auto it = handles_.find(meta.method_id);
            if(it == handles_.end())
            {
                LOG_ERROR << "Method not found";
                sendErrorResponse(conn, meta, RpcErrorCode::MethodNotFound);
                continue;
            }

            auto handler = it->second;
            std::weak_ptr<cmlib::TcpConnection> weakPtr = conn;
            cmlib::EventLoop* ioloop = conn->getLoop();
           
            auto success = taskThreadPool_->tryEnqueue([weakPtr,ioloop,tm,handler,meta = std::move(meta),body = std::move(body)] 
            {
                std::unique_ptr<google::protobuf::Message> response;

                try 
                {
                    response = handler(body);
                } 
                catch (const std::exception& e) 
                {
                    LOG_ERROR<< "Handle exception: "<<e.what() << ", seq= " <<meta.seq;
                    ioloop->runInLoop([weakPtr,meta = std::move(meta)]()
                    {
                        auto conn = weakPtr.lock();
                        if(conn)
                        {
                            RPC_Meta respMeta = meta;
                            respMeta.err_code = static_cast<int32_t>(RpcErrorCode::InternalError);
                            respMeta.retransmit = 0;
                            cmlib::Buffer errorBuff;
                            encode(&errorBuff, Flag::kResponse, kVersion, respMeta, nullptr);
                            conn->send(&errorBuff);
                        }
                    });
                    return;
                } 
                catch(...)
                {
                    LOG_ERROR << "Unknown handler exception, seq=" << meta.seq;
                    ioloop->runInLoop([weakPtr, meta = std::move(meta)]() {
                        auto conn = weakPtr.lock();
                        if (conn) {
                            RPC_Meta respMeta = meta;
                            respMeta.err_code = static_cast<int32_t>(
                                RpcErrorCode::InternalError);
                            respMeta.retransmit = 0;
                            cmlib::Buffer buf;
                            encode(&buf, Flag::kResponse, kVersion, respMeta, nullptr);
                            conn->send(&buf);
                        }
                    });
                    return;
                }
                
                // 处理结果
                auto respShared = std::shared_ptr<google::protobuf::Message>(
                    std::move(response));
                ioloop->runInLoop(
                    [weakPtr, respShared, meta = std::move(meta)]() mutable
                {
                    auto conn = weakPtr.lock();
                    if (!conn || !conn->connected()) return;

                    if (respShared) {
                        meta.err_code = static_cast<int32_t>(
                            RpcErrorCode::NoError);
                        meta.retransmit = 0;
                        cmlib::Buffer sendBuf;
                        encode(&sendBuf, Flag::kResponse, kVersion, meta, respShared.get());
                        conn->send(&sendBuf);
                    } else {
                        LOG_WARNING << "Handler returned null, seq=" << meta.seq;
                        meta.err_code = static_cast<int32_t>(
                            RpcErrorCode::ParseError);
                        meta.retransmit = 0;
                        cmlib::Buffer sendBuf;
                        encode(&sendBuf, Flag::kResponse, kVersion, meta, nullptr);
                        conn->send(&sendBuf);
                    }
                });
            });
            if(!success)
            {
                LOG_WARNING<<"TaskThreadPool is full, seq=" <<meta.seq;
                sendErrorResponse(conn, meta, RpcErrorCode::TaskPoolFull);
            }
        }
    }
}

void RPCServer::start()
{
    std::unique_lock<std::mutex> lock(mutex_);
    if(started_) return;

    if(registry_)
    {
        if(serviceNames_.empty())
            LOG_WARNING<<"No service names configured in starting";

        Endpoint ep;
        ep.host = getLocalIp();
        ep.port = listenAddr_.toPort();
        ep.weight = 1;

        for(const auto& svc : serviceNames_)
        {
            ep.service = svc;
            registry_->registerService(svc, ep);
            LOG_INFO << "Service " << svc << " registered at " << ep.host << ":" << ep.port;
        }
    }else {
        throw std::runtime_error("No service names configured");
    }

    tcpServer_.start();
    started_ = true;
}

void RPCServer::stop()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if(!started_) return;
        started_ = false;
    }
    tcpServer_.stop();

    std::unique_lock<std::mutex> lock(mutex_);
    if(registry_)
    {
        Endpoint ep;
        ep.host = getLocalIp();
        ep.port = listenAddr_.toPort();
        ep.weight = 1;

        for(const auto& svc : serviceNames_)
        {
            ep.service = svc;
            registry_->deregisterService(svc, ep);
            LOG_INFO << "Service " << svc << " deregistered at " << ep.host << ":" << ep.port;
        }
    }
    registry_->shutdown();
}

void RPCServer::addService(const std::string& serviceName)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if(serviceNames_.count(serviceName))
    {
        LOG_WARNING << "The serviceName has found in serviceNameSet";
        return;
    }
    serviceNames_.insert(serviceName);

    Endpoint ep;
    ep.service = serviceName;
    ep.host = getLocalIp();
    ep.port = listenAddr_.toPort();
    ep.weight = 1;

    if(started_)
        registry_->registerService(serviceName, ep);
}

//-------私有成员函数-------
std::string RPCServer::handleMessage(const std::string& msg)
{
    return "RPCServer Echo: " + msg;
}

std::string RPCServer::getLocalIp() const
{
    //读取本地地址环境变量，若有效则直接返回环境变量所指地址
    const char* env = getenv("MY_RPC_LOCAL_IP");

    if (env && env[0] != '\0') 
    {
        return env;   // 环境变量有效，直接返回
    }

    //获取网卡IP等非环回地址
    //-----基于测试才将其注释掉
    // struct ifaddrs* ifAddrStruct = nullptr;
    // if(getifaddrs(&ifAddrStruct) == 0)
    // {
    //     for(struct ifaddrs* ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next)
    //     {
    //         //如果地址为空或者不为Ipv4则跳过
    //         if(!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET)
    //             continue;
    //         auto* addr_in = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
    //         //排除环回地址
    //         if(addr_in->sin_addr.s_addr == htonl(INADDR_LOOPBACK))
    //             continue;

    //          char ip[INET_ADDRSTRLEN];
    //          inet_ntop(AF_INET, &addr_in->sin_addr, ip, sizeof(ip));
    //          freeifaddrs(ifAddrStruct);

    //          return std::string(ip);
    //     }
    //     freeifaddrs(ifAddrStruct);
    // }

    LOG_WARNING<<"GetLocalIp return ip is 127.0.0.1";
    return "127.0.0.1";
}

void RPCServer::sendErrorResponse(const cmlib::TcpConnectionPtr& conn,
                                  const RPC_Meta& requestMeta,
                                  RpcErrorCode errcode)
{
    if(!conn || !conn->connected()) return;
    RPC_Meta responseMeta = requestMeta;
    responseMeta.retransmit = 0;
    responseMeta.err_code = static_cast<int32_t>(errcode);

    cmlib::Buffer errorBuff;
    encode(&errorBuff, Flag::kResponse, kVersion, responseMeta, nullptr);
    
    conn->send(&errorBuff);
}