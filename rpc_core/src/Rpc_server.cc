#include "../include/Rpc_server.h"
#include "Service/Endpoint.h"
#include "TaskThreadPool.h"
#include "message.pb.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/Log/Logger.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"

// Protobuf 消息定义
#include "message.pb.h"

//工具函数
#include "ToolFunc.h"

#include <arpa/inet.h>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <utility>
#include <ifaddrs.h>

RPCServer::RPCServer(EventLoop* loop, InetAddress& listenAddr) : tcpServer_(loop, TcpServer::ThreadPoolInitCallback(), listenAddr), taskThreadPool_(std::make_unique<TaskThreadPool>()) //任务线程池的线程数默认为8
{
    tcpServer_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm) { onMessage(conn, buff, tm); });

    taskThreadPool_->start();
}

RPCServer::RPCServer(EventLoop* loop, 
                     InetAddress& listenAddr,
                     std::shared_ptr<isServiceRegister> registry,
                     const std::string& serviceName) 
                    : tcpServer_(loop, 
                        TcpServer::ThreadPoolInitCallback(), 
                        listenAddr),
                       taskThreadPool_(std::make_unique<TaskThreadPool>()),
                       listenAddr_(listenAddr),
                       registry_(std::move(registry)),
                       serviceName_(serviceName)
{
    tcpServer_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm) { onMessage(conn, buff, tm); });
    taskThreadPool_->start();
}

RPCServer::~RPCServer()
{
    stop();
}

void RPCServer::onMessage(const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm)
{   

    uint32_t minLen = sizeof(Header) + sizeof(RPC_Meta);
    // while(decode(buff, header, body))
    while(buff->readableBytes() >= minLen)
    {
        Header header;
        RPC_Meta meta;
        std::string body;
        
        if(!decode(buff, header, meta, body)) break;

        if (header.Flags == 3) // ACK 确认
        {
            if (conn)
                conn->ackReceived(meta.seq);
            continue;
        }

        if(header.Flags == 0)
        {
            std::weak_ptr<TcpConnection> weakPtr = conn;
            EventLoop* ioloop = conn->getLoop();

            auto it = handles_.find(meta.method_id);
            if(it == handles_.end())
            {
                LOG_ERROR << "Method not found";
                // taskThreadPool_->enqueue(
                //     [weakPtr,ioloop,tm,meta = std::move(meta)] 
                //     { 
                //         ioloop->runInLoop
                //         ([weakPtr, meta = std::move(meta)]{
                //         auto conn = weakPtr.lock();
                //         Buffer sendBuff;
                //         encode(&sendBuff, 1, 1, meta, const google::protobuf::Message &msg)
                //         conn->send();
                //         });
                //     });
                continue;
            }
            auto handler = it->second;

            taskThreadPool_->enqueue([weakPtr,ioloop,tm,handler,meta = std::move(meta),body = std::move(body)]
            {
                std::unique_ptr<google::protobuf::Message> response;

                try 
                {
                    response = handler(body);
                } catch (...) {
                    LOG_ERROR<<"处理业务逻辑时发生错误, seq = " << meta.seq;
                }

                auto respShared = std::shared_ptr<google::protobuf::Message>(std::move(response));
                ioloop->runInLoop([weakPtr, respShared, meta = std::move(meta)]
                {
                    auto conn = weakPtr.lock();
                    if (conn && conn->connected() && respShared)
                    {
                        Buffer sendBuff;
                        encode(&sendBuff, 1, 1, meta, *respShared);

                        conn->send(&sendBuff);
                    }
                }
            );
            });
        }
    }
}

void RPCServer::start()
{
    if(started_) return;
    tcpServer_.start();
    started_ = true;

    if(registry_)
    {
        Endpoint ep;
        ep.host = getLocalIp();
        ep.port = listenAddr_.toPort();
        ep.weight = 1;
        registry_->registerService(serviceName_, ep);
    }
}

void RPCServer::stop()
{
    if(!started_) return;
    
    if(registry_)
    {
        Endpoint ep;
        ep.host = getLocalIp();
        ep.port = listenAddr_.toPort();
        ep.weight = 1;

        registry_->deregisterService(serviceName_, ep);
        LOG_INFO << "Service " << serviceName_ << " deregistered at " << ep.host << ":" << ep.port;
    }
    tcpServer_.stop();
    started_ = false;
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

    struct ifaddrs* ifAddrStruct = nullptr;
    if(getifaddrs(&ifAddrStruct) == 0)
    {
        for(struct ifaddrs* ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next)
        {
            //如果地址为空或者不为Ipv4则跳过
            if(!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET)
                continue;
            auto* addr_in = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
            //排除环回地址
            if(addr_in->sin_addr.s_addr == htonl(INADDR_LOOPBACK))
                continue;

             char ip[INET_ADDRSTRLEN];
             inet_ntop(AF_INET, &addr_in->sin_addr, ip, sizeof(ip));
             freeifaddrs(ifAddrStruct);

             return std::string(ip);
        }
        freeifaddrs(ifAddrStruct);
    }

    return "127.0.0.1";
}