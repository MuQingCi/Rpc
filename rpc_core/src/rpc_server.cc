#include "../include/rpc_server.h"
#include "message.pb.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/Log/Logger.h"
#include "net/TcpServer.h"
#include "net/TcpConnection.h"

// Protobuf 消息定义
#include "message.pb.h"

//工具函数
#include "toolFunc.h"
#include <cstdint>
#include <memory>
#include <string>

RPCServer::RPCServer(EventLoop* loop, InetAddress& listenAddr) : tcpServer_(loop, TcpServer::ThreadPoolInitCallback(), listenAddr), taskThreadPool_(std::make_unique<TaskThreadPool>()) //任务线程池的线程数默认为8
{
    tcpServer_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buff, Timestamp tm) { onMessage(conn, buff, tm); });

    taskThreadPool_->start();
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


std::string RPCServer::handleMessage(const std::string& msg)
{
    return "RPCServer Echo: " + msg;
}

void RPCServer::start()
{
    if(started_) return;
    tcpServer_.start();
}

