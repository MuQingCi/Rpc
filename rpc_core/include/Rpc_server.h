#ifndef CLEARMOON_RCP_SERVER_H
#define CLEARMOON_RCP_SERVER_H

// ClearMoon 网络库
#include "Service/IsServiceRegister.h"
#include "net/Log/Logger.h"
#include "net/TcpServer.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "TaskThreadPool.h"

//工具函数
#include "ToolFunc.h"

#include <cstdint>
#include <functional>
#include <google/protobuf/message.h>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <set>
#include <string>
#include <utility>

const uint32_t kValidFunc = 1001;

namespace cmlib = clearmoon::net;

class RPCServer
{
public:
using RpcHandler = std::function<std::unique_ptr<google::protobuf::Message>(const std::string& reqBody)>;

    RPCServer(cmlib::EventLoop* loop, cmlib::InetAddress& listenAddr);

    //含配置注册的构造函数--单服务
    RPCServer(cmlib::EventLoop* loop, cmlib::InetAddress& listenAddr,std::shared_ptr<isServiceRegister> registry, const std::string& serviceName);

    //含配置注册的构造函数
    RPCServer(cmlib::EventLoop* loop, cmlib::InetAddress& listenAddr,std::shared_ptr<isServiceRegister> registry);

    ~RPCServer();

    void onMessage(const cmlib::TcpConnectionPtr&, cmlib::Buffer*, cmlib::Timestamp);

    template<typename Request, typename  Response>
    void registerMethod(std::function<std::unique_ptr<Response>(Request& req)> logicFunc);

    void start();
    void stop();

    void addService(const std::string& serviceName);
private:
    std::string getLocalIp() const;
    void sendErrorResponse(const cmlib::TcpConnectionPtr& conn,
                           const RPC_Meta& requestMeta,
                           RpcErrorCode errcode);

    //echo处理逻辑
    std::string handleMessage(const std::string& msg);

    cmlib::TcpServer tcpServer_;
    cmlib::InetAddress listenAddr_;

    bool started_ = false;

    //方法ID methodId->对应的业务逻辑回调
    //using RpcHandler = std::function<std::unique_ptr<google::protobuf::Message>(const std::string& reqBody)>;
    std::map<uint32_t, RpcHandler> handles_;

    //任务线程池
    std::unique_ptr<TaskThreadPool> taskThreadPool_;

    //服务配置类
    std::shared_ptr<isServiceRegister> registry_;
    //服务名
    std::set<std::string> serviceNames_;

    mutable std::mutex mutex_;
};

template<typename Request, typename  Response>
void RPCServer::registerMethod(std::function<std::unique_ptr<Response>(Request& req)> logicFunc)
{
    uint32_t methodId = getMethodId<Request>();

    handles_[methodId] = [logicFunc = std::move(logicFunc)] (const std::string&body) -> std::unique_ptr<google::protobuf::Message>
    {
        Request req;
        if(!req.ParseFromString(body))
        {
            LOG_INFO << "解析消息体失败";
            return nullptr;
        }

        auto res = logicFunc(req);
        return std::unique_ptr<google::protobuf::Message>(std::move(res));
    };
}


#endif
