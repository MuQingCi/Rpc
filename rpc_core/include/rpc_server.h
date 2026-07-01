#ifndef CLEARMOON_RCP_SERVER_H
#define CLEARMOON_RCP_SERVER_H

// ClearMoon 网络库
#include "net/Log/Logger.h"
#include "net/TcpServer.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"
#include "toolFunc.h"

#include <cstdint>
#include <functional>
#include <google/protobuf/message.h>
#include <map>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <utility>

const uint32_t kValidFunc = 1001;

using namespace clearmoon;
using namespace clearmoon::net;

class RPCServer
{
public:
using RpcHandler = std::function<std::unique_ptr<google::protobuf::Message>(const std::string& reqBody)>;

    RPCServer(EventLoop* loop, InetAddress& listenAddr);

    void onMessage(const TcpConnectionPtr&, Buffer*, Timestamp);

    template<typename Request, typename  Response>
    void registerMethod(std::function<std::unique_ptr<Response>(Request& req)> logicFunc);

    void start();
private:
    // //请求分发路由(由method_id决定使用的doRequest)
    // std::string MethodRoute(RPC_Meta& meta, std::string& body);

    // template<typename Request>
    // std::string doRequest(std::string& body);

    // //回应分发路由(由method_id决定使用的doResponse)
    // auto ResponseRoute(uint32_t method_id, std::string& body);

    // template<typename Response>
    // Response doResponse(std::string& body);

    std::string handleMessage(std::string msg);

    TcpServer tcpServer_;

    bool started_ = false;

    std::map<uint32_t, RpcHandler> handles_;
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
