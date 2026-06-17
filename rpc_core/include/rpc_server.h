#ifndef CLEARMOON_RCP_H
#define CLEARMOON_RCP_H

// ClearMoon 网络库
#include "net/TcpServer.h"
#include "net/TcpConnection.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Buffer.h"
#include "net/Callbacks.h"

// Protobuf 消息定义
#include "message.pb.h"
#include <cstdint>
#include <netinet/in.h>
#include <string>

using namespace clearmoon::net;

struct Header
{
    uint16_t id;
    uint16_t status;
    uint32_t totalLen;
};

template<typename Msg>
void encode(Buffer* buff, uint16_t id, uint16_t status, Msg& msg)
{
    std::string body;
    msg.SerializeToString(&body);
    uint32_t total_len = static_cast<uint32_t>(body.size());

    buff->append(body.data(), body.size());

    //填充头部 按 Id-Status-Length逆序填入
    uint32_t netToalLen = htonl(total_len);
    buff->prependInt32(netToalLen);

    uint16_t netStatus = htons(status);
    buff->prependInt16(netStatus);

    uint16_t netId = htons(id);
    buff->prependInt16(netId);
}

bool decode(Buffer* buff, Header& header, std::string& body)
{
    uint32_t minLength = static_cast<uint32_t>(sizeof(uint32_t) + sizeof(uint16_t) * 2);
    if(buff->readableBytes() < minLength) return false;
    
    uint16_t netId = (*reinterpret_cast<const uint16_t*>(buff->peek()));
    header.id = ntohs(netId);
    buff->retrieve(sizeof(header.id));

    uint16_t netStatus = (*reinterpret_cast<const uint16_t*>(buff->peek()));
    header.status = ntohs(netStatus);
    buff->retrieve(sizeof(header.status));

    uint32_t netTotalLen = (*reinterpret_cast<const uint32_t*>(buff->peek()));
    header.totalLen = ntohl(netTotalLen);
    buff->retrieve(sizeof(header.totalLen));

    if(buff->readableBytes() < header.totalLen) return false;

    body = buff->readAllAsString();
    return true;
}

class RPCServer
{
public:

private:

};
#endif
