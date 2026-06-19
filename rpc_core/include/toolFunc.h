#ifndef CLEARMOON_TOOLFUNC_H
#define CLEARMOON_TOOLFUNC_H

#include "net/Buffer.h"

#include <google/protobuf/message.h>

using namespace clearmoon;
using namespace clearmoon::net;

struct Header
{
    uint16_t id;
    uint16_t status;
    uint32_t totalLen;
};

//编码函数(负责把有关信息和消息数据填入Buffer头部)
void encode(Buffer* buff, uint16_t id, uint16_t status, google::protobuf::Message& msg);

//解码函数(负责把有关信息和消息数据从Buffer中提取出来)
bool decode(Buffer* buff, Header& header, std::string& body);


#endif
